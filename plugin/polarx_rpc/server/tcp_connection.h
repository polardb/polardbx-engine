//
// Created by zzy on 2022/7/6.
//

#pragma once

#include <atomic>
#include <cstdint>
#include <cstring>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include <poll.h> /// for poll wait

#include "../coders/polarx_encoder.h"
#include "../coders/protocol_fwd.h"
#include "../common_define.h"
#include "../polarx_rpc.h"
#include "../secure/auth_challenge_response.h"
#include "../session/pkt.h"
#include "../session/session_manager.h"
#include "../utility/atomicex.h"
#include "../utility/perf.h"
#include "../utility/time.h"

#include "epoll.h"
#include "epoll_group_ctx.h"
#include "socket_operator.h"

namespace polarx_rpc {

static constexpr uint32_t MIN_TCP_BUF = 0x800;    // 2KB
static constexpr uint32_t MAX_TCP_BUF = 0x200000; // 2MB

static constexpr uint32_t MIN_FIXED_DEALING_BUF = 0x1000;    // 4KB
static constexpr uint32_t MAX_FIXED_DEALING_BUF = 0x4000000; // 64MB

static constexpr uint32_t MAX_NET_WRITE_TIMEOUT = 60 * 1000; // 60s

class CtcpConnection final : public CepollCallback {
  NO_COPY_MOVE(CtcpConnection);

private:
  CmtEpoll &epoll_;
  const uint64_t tcp_id_;
  const std::string ip_, host_;
  const uint16_t port_;

  int fd_;

  /// lock for socket read
  CspinLock read_lock_;
  std::atomic<bool> recheck_;

  /// protocol info
  bool is_gx_;
  uint8_t gx_ver_;
  int hdr_sz_;

  /// receive buffer
  std::unique_ptr<uint8_t[]> fixed_buf_{};
  size_t fixed_buf_size_;
  std::unique_ptr<uint8_t[]> dyn_buf_{};
  size_t dyn_buf_size_;

  int64_t last_big_packet_time_;

  uint8_t *buf_ptr_;
  int buf_pos_;

  /// lifetime control
  std::mutex exit_lock_;
  bool registered_;

  /**
   * This field keep track the use of CtcpConnection object.
   * Following operations will add/sub the reference:
   * 1. new TCP connection in listener
   * 2. successfully register in epoll
   * 3. removed from epoll and reclaim in timer callback
   * 4. pre-event callback
   * 5. event callback
   * Note: When reference count reach 0, the object should be freed.
   */
  std::atomic<intptr_t> reference_; /// init with 1

  /// general encoder for messages
  CpolarxEncoder encoder_;

  /// auth info
  bool authed_;
  Authentication_interface_ptr auth_handler_;

  /// sessions
  CsessionManager sessions_;

  /// send lock
  std::mutex send_lock_;

  /// perf recorder
  std::atomic<int64_t> event_time_first_{0};
  std::atomic<int64_t> event_time_all_{0};

  static inline void reclaim(void *ctx) {
    auto tcp = reinterpret_cast<CtcpConnection *>(ctx);
    if (tcp->sub_reference() <= 0)
      delete tcp;
  }

  inline void tcp_log(plugin_log_level level, int err,
                      const char *what = nullptr, const char *extra = nullptr) {
    my_plugin_log_message(&plugin_info.plugin_info, level,
                          "TCP[%lu] %s:%u, %s%s%s%s%s", tcp_id_, host_.c_str(),
                          port_, nullptr == what ? "" : what,
                          what != nullptr && extra != nullptr ? "; " : "",
                          nullptr == extra ? "" : extra, err <= 0 ? "" : ". ",
                          err <= 0 ? "" : std::strerror(err));
  }

  inline void tcp_info(int err, const char *what = nullptr,
                       const char *extra = nullptr) {
    tcp_log(MY_INFORMATION_LEVEL, err, what, extra);
  }

  inline void tcp_warn(int err, const char *what = nullptr,
                       const char *extra = nullptr) {
    tcp_log(MY_WARNING_LEVEL, err, what, extra);
  }

  inline void tcp_err(int err, const char *what = nullptr,
                      const char *extra = nullptr) {
    tcp_log(MY_ERROR_LEVEL, err, what, extra);
  }

  inline void fin(const char *reason = nullptr) {
    if (LIKELY(fd_ > 0)) {
      std::lock_guard<std::mutex> lck(exit_lock_);
      if (LIKELY(fd_ > 0)) {
        Csocket::shutdown(fd_);
        if (LIKELY(registered_)) {
          auto err = epoll_.del_fd(fd_);
          if (UNLIKELY(err != 0)) {
            tcp_err(-err, "failed to unregister epoll");
            /// abort? may cause dangling pointer and may segment fault
            // unireg_abort(MYSQLD_ABORT_EXIT);
          }
          /// successfully removed and do reclaim
          registered_ = false;
          /// schedule a async reclaim and sub reference in callback
          auto trigger_time = Ctime::steady_ms() + MAX_EPOLL_TIMEOUT * 2;
          epoll_.push_trigger({nullptr, nullptr, this, reclaim}, trigger_time);
          tcp_info(0, "schedule a lazy destructor", reason);
        }
        Csocket::close(fd_);

        /// shutdown all sessions
        sessions_.shutdown(epoll_.session_count());

        fd_ = -1;
      }
    }
  }

  void set_fd(int fd) final {
    std::lock_guard<std::mutex> lck(exit_lock_);
    fd_ = fd;
  }

  inline void add_reference() {
    auto before = reference_.fetch_add(1, std::memory_order_relaxed);
    assert(before > 0);
  }

  /// return reference count after invoke
  inline intptr_t sub_reference() {
    auto before = reference_.fetch_sub(1, std::memory_order_release);
    assert(before >= 1);
    if (UNLIKELY(1 == before))
      std::atomic_thread_fence(std::memory_order_acquire);
    return before - 1;
  }

  void fd_registered() final {
    tcp_info(0, "connected and registered");
    add_reference();
    std::lock_guard<std::mutex> lck(exit_lock_);
    registered_ = true;
  }

  void pre_events() final {
    if (enable_perf_hist) {
      auto now = Ctime::steady_ns();
      int64_t exp = 0;
      event_time_first_.compare_exchange_strong(
          exp, now, std::memory_order_relaxed, std::memory_order_relaxed);
      exp = 0;
      event_time_all_.compare_exchange_strong(
          exp, now, std::memory_order_relaxed, std::memory_order_relaxed);
    }
    add_reference();
  }

  inline std::unique_ptr<ProtoMsg> decode(uint8_t type, const void *data,
                                          size_t length) {
    std::unique_ptr<ProtoMsg> msg;
    switch (type) {
    case Polarx::ClientMessages::CON_CAPABILITIES_GET:
      msg.reset(new Polarx::Connection::CapabilitiesGet());
      break;

    case Polarx::ClientMessages::SESS_AUTHENTICATE_START:
      msg.reset(new Polarx::Session::AuthenticateStart());
      break;

    case Polarx::ClientMessages::SESS_AUTHENTICATE_CONTINUE:
      msg.reset(new Polarx::Session::AuthenticateContinue());
      break;

    case Polarx::ClientMessages::EXPECT_OPEN:
      msg.reset(new Polarx::Expect::Open());
      break;

    case Polarx::ClientMessages::EXPECT_CLOSE:
      msg.reset(new Polarx::Expect::Close());
      break;

    case Polarx::ClientMessages::EXEC_PLAN_READ:
      msg.reset(new Polarx::ExecPlan::ExecPlan());
      break;

    case Polarx::ClientMessages::EXEC_SQL:
      msg.reset(new Polarx::Sql::StmtExecute());
      break;

    case Polarx::ClientMessages::SESS_NEW:
      msg.reset(new Polarx::Session::NewSession());
      break;

    case Polarx::ClientMessages::SESS_KILL:
      msg.reset(new Polarx::Session::KillSession());
      break;

    case Polarx::ClientMessages::SESS_CLOSE:
      msg.reset(new Polarx::Session::Close());
      break;

    case Polarx::ClientMessages::TOKEN_OFFER:
      msg.reset(new Polarx::Sql::TokenOffer());
      break;

    case Polarx::ClientMessages::GET_TSO:
      msg.reset(new Polarx::ExecPlan::GetTSO());
      break;

    default:
      return {};
    }

    // feed the data to the command (up to the specified boundary)
    google::protobuf::io::CodedInputStream stream(
        reinterpret_cast<const uint8_t *>(data), static_cast<int>(length));
    stream.SetTotalBytesLimit(static_cast<int>(length), -1 /*no warnings*/);
    msg->ParseFromCodedStream(&stream);

    if (!msg->IsInitialized()) {
      tcp_warn(0, "failed to parsing message",
               msg->InitializationErrorString().c_str());
      return {};
    }
    return msg;
  }

  inline void send_auth_result(const uint64_t &sid,
                               const Authentication_interface::Response &r) {
    switch (r.status) {
    case Authentication_interface::Succeeded: {
      /// record user name and new host
      auto &info = auth_handler_->get_authentication_info();
      sessions_.set_user(info.m_authed_account_name.c_str());
      authed_ = true;
      tcp_info(0, "auth success");

      /// reset handler and send success notify
      auth_handler_.reset();

      ::Polarx::Session::AuthenticateOk msg;
      msg.set_auth_data(r.data);
      msg_enc()
          .encode_protobuf_message<
              ::Polarx::ServerMessages::SESS_AUTHENTICATE_OK>(msg);
    } break;

    case Authentication_interface::Failed:
      /// reset auth handler
      auth_handler_.reset();
      /// send error message
      msg_enc().reset_sid(sid);
      msg_enc().encode_error(Polarx::Error::ERROR, r.error_code, r.data,
                             "HY000");
      break;

    default:
      /// send continue challenge
      ::Polarx::Session::AuthenticateContinue msg;
      msg.set_auth_data(r.data);
      msg_enc()
          .encode_protobuf_message<
              ::Polarx::ServerMessages::SESS_AUTHENTICATE_CONTINUE>(msg);
    }

    /// flush buffer
    encoder().flush(*this);
  }

  inline void auth_routine(const uint64_t &sid, msg_t &&msg) {
    switch (msg.type) {
    case Polarx::ClientMessages::CON_CAPABILITIES_GET: {
      /// for alive probe
      ::Polarx::Connection::Capabilities caps;
      auto cap = caps.add_capabilities();
      cap->set_name("polarx_rpc");
      auto v = cap->mutable_value();
      v->set_type(::Polarx::Datatypes::Any::SCALAR);
      auto s = v->mutable_scalar();
      s->set_type(::Polarx::Datatypes::Scalar::V_UINT);
      s->set_v_unsigned_int(1);

      /// send to client
      msg_enc().reset_sid(sid);
      msg_enc()
          .encode_protobuf_message<::Polarx::ServerMessages::CONN_CAPABILITIES>(
              caps);
      encoder().flush(*this);
    } break;

    case Polarx::ClientMessages::SESS_AUTHENTICATE_START: {
      /// start auth
      const auto &authm =
          static_cast<const ::Polarx::Session::AuthenticateStart &>(*msg.msg);
      Authentication_interface::Response r;
      if (LIKELY(!auth_handler_)) {
        auth_handler_ = Sasl_mysql41_auth::create(*this, nullptr);
        r = auth_handler_->handle_start(authm.mech_name(), authm.auth_data(),
                                        authm.initial_response());
        tcp_info(0, "start auth", authm.mech_name().c_str());
      } else {
        r.status = Authentication_interface::Error,
        r.error_code = ER_NET_PACKETS_OUT_OF_ORDER;
      }
      send_auth_result(sid, r);
    } break;

    case Polarx::ClientMessages::SESS_AUTHENTICATE_CONTINUE: {
      /// continue auth
      const auto &authm =
          static_cast<const ::Polarx::Session::AuthenticateContinue &>(
              *msg.msg);
      Authentication_interface::Response r;
      if (LIKELY(auth_handler_)) {
        r = auth_handler_->handle_continue(authm.auth_data());
        tcp_info(0, "continue auth");
      } else {
        r.status = Authentication_interface::Error,
        r.error_code = ER_NET_PACKETS_OUT_OF_ORDER;
      }
      send_auth_result(sid, r);
    } break;

    default:
      send_auth_result(
          sid, {Authentication_interface::Error, ER_ACCESS_DENIED_ERROR});
      break;
    }
  }

  /// scheduled task
  class CdelayedTask final : public Ctask<CdelayedTask> {
  private:
    const uint64_t sid_;
    CtcpConnection *tcp_;

    friend class Ctask;

    void run() {
      auto s = tcp_->sessions_.get_session(sid_);
      if (s)
        s->run();
    }

  public:
    CdelayedTask(const uint64_t &sid, CtcpConnection *tcp)
        : sid_(sid), tcp_(tcp) {
      tcp_->add_reference();
    }

    ~CdelayedTask() final {
      auto after = tcp_->sub_reference();
      if (after <= 0) {
        assert(0 == after);
        delete tcp_;
      }
    }
  };

  bool events(uint32_t events, int index, int total) final {
    if (UNLIKELY((events & EPOLLERR) != 0 || fd_ < 0)) {
      fin("error occurs or fd closed");
      return sub_reference() > 0;
    }

    if (LIKELY((events & EPOLLIN) != 0)) {
      /// read event
      std::map<uint64_t, bool> notify_set;

      do {
        if (UNLIKELY(!read_lock_.try_lock())) {
          recheck_.store(true, std::memory_order_release);
          std::atomic_thread_fence(std::memory_order_seq_cst);
          if (LIKELY(!read_lock_.try_lock()))
            break; /// do any possible notify task
        }

        auto now_time = Ctime::steady_ms();
        auto max_pkt = max_allowed_packet;

        do {
          /// clear flag before read
          recheck_.store(false, std::memory_order_relaxed);

          auto buf_size =
              buf_ptr_ == fixed_buf_.get() ? fixed_buf_size_ : dyn_buf_size_;
          assert(buf_pos_ < static_cast<int>(buf_size));
          auto retry = 0;
          int iret;
          do {
            iret = ::recv(fd_, buf_ptr_ + buf_pos_, buf_size - buf_pos_, 0);
          } while (UNLIKELY(iret < 0 && EINTR == errno && ++retry < 10));
          DBG_LOG(("read pkt once"));

          if (LIKELY(iret > 0)) {
            DBG_LOG(("got pkt"));
            /// perf
            if (enable_perf_hist) {
              /// first pkt received
              auto start_time =
                  event_time_first_.exchange(0, std::memory_order_relaxed);
              if (start_time != 0) {
                auto now = Ctime::steady_ns();
                auto recv_time = now - start_time;
                g_recv_first_hist.update(static_cast<double>(recv_time) / 1e9);
              }
            }

            /// update buffer
            buf_pos_ += iret;
            recheck_.store(true, std::memory_order_relaxed);

            /// dealing buf(consume/extend etc.)
            auto start_pos = 0;
            while (LIKELY(buf_pos_ - start_pos >= hdr_sz_)) {
              /// enough hdr
              uint64_t sid;
              uint8_t type;
              uint32_t extra_data;
              if (is_gx_) {
                auto hdr = reinterpret_cast<const galaxy_pkt_hdr_t *>(
                    buf_ptr_ + start_pos);
                /// version check first
                if (hdr->version != gx_ver_) {
                  fin("bad protocol version");
                  /// fatal error and drop all data
                  buf_pos_ = start_pos = 0;
                  break;
                }
                sid = hdr->sid;
                type = hdr->type;
                extra_data = hdr->length;
              } else {
                auto hdr = reinterpret_cast<const polarx_pkt_hdr_t *>(
                    buf_ptr_ + start_pos);
                sid = hdr->sid;
                type = hdr->type;
                extra_data = hdr->length;
              }
#if defined(__arm__) || defined(__aarch64__)
              sid = __builtin_bswap64(sid);
              extra_data = __builtin_bswap32(extra_data);
#endif

              /// check extra data size
              if (extra_data > max_pkt) {
                fin("max allowed packet size exceeded");
                /// fatal error and drop all data
                buf_pos_ = start_pos = 0;
                break;
              }

              auto full_pkt_size = hdr_sz_ + extra_data - 1;
              if (UNLIKELY(full_pkt_size > fixed_buf_size_))
                last_big_packet_time_ = now_time; /// refresh dyn buf time

              /// check enough data?
              if (LIKELY(buf_pos_ - start_pos >=
                         static_cast<int>(full_pkt_size))) {
                /// full packet
                msg_t msg{type, decode(type, buf_ptr_ + start_pos + hdr_sz_,
                                       extra_data - 1)};
                if (!msg.msg) {
                  /// decode error
                  fin("decode error");
                  /// fatal error and drop all data
                  buf_pos_ = start_pos = 0;
                  break;
                }

                if (enable_perf_hist &&
                    buf_pos_ == start_pos + static_cast<int>(full_pkt_size)) {
                  /// all pkt received
                  auto start_time =
                      event_time_all_.exchange(0, std::memory_order_relaxed);
                  if (start_time != 0) {
                    auto now = Ctime::steady_ns();
                    auto recv_time = now - start_time;
                    g_recv_all_hist.update(static_cast<double>(recv_time) /
                                           1e9);
                  }
                }

                if (LIKELY(authed_) &&
                    LIKELY(msg.type !=
                           Polarx::ClientMessages::CON_CAPABILITIES_GET))
                  sessions_.execute(*this, sid, std::move(msg), notify_set);
                else /// do auth routine
                  auth_routine(sid, std::move(msg));

                start_pos += static_cast<int>(full_pkt_size);
              } else {
                /// check buffer large enough?
                if (UNLIKELY(full_pkt_size > buf_size)) {
                  /// need larger buffer
                  dyn_buf_size_ = full_pkt_size * 2;
                  if (dyn_buf_size_ > max_pkt + hdr_sz_ - 1)
                    dyn_buf_size_ = max_pkt + hdr_sz_ - 1;
                  assert(dyn_buf_size_ >= full_pkt_size);
                  std::unique_ptr<uint8_t[]> new_dyn_buf(
                      new uint8_t[dyn_buf_size_]);
                  ::memcpy(new_dyn_buf.get(), buf_ptr_ + start_pos,
                           buf_pos_ - start_pos);
                  dyn_buf_.swap(new_dyn_buf);
                  buf_ptr_ = dyn_buf_.get();
                  buf_pos_ -= start_pos;
                  start_pos = 0;
                }
                break; /// continue receiving
              }
            }

            /// clear consumed data
            if (start_pos != 0) {
              /// should move it to head
              if (buf_pos_ > start_pos)
                ::memmove(buf_ptr_, buf_ptr_ + start_pos, buf_pos_ - start_pos);
              buf_pos_ -= start_pos;
              start_pos = 0;
            }
            assert(0 == start_pos);

            if (0 == buf_pos_) {
              /// no data in buf, and try shrink
              if (now_time - last_big_packet_time_ > 10 * 1000 && /// >10s
                  buf_ptr_ != fixed_buf_.get()) {                 /// dyn buf
                dyn_buf_size_ = 0;
                dyn_buf_.reset();
                buf_ptr_ = fixed_buf_.get(); /// back to fixed buf
              }
            }

          } else if (UNLIKELY(0 == iret)) {
            DBG_LOG(("read EOF"));
            fin("eof");
          } else {
            auto err = errno;
            if (err != EAGAIN && err != EWOULDBLOCK) {
              /// fatal error
              tcp_err(err, "recv error");
              fin("fatal error");
            }
            /// would block error, just leave it
          }

        } while (recheck_.load(std::memory_order_acquire));

        read_lock_.unlock();
        std::atomic_thread_fence(std::memory_order_seq_cst);
      } while (UNLIKELY(recheck_.load(std::memory_order_acquire)));

      /// dealing notify or direct run outside the read lock.
      if (!notify_set.empty()) {
        /// last one in event queue and the last one in notify set,
        /// can run directly, or the trx session in last event(if exists)
        /// record the sid which is in trx
        uint64_t trx_sid = UINT64_C(0xFFFFFFFFFFFFFFFF);
        auto cnt = notify_set.size();
        for (const auto &pair : notify_set) {
          const auto &sid = pair.first;
          const auto &in_trx = pair.second;
          /// record it if empty
          if (UINT64_C(0xFFFFFFFFFFFFFFFF) == trx_sid && in_trx) {
            trx_sid = sid;
            continue; /// deferred
          }
          if (index + 1 == total &&
              (0 == --cnt && UINT64_C(0xFFFFFFFFFFFFFFFF) == trx_sid)) {
            /// last one in set and no trx session so run directly
            DBG_LOG(("tcp_conn run last session %lu directly", sid));
            auto s = sessions_.get_session(sid);
            if (s)
              s->run();
          } else {
            /// schedule in work task
            DBG_LOG(("tcp_conn schedule task session %lu", sid));
            std::unique_ptr<CdelayedTask> task(new CdelayedTask(sid, this));
            auto bret = epoll_.push_work(task->gen_task());
            if (LIKELY(bret))
              task.release(); /// release the owner of queue
            else {
              /// queue is full, try and kill if timeout
              task.reset();
              if (LIKELY(sessions_.remove_and_shutdown(epoll_.session_count(),
                                                       sid))) {
                /// send fatal msg()(not lock protected so new one)
                CpolarxEncoder enc(sid);
                enc.message_encoder().encode_error(
                    Polarx::Error::FATAL, ER_POLARX_RPC_ERROR_MSG,
                    "Work queue overflow.", "70100");
                enc.flush(*this);
              }
            }
          }
        }
        /// run session in trx directly
        if (trx_sid != UINT64_C(0xFFFFFFFFFFFFFFFF)) {
          DBG_LOG(("tcp_conn run trx session %lu directly", sid));
          auto s = sessions_.get_session(trx_sid);
          if (s)
            s->run();
        }
      }
    }

    return sub_reference() > 0;
  }

  inline int wait_send() {
    auto timeout = net_write_timeout;
    if (UNLIKELY(timeout > MAX_NET_WRITE_TIMEOUT))
      timeout = MAX_NET_WRITE_TIMEOUT;
    ::pollfd pfd{fd_, POLLOUT | POLLERR, 0};
    return ::poll(&pfd, 1, static_cast<int>(timeout));
  }

  friend class Clistener;

public:
  explicit CtcpConnection(CmtEpoll &epoll, uint64_t id, int fd,
                          std::string &&ip, std::string &&host, uint16_t port)
      : epoll_(epoll), tcp_id_(id), ip_(std::forward<std::string>(ip)),
        host_(std::forward<std::string>(host)), port_(port), fd_(fd),
        recheck_(false), is_gx_(false), gx_ver_(0), hdr_sz_(13),
        fixed_buf_size_(0), dyn_buf_size_(0), last_big_packet_time_(0),
        buf_ptr_(nullptr), buf_pos_(0), registered_(false), reference_(1),
        encoder_(0), authed_(false), sessions_(host_, port) {
    DBG_LOG(("new TCP context %s:%u", host_.c_str(), port_));

    /// set TCP buf size if needed
    auto buf_size = tcp_send_buf;
    if (buf_size != 0) {
      if (UNLIKELY(buf_size < MIN_TCP_BUF))
        buf_size = MIN_TCP_BUF;
      else if (UNLIKELY(buf_size > MAX_TCP_BUF))
        buf_size = MAX_TCP_BUF;
    }
    auto err = Csocket::set_sndbuf(fd_, buf_size);
    if (err != 0)
      tcp_warn(-err, "failed to set sndbuf");
    buf_size = tcp_recv_buf;
    if (buf_size != 0) {
      if (UNLIKELY(buf_size < MIN_TCP_BUF))
        buf_size = MIN_TCP_BUF;
      else if (UNLIKELY(buf_size > MAX_TCP_BUF))
        buf_size = MAX_TCP_BUF;
    }
    err = Csocket::set_rcvbuf(fd_, buf_size);
    if (err != 0)
      tcp_warn(-err, "failed to set rcvbuf");

    /// init fixed dealing buf
    fixed_buf_size_ = tcp_fixed_dealing_buf;
    if (UNLIKELY(fixed_buf_size_ < MIN_FIXED_DEALING_BUF))
      fixed_buf_size_ = MIN_FIXED_DEALING_BUF;
    else if (UNLIKELY(fixed_buf_size_ > MAX_FIXED_DEALING_BUF))
      fixed_buf_size_ = MAX_FIXED_DEALING_BUF;
    fixed_buf_.reset(new uint8_t[fixed_buf_size_]);
    buf_ptr_ = fixed_buf_.get();

    /// load protocol info
    is_gx_ = galaxy_protocol;
    gx_ver_ = galaxy_version;
    hdr_sz_ = is_gx_ ? 14 : 13;
  }

  ~CtcpConnection() final { tcp_info(0, "destruct"); }

  inline const std::string &ip() const { return ip_; }
  inline const std::string &host() const { return host_; }

  inline CpolarxEncoder &encoder() { return encoder_; }
  inline protocol::PolarX_Message_encoder &msg_enc() {
    return encoder_.message_encoder();
  }

  inline CsessionManager &session_manager() { return sessions_; }

  inline CmtEpoll &epoll() { return epoll_; }

  inline std::mutex &send_lock() { return send_lock_; }

  /// blocking send
  bool send(const void *data, size_t length) final {
    if (UNLIKELY(fd_ < 0))
      return false;
    auto ptr = reinterpret_cast<const uint8_t *>(data);
    auto retry = 0;
    while (length > 0) {
      auto iret = ::send(fd_, ptr, length, 0);
      if (UNLIKELY(iret <= 0)) {
        auto err = errno;
        if (LIKELY(EAGAIN == err || EWOULDBLOCK == err)) {
          /// need wait
          auto wait = wait_send();
          if (UNLIKELY(wait <= 0)) {
            if (wait < 0)
              tcp_warn(errno, "send poll error");
            else
              tcp_warn(0, "send net write timeout");
            fin();
            return false;
          }
          /// wait done and retry
        } else if (EINTR == err) {
          if (++retry >= 10) {
            tcp_warn(EINTR, "send error with EINTR after retry 10");
            fin();
            return false;
          }
          /// simply retry
        } else {
          /// fatal error
          tcp_err(err, "send error");
          fin();
          return false;
        }
      } else {
        retry = 0; /// clear retry
        ptr += iret;
        length -= iret;
      }
    }
    return true;
  }
};

} // namespace polarx_rpc
