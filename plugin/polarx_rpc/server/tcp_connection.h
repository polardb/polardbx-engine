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

#include <poll.h>  /// for poll wait

#include "m_ctype.h"
#include "sql/mysqld.h"

#include "../coders/polarx_encoder.h"
#include "../coders/protocol_fwd.h"
#include "../common_define.h"
#include "../global_defines.h"
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

static constexpr uint32_t MIN_TCP_BUF = 0x800;     // 2KB
static constexpr uint32_t MAX_TCP_BUF = 0x200000;  // 2MB

static constexpr uint32_t MIN_FIXED_DEALING_BUF = 0x1000;     // 4KB
static constexpr uint32_t MAX_FIXED_DEALING_BUF = 0x4000000;  // 64MB

static constexpr uint32_t MAX_NET_WRITE_TIMEOUT = 60 * 1000;  // 60s

class CtcpConnection final : public CepollCallback {
  NO_COPY_MOVE(CtcpConnection)

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
  std::atomic<intptr_t> reference_;  /// init with 1

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
    if (tcp->sub_reference() <= 0) delete tcp;
  }

  inline void tcp_log(plugin_log_level level, int err,
                      const char *what = nullptr, const char *extra = nullptr) {
    std::lock_guard<std::mutex> plugin_lck(plugin_info.mutex);
    if (plugin_info.plugin_info != nullptr)
      my_plugin_log_message(
          &plugin_info.plugin_info, level, "TCP[%lu] %s:%u(%d,%s), %s%s%s%s%s",
          tcp_id_, host_.c_str(), port_, fd_, registered_ ? "reg" : "unreg",
          nullptr == what ? "" : what,
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
        int64_t fin_start_time = 0;
        if (enable_perf_hist) fin_start_time = Ctime::steady_ns();

        Csocket::shutdown(fd_);
        if (LIKELY(registered_)) {
          auto err = epoll_.del_fd(fd_);
          if (UNLIKELY(err != 0)) {
            tcp_err(-err, "failed to unregister epoll");
            /// abort? may cause dangling pointer and may segment fault
            unireg_abort(MYSQLD_ABORT_EXIT);
          }
          /// successfully removed and do reclaim
          registered_ = false;
          plugin_info.tcp_closing.fetch_add(1, std::memory_order_release);
          /// schedule a async reclaim and sub reference in callback
          auto trigger_time = Ctime::steady_ms() + MAX_EPOLL_TIMEOUT * 2;
          epoll_.push_trigger({nullptr, nullptr, this, reclaim}, trigger_time);
          tcp_info(0, "schedule a lazy destructor", reason);
        }
        Csocket::close(fd_);

        /// shutdown all sessions
        sessions_.shutdown(epoll_.session_count());

        fd_ = -1;

        if (fin_start_time != 0) {
          auto fin_end_time = Ctime::steady_ns();
          auto fin_time = fin_end_time - fin_start_time;
          g_fin_hist.update(static_cast<double>(fin_time) / 1e9);
        }
      }
    }
  }

  void set_fd(int fd) final {
    std::lock_guard<std::mutex> lck(exit_lock_);
    fd_ = fd;
  }

  inline void add_reference() {
    auto before = reference_.fetch_add(1, std::memory_order_relaxed);
    (void)before;
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

  void fd_pre_register() final {
    add_reference();
    {
      std::lock_guard<std::mutex> lck(exit_lock_);
      registered_ = true;
    }
    tcp_info(0, "connected and registered");
  }

  bool fd_rollback_register() final {
    {
      std::lock_guard<std::mutex> lck(exit_lock_);
      registered_ = false;
    }
    tcp_info(0, "rollback register");
    return sub_reference() > 0;
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

#if POLARX_RPC_PKT_DBG
  static uint32_t crc32(const void *data, size_t size,
                        uint32_t crc = 0xffffffff) {
    static const uint32_t table[] = {
        0x00000000, 0x77073096, 0xee0e612c, 0x990951ba, 0x076dc419, 0x706af48f,
        0xe963a535, 0x9e6495a3, 0x0edb8832, 0x79dcb8a4, 0xe0d5e91e, 0x97d2d988,
        0x09b64c2b, 0x7eb17cbd, 0xe7b82d07, 0x90bf1d91, 0x1db71064, 0x6ab020f2,
        0xf3b97148, 0x84be41de, 0x1adad47d, 0x6ddde4eb, 0xf4d4b551, 0x83d385c7,
        0x136c9856, 0x646ba8c0, 0xfd62f97a, 0x8a65c9ec, 0x14015c4f, 0x63066cd9,
        0xfa0f3d63, 0x8d080df5, 0x3b6e20c8, 0x4c69105e, 0xd56041e4, 0xa2677172,
        0x3c03e4d1, 0x4b04d447, 0xd20d85fd, 0xa50ab56b, 0x35b5a8fa, 0x42b2986c,
        0xdbbbc9d6, 0xacbcf940, 0x32d86ce3, 0x45df5c75, 0xdcd60dcf, 0xabd13d59,
        0x26d930ac, 0x51de003a, 0xc8d75180, 0xbfd06116, 0x21b4f4b5, 0x56b3c423,
        0xcfba9599, 0xb8bda50f, 0x2802b89e, 0x5f058808, 0xc60cd9b2, 0xb10be924,
        0x2f6f7c87, 0x58684c11, 0xc1611dab, 0xb6662d3d, 0x76dc4190, 0x01db7106,
        0x98d220bc, 0xefd5102a, 0x71b18589, 0x06b6b51f, 0x9fbfe4a5, 0xe8b8d433,
        0x7807c9a2, 0x0f00f934, 0x9609a88e, 0xe10e9818, 0x7f6a0dbb, 0x086d3d2d,
        0x91646c97, 0xe6635c01, 0x6b6b51f4, 0x1c6c6162, 0x856530d8, 0xf262004e,
        0x6c0695ed, 0x1b01a57b, 0x8208f4c1, 0xf50fc457, 0x65b0d9c6, 0x12b7e950,
        0x8bbeb8ea, 0xfcb9887c, 0x62dd1ddf, 0x15da2d49, 0x8cd37cf3, 0xfbd44c65,
        0x4db26158, 0x3ab551ce, 0xa3bc0074, 0xd4bb30e2, 0x4adfa541, 0x3dd895d7,
        0xa4d1c46d, 0xd3d6f4fb, 0x4369e96a, 0x346ed9fc, 0xad678846, 0xda60b8d0,
        0x44042d73, 0x33031de5, 0xaa0a4c5f, 0xdd0d7cc9, 0x5005713c, 0x270241aa,
        0xbe0b1010, 0xc90c2086, 0x5768b525, 0x206f85b3, 0xb966d409, 0xce61e49f,
        0x5edef90e, 0x29d9c998, 0xb0d09822, 0xc7d7a8b4, 0x59b33d17, 0x2eb40d81,
        0xb7bd5c3b, 0xc0ba6cad, 0xedb88320, 0x9abfb3b6, 0x03b6e20c, 0x74b1d29a,
        0xead54739, 0x9dd277af, 0x04db2615, 0x73dc1683, 0xe3630b12, 0x94643b84,
        0x0d6d6a3e, 0x7a6a5aa8, 0xe40ecf0b, 0x9309ff9d, 0x0a00ae27, 0x7d079eb1,
        0xf00f9344, 0x8708a3d2, 0x1e01f268, 0x6906c2fe, 0xf762575d, 0x806567cb,
        0x196c3671, 0x6e6b06e7, 0xfed41b76, 0x89d32be0, 0x10da7a5a, 0x67dd4acc,
        0xf9b9df6f, 0x8ebeeff9, 0x17b7be43, 0x60b08ed5, 0xd6d6a3e8, 0xa1d1937e,
        0x38d8c2c4, 0x4fdff252, 0xd1bb67f1, 0xa6bc5767, 0x3fb506dd, 0x48b2364b,
        0xd80d2bda, 0xaf0a1b4c, 0x36034af6, 0x41047a60, 0xdf60efc3, 0xa867df55,
        0x316e8eef, 0x4669be79, 0xcb61b38c, 0xbc66831a, 0x256fd2a0, 0x5268e236,
        0xcc0c7795, 0xbb0b4703, 0x220216b9, 0x5505262f, 0xc5ba3bbe, 0xb2bd0b28,
        0x2bb45a92, 0x5cb36a04, 0xc2d7ffa7, 0xb5d0cf31, 0x2cd99e8b, 0x5bdeae1d,
        0x9b64c2b0, 0xec63f226, 0x756aa39c, 0x026d930a, 0x9c0906a9, 0xeb0e363f,
        0x72076785, 0x05005713, 0x95bf4a82, 0xe2b87a14, 0x7bb12bae, 0x0cb61b38,
        0x92d28e9b, 0xe5d5be0d, 0x7cdcefb7, 0x0bdbdf21, 0x86d3d2d4, 0xf1d4e242,
        0x68ddb3f8, 0x1fda836e, 0x81be16cd, 0xf6b9265b, 0x6fb077e1, 0x18b74777,
        0x88085ae6, 0xff0f6a70, 0x66063bca, 0x11010b5c, 0x8f659eff, 0xf862ae69,
        0x616bffd3, 0x166ccf45, 0xa00ae278, 0xd70dd2ee, 0x4e048354, 0x3903b3c2,
        0xa7672661, 0xd06016f7, 0x4969474d, 0x3e6e77db, 0xaed16a4a, 0xd9d65adc,
        0x40df0b66, 0x37d83bf0, 0xa9bcae53, 0xdebb9ec5, 0x47b2cf7f, 0x30b5ffe9,
        0xbdbdf21c, 0xcabac28a, 0x53b39330, 0x24b4a3a6, 0xbad03605, 0xcdd70693,
        0x54de5729, 0x23d967bf, 0xb3667a2e, 0xc4614ab8, 0x5d681b02, 0x2a6f2b94,
        0xb40bbe37, 0xc30c8ea1, 0x5a05df1b, 0x2d02ef8d,
    };

    crc ^= 0xffffffff;  // Restore last state.
    auto ptr = reinterpret_cast<const uint8_t *>(data);

    for (size_t i = 0; i < size; ++i)
      crc = table[(crc ^ ptr[i]) & 0xffu] ^ (crc >> 8u);
    crc = crc ^ 0xffffffff;

    return crc;
  }
#endif

  inline std::unique_ptr<ProtoMsg> decode(uint8_t type, const void *data,
                                          size_t length) {
    std::unique_ptr<ProtoMsg> msg;
    switch (type) {
      case PolarXRPC::ClientMessages::CON_CAPABILITIES_GET:
        msg.reset(new PolarXRPC::Connection::CapabilitiesGet());
        break;

      case PolarXRPC::ClientMessages::SESS_AUTHENTICATE_START:
        msg.reset(new PolarXRPC::Session::AuthenticateStart());
        break;

      case PolarXRPC::ClientMessages::SESS_AUTHENTICATE_CONTINUE:
        msg.reset(new PolarXRPC::Session::AuthenticateContinue());
        break;

      case PolarXRPC::ClientMessages::EXPECT_OPEN:
        msg.reset(new PolarXRPC::Expect::Open());
        break;

      case PolarXRPC::ClientMessages::EXPECT_CLOSE:
        msg.reset(new PolarXRPC::Expect::Close());
        break;

      case PolarXRPC::ClientMessages::EXEC_PLAN_READ:
        msg.reset(new PolarXRPC::ExecPlan::ExecPlan());
        break;

      case PolarXRPC::ClientMessages::EXEC_SQL:
        msg.reset(new PolarXRPC::Sql::StmtExecute());
        break;

      case PolarXRPC::ClientMessages::SESS_NEW:
        msg.reset(new PolarXRPC::Session::NewSession());
        break;

      case PolarXRPC::ClientMessages::SESS_KILL:
        msg.reset(new PolarXRPC::Session::KillSession());
        break;

      case PolarXRPC::ClientMessages::SESS_CLOSE:
        msg.reset(new PolarXRPC::Session::Close());
        break;

      case PolarXRPC::ClientMessages::TOKEN_OFFER:
        msg.reset(new PolarXRPC::Sql::TokenOffer());
        break;

      case PolarXRPC::ClientMessages::GET_TSO:
        msg.reset(new PolarXRPC::ExecPlan::GetTSO());
        break;

      case PolarXRPC::ClientMessages::AUTO_SP:
        msg.reset(new PolarXRPC::ExecPlan::AutoSp());
        break;

      case PolarXRPC::ClientMessages::FILE_OPERATION_GET_FILE_INFO:
        msg.reset(new PolarXRPC::PhysicalBackfill::GetFileInfoOperator);
        break;

      case PolarXRPC::ClientMessages::FILE_OPERATION_FILE_MANAGE:
        msg.reset(new PolarXRPC::PhysicalBackfill::FileManageOperator());
        break;

      case PolarXRPC::ClientMessages::FILE_OPERATION_TRANSFER_FILE_DATA:
        msg.reset(new PolarXRPC::PhysicalBackfill::TransferFileDataOperator());
        break;

      default: {
        char extra[0x100];
        ::snprintf(extra, sizeof(extra), "type: %u, len: %zu", type, length);
        tcp_warn(0, "failed to parsing message", extra);
        return {};
      }
    }

    // feed the data to the command (up to the specified boundary)
    google::protobuf::io::CodedInputStream stream(
        reinterpret_cast<const uint8_t *>(data), static_cast<int>(length));
#if GOOGLE_PROTOBUF_VERSION / 1000000 >= 3
    stream.SetTotalBytesLimit(static_cast<int>(length));
#else
    stream.SetTotalBytesLimit(static_cast<int>(length), -1 /*no warnings*/);
#endif
    msg->ParseFromCodedStream(&stream);

    if (!msg->IsInitialized()) {
      char extra[0x100];
      ::snprintf(extra, sizeof(extra), "type: %u, len: %zu, info: %s", type,
                 length, msg->InitializationErrorString().c_str());
      tcp_warn(0, "failed to parsing message", extra);
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

        ::PolarXRPC::Session::AuthenticateOk msg;
        msg.set_auth_data(r.data);
        msg_enc()
            .encode_protobuf_message<
                ::PolarXRPC::ServerMessages::SESS_AUTHENTICATE_OK>(msg);
      } break;

      case Authentication_interface::Failed:
        /// reset auth handler
        auth_handler_.reset();
        /// send error message
        msg_enc().reset_sid(sid);
        msg_enc().encode_error(PolarXRPC::Error::ERROR, r.error_code, r.data,
                               "HY000");
        break;

      default:
        /// send continue challenge
        ::PolarXRPC::Session::AuthenticateContinue msg;
        msg.set_auth_data(r.data);
        msg_enc()
            .encode_protobuf_message<
                ::PolarXRPC::ServerMessages::SESS_AUTHENTICATE_CONTINUE>(msg);
    }

    /// flush buffer
    encoder().flush(*this);
  }

  inline void auth_routine(const uint64_t &sid, msg_t &&msg) {
    switch (msg.type) {
      case PolarXRPC::ClientMessages::CON_CAPABILITIES_GET: {
        /// for alive probe
        ::PolarXRPC::Connection::Capabilities caps;
        auto cap = caps.add_capabilities();
        cap->set_name("polarx_rpc");
        auto v = cap->mutable_value();
        v->set_type(::PolarXRPC::Datatypes::Any::SCALAR);
        auto s = v->mutable_scalar();
        s->set_type(::PolarXRPC::Datatypes::Scalar::V_UINT);
        s->set_v_unsigned_int(1);

        /// send to client
        msg_enc().reset_sid(sid);
        msg_enc()
            .encode_protobuf_message<
                ::PolarXRPC::ServerMessages::CONN_CAPABILITIES>(caps);
        encoder().flush(*this);
      } break;

      case PolarXRPC::ClientMessages::SESS_AUTHENTICATE_START: {
        /// start auth
        const auto &authm =
            static_cast<const ::PolarXRPC::Session::AuthenticateStart &>(
                *msg.msg);
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

      case PolarXRPC::ClientMessages::SESS_AUTHENTICATE_CONTINUE: {
        /// continue auth
        const auto &authm =
            static_cast<const ::PolarXRPC::Session::AuthenticateContinue &>(
                *msg.msg);
        Authentication_interface::Response r;
        if (LIKELY(auth_handler_)) {
          r = auth_handler_->handle_continue(authm.auth_data());
          tcp_info(0, "continue auth",
                   r.data.empty() ? nullptr : r.data.c_str());
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
      if (s) s->run();
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

      /// recode decode time
      int64_t decode_start_time = 0;
      if (enable_perf_hist) decode_start_time = Ctime::steady_ns();

      /// start decode routine
      do {
        if (UNLIKELY(!read_lock_.try_lock())) {
          recheck_.store(true, std::memory_order_release);
          std::atomic_thread_fence(std::memory_order_seq_cst);
          if (LIKELY(!read_lock_.try_lock()))
            break;  /// do any possible notify task
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
                  /// warn
                  char extra[0x100];
                  ::snprintf(extra, sizeof(extra), "expected: %u, now: %u",
                             gx_ver_, hdr->version);
                  tcp_warn(0, "bad protocol version", extra);
                  /// shutdown
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
#if defined(__BYTE_ORDER__) && (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
              sid = __builtin_bswap64(sid);
              extra_data = __builtin_bswap32(extra_data);
#endif

              /// check extra data size
              if (extra_data > max_pkt) {
                /// send fatal message before shutdown
                msg_enc().reset_sid(sid);
                msg_enc().encode_error(PolarXRPC::Error::FATAL,
                                       ER_NET_PACKET_TOO_LARGE,
                                       "Got a packet bigger than "
                                       "'polarx_rpc_max_allowed_packet' bytes",
                                       "S1000");
                encoder().flush(*this);
                /// warn
                char extra[0x100];
                ::snprintf(extra, sizeof(extra), "max: %u, now: %u", max_pkt,
                           extra_data);
                tcp_warn(0, "max allowed packet size exceeded", extra);
                /// shutdown
                fin("max allowed packet size exceeded");
                /// fatal error and drop all data
                buf_pos_ = start_pos = 0;
                break;
              }

              auto full_pkt_size = hdr_sz_ + extra_data - 1;
              if (UNLIKELY(full_pkt_size > fixed_buf_size_))
                last_big_packet_time_ = now_time;  /// refresh dyn buf time

              /// check enough data?
              if (LIKELY(buf_pos_ - start_pos >=
                         static_cast<int>(full_pkt_size))) {
#if POLARX_RPC_PKT_DBG
                {
                  auto crc =
                      crc32(buf_ptr_ + start_pos + hdr_sz_, extra_data - 1, 0);
                  char buf[0x100];
                  ::snprintf(buf, sizeof(buf), "sid:%lu type:%u len:%u crc:%x",
                             sid, type, extra_data - 1, crc);
                  tcp_info(0, "pkt recv", buf);
                }
#endif
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
                           PolarXRPC::ClientMessages::CON_CAPABILITIES_GET))
                  sessions_.execute(*this, sid, std::move(msg), notify_set);
                else {
                  /// do auth routine
                  int64_t auth_start_time = 0;
                  if (enable_perf_hist) auth_start_time = Ctime::steady_ns();

                  auth_routine(sid, std::move(msg));

                  if (auth_start_time != 0) {
                    auto auth_end_time = Ctime::steady_ns();
                    auto auth_time = auth_end_time - auth_start_time;
                    g_auth_hist.update(static_cast<double>(auth_time) / 1e9);
                  }
                }

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
#if POLARX_RPC_PKT_DBG
                  tcp_info(0, "switch to larger buffer");
#endif
                }
                break;  /// continue receiving
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
              if (now_time - last_big_packet_time_ > 10 * 1000 &&  /// >10s
                  buf_ptr_ != fixed_buf_.get()) {                  /// dyn buf
                dyn_buf_size_ = 0;
                dyn_buf_.reset();
                buf_ptr_ = fixed_buf_.get();  /// back to fixed buf
#if POLARX_RPC_PKT_DBG
                tcp_info(0, "switch to fixed buffer");
#endif
              }
            }

          } else if (UNLIKELY(0 == iret)) {
            DBG_LOG(("read EOF"));
            fin("eof");
          } else {
            auto err = errno;
#if EAGAIN == EWOULDBLOCK
            if (err != EAGAIN) {
#else
            if (err != EAGAIN && err != EWOULDBLOCK) {
#endif
              /// ignore case of free and invoke concurrently
              bool ignore;
              {
                std::lock_guard<std::mutex> lck(exit_lock_);
                ignore = -1 == fd_ && !registered_;
              }
              if (!ignore) tcp_err(err, "recv error");
              /// fatal error
              fin("fatal error");
            }
            /// would block error, just leave it
          }

        } while (recheck_.load(std::memory_order_acquire));

        read_lock_.unlock();
        std::atomic_thread_fence(std::memory_order_seq_cst);
      } while (UNLIKELY(recheck_.load(std::memory_order_acquire)));

      /// recode decode time
      if (decode_start_time != 0) {
        auto decode_end_time = Ctime::steady_ns();
        auto decode_time = decode_end_time - decode_start_time;
        g_decode_hist.update(static_cast<double>(decode_time) / 1e9);
      }

      /// dealing notify or direct run outside the read lock.
      if (!notify_set.empty()) {
        /// last one in event queue and the last one in notify set,
        /// can run directly, or the trx session in last event(if exists)
        /// record the sid which is in trx
        uint64_t direct_run_sid = UINT64_C(0xFFFFFFFFFFFFFFFF);
        auto cnt = notify_set.size();
        for (const auto &pair : notify_set) {
          const auto &sid = pair.first;
          const auto &in_trx = pair.second;
          --cnt;
          /// record it if empty
          if (index + 1 == total &&
              UINT64_C(0xFFFFFFFFFFFFFFFF) == direct_run_sid &&
              (in_trx || 0 == cnt)) {
            /// in last event and no other direct run
            /// in trx or last one in set
            assert(sid != UINT64_C(0xFFFFFFFFFFFFFFFF));
            direct_run_sid = sid;
          } else {
            /// schedule in work task
            DBG_LOG(("tcp_conn schedule task session %lu", sid));
            std::unique_ptr<CdelayedTask> task(new CdelayedTask(sid, this));
            auto bret = epoll_.push_work(task->gen_task());
            if (LIKELY(bret))
              task.release();  /// release the owner to queue
            else {
              /// queue is full, try and kill if timeout
              task.reset();
              if (LIKELY(sessions_.remove_and_shutdown(epoll_.session_count(),
                                                       sid, true))) {
                /// send fatal msg()(not lock protected so new one)
                CpolarxEncoder enc(sid);
                enc.message_encoder().encode_error(
                    PolarXRPC::Error::FATAL, ER_POLARX_RPC_ERROR_MSG,
                    "Work queue overflow. Larger "
                    "'polarx_rpc_epoll_work_queue_capacity' needed.",
                    "70100");
                enc.flush(*this);
              }
            }
          }
        }
        /// run session in trx directly
        if (direct_run_sid != UINT64_C(0xFFFFFFFFFFFFFFFF)) {
          DBG_LOG(("tcp_conn run session %lu directly", direct_run_sid));
          assert(index + 1 == total);  /// must be last event
          auto s = sessions_.get_session(direct_run_sid);
          if (s) s->run();
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
      : epoll_(epoll),
        tcp_id_(id),
        ip_(std::forward<std::string>(ip)),
        host_(std::forward<std::string>(host)),
        port_(port),
        fd_(fd),
        recheck_(false),
        is_gx_(false),
        gx_ver_(0),
        hdr_sz_(13),
        fixed_buf_size_(0),
        dyn_buf_size_(0),
        last_big_packet_time_(0),
        buf_ptr_(nullptr),
        buf_pos_(0),
        registered_(false),
        reference_(1),
        encoder_(0),
        authed_(false),
        sessions_(host_, port) {
    DBG_LOG(("new TCP context %s:%u", host_.c_str(), port_));
    plugin_info.tcp_connections.fetch_add(1, std::memory_order_release);

    /// set TCP buf size if needed
    auto buf_size = tcp_send_buf;
    if (buf_size != 0) {
      if (UNLIKELY(buf_size < MIN_TCP_BUF))
        buf_size = MIN_TCP_BUF;
      else if (UNLIKELY(buf_size > MAX_TCP_BUF))
        buf_size = MAX_TCP_BUF;
    }
    auto err = Csocket::set_sndbuf(fd_, buf_size);
    if (err != 0) tcp_warn(-err, "failed to set sndbuf");
    buf_size = tcp_recv_buf;
    if (buf_size != 0) {
      if (UNLIKELY(buf_size < MIN_TCP_BUF))
        buf_size = MIN_TCP_BUF;
      else if (UNLIKELY(buf_size > MAX_TCP_BUF))
        buf_size = MAX_TCP_BUF;
    }
    err = Csocket::set_rcvbuf(fd_, buf_size);
    if (err != 0) tcp_warn(-err, "failed to set rcvbuf");

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

    /// register in epoll
    epoll_.register_tcp(this);
  }

  ~CtcpConnection() final {
    /// unregister in epoll
    epoll_.unregister_tcp(this);

    plugin_info.tcp_closing.fetch_sub(1, std::memory_order_release);
    plugin_info.tcp_connections.fetch_sub(1, std::memory_order_release);
    tcp_info(0, "destruct");
  }

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
    if (UNLIKELY(fd_ < 0)) return false;
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
            fin("send error while wait");
            return false;
          }
          /// wait done and retry
        } else if (EINTR == err) {
          if (++retry >= 10) {
            tcp_warn(EINTR, "send error with EINTR after retry 10");
            fin("send error after retry 10");
            return false;
          }
          /// simply retry
        } else {
          /// fatal error
          tcp_err(err, "send error");
          fin("send error");
          return false;
        }
      } else {
        retry = 0;  /// clear retry
        ptr += iret;
        length -= iret;
      }
    }
    return true;
  }
};

}  // namespace polarx_rpc
