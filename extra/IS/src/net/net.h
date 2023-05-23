/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  net.h,v 1.0 07/26/2016 04:45:41 PM
 *yingqiang.zyq(yingqiang.zyq@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file net.h
 * @author yingqiang.zyq(yingqiang.zyq@alibaba-inc.com)
 * @date 07/26/2016 04:45:41 PM
 * @version 1.0
 * @brief
 *
 **/

#ifndef cluster_net_INC
#define cluster_net_INC

#include <easy_io.h>
#include <string>

namespace alisql {

class NetServer : public std::enable_shared_from_this<NetServer> {
 public:
  NetServer() : c(nullptr) {}
  virtual ~NetServer() {}
  std::string strAddr;
  void *c;
};
typedef std::shared_ptr<NetServer> NetServerRef;

/**
 * @class Net
 *
 * @brief interface of net module
 *
 **/
class Net {
 public:
  Net() {}
  virtual ~Net() {}

  /* TODO here we should use a general handler. */
  virtual easy_addr_t createConnection(const std::string &addr,
                                       NetServerRef server, uint64_t timeout,
                                       uint64_t index) = 0;
  virtual void disableConnection(easy_addr_t addr) = 0;
  virtual int sendPacket(easy_addr_t addr, const char *buf, uint64_t len,
                         uint64_t id) = 0;
  virtual int sendPacket(easy_addr_t addr, const std::string &buf,
                         uint64_t id) = 0;
  virtual int setRecvPacketCallback(void *handler) = 0;

  virtual int init(void *ptr) = 0;
  virtual int start(int portArg) = 0;
  virtual int shutdown() = 0;

 protected:
 private:
  Net(const Net &other);                   // copy constructor
  const Net &operator=(const Net &other);  // assignment operator

};                                         /* end of class Net */

}  // namespace alisql

#endif  // #ifndef cluster_net_INC
