/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  client_service.h,v 1.0 08/23/2016 10:02:33 AM
 *yingqiang.zyq(yingqiang.zyq@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file client_service.h
 * @author yingqiang.zyq(yingqiang.zyq@alibaba-inc.com)
 * @date 08/23/2016 10:02:33 AM
 * @version 1.0
 * @brief
 *
 **/

#ifndef client_service_INC
#define client_service_INC

#include <cstring>
#include <map>
#include <string>
#include "easyNet.h"

namespace alisql {

class Paxos;

/**
 * @class ClientService
 *
 * @brief
 *
 **/
class ClientService {
 public:
  ClientService() = default;
  virtual ~ClientService() = default;

  const std::string &get(const std::string &key) { return map_[key]; }
  void set(const std::string &key, const std::string &val);
  const std::string set(const char *strKeyVal, uint64_t len);
  int serviceProcess(easy_request_t *r, void *args);

 protected:
  std::map<const std::string, const std::string> map_;

 private:
  ClientService(const ClientService &other);  // copy constructor
  const ClientService &operator=(
      const ClientService &other);            // assignment operator

};                                            /* end of class ClientService */

}  // namespace alisql

#endif  // #ifndef client_service_INC
