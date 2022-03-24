/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  data_storage.h,v 1.0 08/12/2016 02:46:50 PM hangfeng.fj(hangfeng.fj@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file data_storage.h
 * @author hangfeng.fj(hangfeng.fj@alibaba-inc.com)
 * @date 08/12/2016 02:15:50 PM
 * @version 1.0
 * @brief 
 *
 **/

#ifndef  cluster_data_storage_INC
#define  cluster_data_storage_INC

#include <map>
#include <string>
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rd_raft_log.h"
#include "raft.h"
#include "rocksdb/options.h"

namespace alisql {

/**
 * @class DataStorage
 *
 * @brief class for DataStorage
 *
 **/
class DataStorage {
  public:
    DataStorage(const std::string &dataDir, rocksdb::Options &options);
    virtual ~DataStorage();

    int openDataBase(const std::string &name);
    void closeDataBase(const std::string &name);

    //name is db name, default is ""
    virtual int get(const std::string &name, const std::string &key, std::string *value);
    virtual int set(const std::string &name, const std::string &key, 
                    const std::string &value, uint64_t lastAppliedIndex);
    virtual int del(const std::string &name, const std::string &key, 
                    uint64_t lastAppliedIndex);
    int getDbByName(const std::string &name, rocksdb::DB **db);
    static const std::string defaultUser;
    static const std::string lastAppliedIndexTag;
  private:
    std::string dataDir_;
    std::map<std::string, rocksdb::DB*> dbs_;
};/* end of class DataStorage */

};/* end of namespace alisql */

#endif     //#ifndef cluster_data_storage_INC 
