/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  data_storage.cc,v 1.0 08/12/2016 02:15:50 PM hangfeng.fj(hangfeng.fj@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file data_storage.cc
 * @author hangfeng.fj(hangfeng.fj@alibaba-inc.com)
 * @date 08/12/2016 02:15:50 PM
 * @version 1.0
 * @brief 
 *
 **/

#include "data_storage.h"
#include "files.h"
#include <gflags/gflags.h>

namespace alisql {

const std::string DataStorage::defaultUser= "";
const std::string DataStorage::lastAppliedIndexTag= "@lastAppliedIndexTag@";

DataStorage::DataStorage(const std::string &dataDir, rocksdb::Options &options)
  : dataDir_(dataDir) 
{
  bool ok= mkdirs(dataDir.c_str());

  if (!ok) 
  {
    LOG_INFO("failed to create dir :%s\n", dataDir.c_str());
    abort();
  }

  // Create default database for shared namespace, i.e. anonymous user
  std::string full_name= dataDir + "/@db";
  LOG_INFO("[rocksdb data storage]: writer_buffer_size: %lld\n", options.write_buffer_size);
  rocksdb::DB* default_db= NULL;
  rocksdb::Status status= rocksdb::DB::Open(options, full_name, &default_db);
  assert(status.ok());
  dbs_[""]= default_db;
}


DataStorage::~DataStorage()
{
  for (std::map<std::string, rocksdb::DB*>::iterator it= dbs_.begin();
       it != dbs_.end(); ++it) 
  {
    delete it->second;
    it->second= NULL;
  }
}

int DataStorage::openDataBase(const std::string &name) 
{}

void DataStorage::closeDataBase(const std::string &name)
{}

int DataStorage::getDbByName(const std::string &name, rocksdb::DB **db) 
{
  if (dbs_.find(name) == dbs_.end()) 
  {
    LOG_INFO("Inexist user %s\n", name.c_str());      
    return -1;
  }

  *db= dbs_[name];
  if (*db == NULL) 
  {
    LOG_INFO("The database %s is closed!\n", name.c_str());    
    return -1;
  }

  return 0;
}

int DataStorage::get(const std::string &name, const std::string &key, std::string *value)
{
  if (value == NULL)
  {
    LOG_INFO("invalid value address!\n");
    return -1;
  }

  rocksdb::DB *db= NULL;

  int ret= getDbByName(name, &db);
  if (ret == -1)
  { 
    return ret;
  }

  rocksdb::Status status = db->Get(rocksdb::ReadOptions(), key, value);
  return (status.ok()) ? 0 : -1;
}

int DataStorage::set(const std::string &name, const std::string &key, 
                     const std::string &value, uint64_t lastAppliedIndex) 
{
  rocksdb::DB *db= NULL;

  int ret= getDbByName(name, &db);
  if (ret == -1)
  { 
    return ret;
  }

  rocksdb::WriteOptions write_options;
  write_options.sync= false;
  rocksdb::WriteBatch batch;
  batch.Put(key, value);
  batch.Put(DataStorage::lastAppliedIndexTag, RDRaftLog::intToString(lastAppliedIndex));
  rocksdb::Status status= db->Write(write_options, &batch);
  return (status.ok()) ? 0 : -1;
}

int DataStorage::del(const std::string &name, const std::string &key, 
                     uint64_t lastAppliedIndex)
{
  rocksdb::DB *db= NULL;

  int ret= getDbByName(name, &db);
  if (ret == -1)
  { 
    return ret;
  }

  rocksdb::WriteOptions write_options;
  write_options.sync= false;
  rocksdb::WriteBatch batch;
  batch.Delete(key);
  batch.Put(DataStorage::lastAppliedIndexTag, RDRaftLog::intToString(lastAppliedIndex));
  rocksdb::Status status= db->Write(write_options, &batch);
  return (status.ok()) ? 0 : -1;
}


} //namespace alisql
