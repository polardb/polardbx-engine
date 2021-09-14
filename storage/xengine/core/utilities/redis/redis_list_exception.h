/**
 * A simple structure for exceptions in RedisLists.
 *
 * @author Deon Nicholas (dnicholas@fb.com)
 * Copyright 2013 Facebook
 */

#ifndef ROCKSDB_LITE
#pragma once
#include <exception>

namespace xengine {
namespace util {

class RedisListException : public std::exception {
 public:
  const char* what() const throw() override {
    return "Invalid operation or corrupt data in Redis List.";
  }
};

}  //  namespace util
}  //  namespace xengine
#endif
