/**
 * Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.
 */

#include <iostream>
#include <sstream>
#include "encdb.h"
#include "mysql/service_security_context.h" /* Security context service */
#include "plugin.h"
#include "sql/auth/auth_acls.h"
#include "sql/auth/sql_acl.h"
#include "sql/sql_class.h"
#include "storage.h"

#define MYSQL_RESULT_SIZE 255
#define UDF_GET_USERNAME() \
  (std::string(current_thd->security_context()->user().str))
using std::string;

extern void *encdb_plugin_malloc(size_t size);
extern void encdb_plugin_free(void *ptr);

const char REQUEST_ERR_MSG[] =
    "{\"status\":4026990592, \"body\":\"request is empty\"}";

// UDFs
inline char *handle_udf_malloc_error(char *error, char *is_null) {
  *error = 1;
  *is_null = 1;
  return NULL;
}

/*
 * encdb_process_message is a function used by encdb users.
 * It provides key functions to key management and encryption rule setting for
 * encdb. A sample usage: SELECT encdb_process_message('{"request_type":0}');
 */
extern "C" bool encdb_process_message_init(UDF_INIT *initid, UDF_ARGS *args,
                                           char *message) {
  if (args->arg_count != 1) {
    strcpy(message, "encdb_process_message expects 1 argument");
    return 1;
  }
  initid->ptr = NULL;
  return 0;
}
extern "C" char *encdb_process_message(UDF_INIT *initid, UDF_ARGS *args,
                                       char *result, unsigned long *length,
                                       char *is_null, char *error) {
  if (!args->args[0] || !args->lengths[0]) {
    *length = sizeof(REQUEST_ERR_MSG);
    return const_cast<char *>(REQUEST_ERR_MSG);
  }
  const std::string message(args->args[0], args->lengths[0]);
  std::string tmp_mac;
  char *ret_buf = result;
  std::string ret;
  encdb::KeyStore::processKeyRequest(message, UDF_GET_USERNAME(), "", tmp_mac,
                                     ret);
  if (ret.size() > MYSQL_RESULT_SIZE) {
    if (initid->ptr != NULL) {
      encdb_plugin_free(initid->ptr);
    }
    ret_buf = initid->ptr = (char *)encdb_plugin_malloc(ret.size());
    if (ret_buf == NULL) {
      return handle_udf_malloc_error(error, is_null);
    }
  }

  memcpy(ret_buf, ret.c_str(), ret.size());
  *length = ret.size();
  return ret_buf;
}
extern "C" void encdb_process_message_deinit(UDF_INIT *initid) {
  if (initid->ptr != NULL) {
    encdb_plugin_free(initid->ptr);
  }
}

/*
 * A UDF used for debugging purpose. This function exports all rules inside
 * memory. Since some times sensitive rules can be inconsistent between memory
 * cache and db storage, this function can help debugging.
 */
extern "C" bool encdb_rule_export_init(UDF_INIT *initid, UDF_ARGS *, char *) {
  initid->ptr = NULL;
  return 0;
}
extern "C" char *encdb_rule_export(UDF_INIT *initid, UDF_ARGS *, char *result,
                                   unsigned long *length, char *is_null,
                                   char *error) {
  std::string err_msg;
  std::string rules = encdb::KeyStore::ruleExport(err_msg);
  if (!err_msg.empty()) {
    my_error(ER_ENCDB_UDF_ERROR, MYF(0), "encdb_rule_export", err_msg.c_str());
    *error = 1;
    return NULL;
  }
  char *ret_buf = result;
  if (rules.size() > MYSQL_RESULT_SIZE) {
    ret_buf = initid->ptr = (char *)encdb_plugin_malloc(rules.size());
    if (initid->ptr == NULL) {
      return handle_udf_malloc_error(error, is_null);
    }
  }

  memcpy(ret_buf, rules.data(), rules.size());
  *length = rules.size();
  return ret_buf;
}

extern "C" void encdb_rule_export_deinit(UDF_INIT *initid) {
  if (initid->ptr != NULL) {
    encdb_plugin_free(initid->ptr);
  }
}

extern "C" bool encdb_userauth_export_init(UDF_INIT *initid, UDF_ARGS *,
                                           char *) {
  initid->ptr = NULL;
  return 0;
}
extern "C" char *encdb_userauth_export(UDF_INIT *initid, UDF_ARGS *,
                                       char *result, unsigned long *length,
                                       char *is_null, char *error) {
  std::string err_msg;
  std::string userauth = encdb::KeyStore::userAuthExport(err_msg);
  if (!err_msg.empty()) {
    my_error(ER_ENCDB_UDF_ERROR, MYF(0), "encdb_userauth_export",
             err_msg.c_str());
    *error = 1;
    return NULL;
  }
  char *ret_buf = result;
  if (userauth.size() > MYSQL_RESULT_SIZE) {
    ret_buf = initid->ptr = (char *)encdb_plugin_malloc(userauth.size());
    if (initid->ptr == NULL) {
      return handle_udf_malloc_error(error, is_null);
    }
  }

  memcpy(ret_buf, userauth.data(), userauth.size());
  *length = userauth.size();
  return ret_buf;
}

extern "C" void encdb_userauth_export_deinit(UDF_INIT *initid) {
  if (initid->ptr != NULL) {
    encdb_plugin_free(initid->ptr);
  }
}

extern "C" bool encdb_reload_rules_init(UDF_INIT *, UDF_ARGS *, char *) {
  return 0;
}

/*
 * This function forces a RELOAD operation on database stored sensitive rules.
 * After this function, it's guaranteed that all rules in database tables will
 * be inside memory cache as well.
 */
extern "C" char *encdb_reload_rules(UDF_INIT *, UDF_ARGS *, char *result,
                                    unsigned long *length, char *, char *) {
  result[0] = 'O';
  result[1] = 'K';
  *length = 2;
  encdb::KeyStore::reloadAllRules();
  return result;
}
extern "C" void encdb_reload_rules_deinit(UDF_INIT *) {}

inline bool is_super_user() {
  return current_thd->security_context()->check_access(SUPER_ACL);
}

/*
 * This function decrypts encrypted data. Since it's highly risky, the function
 * can only be called by Aliyun DBA aurora.
 */
extern "C" bool encdb_decrypt_init(UDF_INIT *initid, UDF_ARGS *args,
                                   char *message) {
  if (args->arg_count != 2) {
    strcpy(message,
           "encdb_decrypt expects 2 arguments, ciphertext and username");
    return 1;
  }
  initid->ptr = NULL;
  if (!is_super_user()) {
    strcpy(message, "encdb_decrypt can only be executed by superuser");
    return 1;
  }
  return 0;
}
extern "C" char *encdb_decrypt(UDF_INIT *initid, UDF_ARGS *args, char *result,
                               unsigned long *length, char *is_null,
                               char *error) {
  if (!args->args[0] || !args->args[1]) {
    *error = 1;
    *is_null = 1;
    return NULL;
  }
  const char *cipher_text = args->args[0];
  uint32_t cipher_size = args->lengths[0];

  std::string uname =
      std::string((const char *)args->args[1], args->lengths[1]);
  char *plain_text = result;
  if (cipher_size > MYSQL_RESULT_SIZE) {
    if (initid->ptr != NULL) {
      encdb_plugin_free(initid->ptr);
    }
    plain_text = initid->ptr = (char *)encdb_plugin_malloc(cipher_size);
    if (plain_text == NULL) {
      return handle_udf_malloc_error(error, is_null);
    }
  }

  uint32_t plain_size = cipher_size;
  // call decrypt
  uint8_t type;
  EncdbStatus status = encdb::EncdbCipher::decrypt(
      uname, reinterpret_cast<uint8_t *>(plain_text), &plain_size, &type,
      reinterpret_cast<const uint8_t *>(cipher_text), cipher_size);
  if (status != 0) {
    // error handling
    *error = 1;
    *is_null = 1;
    return NULL;
  }
  *length = plain_size;
  return plain_text;
}

extern "C" void encdb_decrypt_deinit(UDF_INIT *initid) {
  if (initid->ptr != NULL) {
    encdb_plugin_free(initid->ptr);
  }
}

/*
 * This function is used for test only, it can help us to import MEK on command
 * line. Sample usage: select
 * encdb_process_message(encdb_generate_mek_request(encdb_process_message(
 * '{"request_type":0}'),'00112233445566778899aabbccddeeff'));
 */
extern "C" bool encdb_generate_mek_request_init(UDF_INIT *initid,
                                                UDF_ARGS *args, char *message) {
  if (args->arg_count < 2) {
    strcpy(
        message,
        "encdb_generate_mek_request expects 2 or 3 arguments, server_info and "
        "mek (hex string) and algo");
    return 1;
  }
  initid->ptr = NULL;
  return 0;
}

extern "C" char *encdb_generate_mek_request(UDF_INIT *initid, UDF_ARGS *args,
                                            char *result, unsigned long *length,
                                            char *is_null, char *error) {
  const char *server_info = args->args[0];
  uint32_t server_info_size = args->lengths[0];
  std::string server_info_str(server_info, server_info_size);
  const char *mek = args->args[1];
  uint32_t mek_size = args->lengths[1];
  std::string algo;
  if (args->arg_count == 3) {
    const char *alg = args->args[2];
    algo.assign(alg);
  }
  std::string mek_str(mek, mek_size);
  std::string request;
  if (algo.empty())
    request = encdb::ForTestOnly::generateMekProvisionMsgFromServerInfo(
        server_info_str, mek_str);
  else
    request = encdb::ForTestOnly::generateMekProvisionMsgFromServerInfo(
        server_info_str, mek_str, algo);
  char *ret_buf = result;
  if (request.size() > MYSQL_RESULT_SIZE) {
    if (initid->ptr != NULL) {
      encdb_plugin_free(initid->ptr);
    }
    ret_buf = initid->ptr = (char *)encdb_plugin_malloc(request.size());
    if (ret_buf == NULL) {
      return handle_udf_malloc_error(error, is_null);
    }
  }

  memcpy(ret_buf, request.data(), request.size());
  *length = request.size();
  return ret_buf;
}

extern "C" void encdb_generate_mek_request_deinit(UDF_INIT *initid) {
  if (initid->ptr != NULL) {
    encdb_plugin_free(initid->ptr);
  }
}

/*
 * This function is used for test only. It generates a MEK_UPDATE (local MEK
 * rotation) request that drives rotate_local_mek() on the server side, using a
 * server_info generated from encdb_process_message. Sample usage:
 * select encdb_process_message(encdb_generate_mek_update_request(
 *   encdb_process_message('{"request_type":0, "version":"1.2.15"}'),
 *   '<old_hex_mek>', '<new_hex_mek>'));
 */
extern "C" bool encdb_generate_mek_update_request_init(UDF_INIT *initid,
                                                       UDF_ARGS *args,
                                                       char *message) {
  if (args->arg_count != 3) {
    strcpy(message,
           "encdb_generate_mek_update_request expects 3 arguments, server_info "
           "and old_mek (hex string) and new_mek (hex string)");
    return 1;
  }
  initid->ptr = NULL;
  return 0;
}

extern "C" char *encdb_generate_mek_update_request(UDF_INIT *initid,
                                                   UDF_ARGS *args, char *result,
                                                   unsigned long *length,
                                                   char *is_null, char *error) {
  std::string server_info_str(args->args[0], args->lengths[0]);
  std::string old_mek_str(args->args[1], args->lengths[1]);
  std::string new_mek_str(args->args[2], args->lengths[2]);
  std::string request = encdb::ForTestOnly::generateMekUpdateMsgFromServerInfo(
      server_info_str, old_mek_str, new_mek_str);
  char *ret_buf = result;
  if (request.size() > MYSQL_RESULT_SIZE) {
    if (initid->ptr != NULL) {
      encdb_plugin_free(initid->ptr);
    }
    ret_buf = initid->ptr = (char *)encdb_plugin_malloc(request.size());
    if (ret_buf == NULL) {
      return handle_udf_malloc_error(error, is_null);
    }
  }

  memcpy(ret_buf, request.data(), request.size());
  *length = request.size();
  return ret_buf;
}

extern "C" void encdb_generate_mek_update_request_deinit(UDF_INIT *initid) {
  if (initid->ptr != NULL) {
    encdb_plugin_free(initid->ptr);
  }
}
/*
 * A function that supports command line operation on user authorization and
 * sensitive rules management.
 */
extern "C" bool encdb_rule_op_init(UDF_INIT *initid, UDF_ARGS *args,
                                   char *message) {
  if (args->arg_count != 2) {
    strcpy(message,
           "encdb_rule_op expects 2 arguments, op (add, update, grant or "
           "delete) and param (json string)");
    return 1;
  }
  initid->ptr = NULL;
  return 0;
}

extern "C" char *encdb_rule_op(UDF_INIT *initid, UDF_ARGS *args, char *result,
                               unsigned long *length, char *is_null,
                               char *error) {
  if (!args->args[0] || !args->args[1]) {
    my_error(ER_ENCDB_UDF_ERROR, MYF(0), "encdb_rule_op",
             "NULL argument is not allowed");
    *error = 1;
    return NULL;
  }
  std::string op_str((const char *)args->args[0], args->lengths[0]);
  std::string param_str((const char *)args->args[1], args->lengths[1]);
  std::string err_msg;
  std::string ret = encdb::KeyStore::ruleOp(err_msg, op_str, param_str);
  if (!err_msg.empty()) {
    my_error(ER_ENCDB_UDF_ERROR, MYF(0), "encdb_rule_op", err_msg.c_str());
    *error = 1;
    return NULL;
  }
  char *ret_buf = result;
  if (ret.size() > MYSQL_RESULT_SIZE) {
    ret_buf = initid->ptr = (char *)encdb_plugin_malloc(ret.size());
    if (ret_buf == NULL) {
      return handle_udf_malloc_error(error, is_null);
    }
  }

  memcpy(ret_buf, ret.data(), ret.size());
  *length = ret.size();
  return ret_buf;
}
extern "C" void encdb_rule_op_deinit(UDF_INIT *initid) {
  if (initid->ptr != NULL) {
    encdb_plugin_free(initid->ptr);
  }
}
