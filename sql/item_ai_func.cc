/*****************************************************************************

Copyright (c) 2013, 2026, Alibaba and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
limited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/
/**
  @file sql/item_ai_func.cc
  Implementation of AI built-in functions.

  Uses OpenSSL (already linked in mysqld) for HTTPS communication with
  OpenAI-compatible LLM API endpoints.

  Uses rapidjson (bundled) for JSON construction and parsing.
*/

#include "sql/item_ai_func.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <string>
#include <vector>

#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/ssl.h>

#include "my_rapidjson_size_t.h"

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "my_dbug.h"
#include "mysqld.h"  // lower_case_table_names, files_charset_info
#include "mysqld_error.h"
#include "sql/ai/ai_cache.h"  // im::Ai_model_cache, im::get_global_api_key
#include "sql/auth/auth_acls.h"             // SELECT_ACL
#include "sql/auth/auth_common.h"           // check_table_access, get_column_grant
#include "sql/dd/cache/dictionary_client.h"  // dd::cache::Dictionary_client
#include "sql/dd/dd_schema.h"                // dd::Schema_MDL_locker
#include "sql/dd/types/column.h"             // dd::Column
#include "sql/dd/types/index.h"              // dd::Index
#include "sql/dd/types/index_element.h"      // dd::Index_element
#include "sql/dd/types/schema.h"             // dd::Schema
#include "sql/dd/types/table.h"              // dd::Table
#include "sql/sql_class.h"
#include "sql/sql_show.h"  // append_identifier
#include "vidx/vidx_common.h"  // vidx::from_vector_to_string, VECTOR_PRECISION

/**
  Resolve the effective API key for an AI model.
  If the model has its own api_key (user-supplied), use it.
  Otherwise fall back to the DASHSCOPE_API_KEY environment variable captured
  at startup.
  Returns an empty string only if all sources are empty.
*/
static std::string resolve_api_key(const im::Ai_cached_model &cached) {
  if (!cached.api_key.empty()) return cached.api_key;
  return im::resolve_global_api_key();
}

/**
  Treat a NULL input as a NULL output silently, following the SQL scalar
  function convention (UPPER(NULL)=NULL, ABS(NULL)=NULL).  A NULL value in
  one row must not abort the entire SELECT/UPDATE batch.

  @param arg          Item argument to test.
  @param null_value   Out: set to true when the argument is NULL.
  @return true if the argument is NULL and the caller should propagate NULL.
*/
static inline bool ai_arg_is_null(Item *arg, bool *null_value) {
  if (arg->null_value) {
    *null_value = true;
    return true;
  }
  return false;
}

/**
  Push a non-fatal warning to indicate that an empty-string input was
  silently converted to NULL.  Used by AI functions (currently only
  AI_EMBEDDING) whose upstream services reject empty input but where
  aborting the whole statement on a single empty row is undesirable.
*/
static inline void ai_warn_empty_input(THD *thd, const char *fn_name) {
  push_warning_printf(thd, Sql_condition::SL_WARNING, ER_WRONG_ARGUMENTS,
                      "%s: empty input text returned NULL", fn_name);
}

/**
  Recognize a VECTOR-typed Item argument (e.g. a VECTOR(N) column or the
  output of VEC_FROMTEXT) whose runtime value is packed float32 binary, and
  convert it to the textual JSON-array form so callers that consume
  vector-as-text (parse_json_vector) work uniformly.

  Detection mirrors Item_func_vec_totext::resolve_type(): a STRING_RESULT
  Item with the binary collation and a length that is a positive multiple
  of VECTOR_PRECISION is treated as a VECTOR binary value.

  This is intentionally a runtime fallback: callers should still recognize
  obvious JSON-text vectors (strings starting with '[') first, so plain
  text vectors authored as string literals continue to work without an
  extra conversion.

  @param arg       Item argument.
  @param raw_str   String value of the argument (already obtained via
                   args[i]->val_str()).  Must be non-null.
  @param out_buf   Out: receives the textual JSON-array vector on success.
  @return true if @c raw_str was recognized as VECTOR binary and converted;
          false otherwise (caller should fall back to the original
          text-based detection).
*/
static bool try_extract_vector_text(Item *arg, String *raw_str,
                                    String *out_buf) {
  if (arg == nullptr || raw_str == nullptr || raw_str->ptr() == nullptr) {
    return false;
  }
  if (arg->result_type() != STRING_RESULT) return false;
  if (arg->collation.collation != &my_charset_bin) return false;
  const size_t len = raw_str->length();
  if (len == 0 || (len % vidx::VECTOR_PRECISION) != 0) return false;

  if (vidx::from_vector_to_string(raw_str, vidx::VECTOR_PRECISION,
                                  &my_charset_numeric, out_buf)) {
    return false;
  }
  return true;
}

static bool find_default_ai_model(const char *fn_name,
                                  const char *function_name,
                                  im::Ai_cached_model *cached,
                                  bool *null_value) {
  std::string default_model_name;
  im::Ai_default_model_lookup lookup =
      im::Ai_model_cache::instance()->find_default_model(
          function_name, cached, &default_model_name);
  if (lookup == im::AI_DEFAULT_MODEL_FOUND) return false;

  if (lookup == im::AI_DEFAULT_MODEL_MISSING) {
    my_printf_error(ER_WRONG_ARGUMENTS,
                    "%s: default model '%s' for %s not found in "
                    "ai_model_config.",
                    MYF(0), fn_name, default_model_name.c_str(),
                    function_name);
  } else {
    my_printf_error(ER_WRONG_ARGUMENTS,
                    "%s: no default model configured for %s.", MYF(0),
                    fn_name, function_name);
  }
  *null_value = true;
  return true;
}

static bool find_default_ai_model(const char *fn_name,
                                  const char *function_name,
                                  im::Ai_cached_model *cached,
                                  std::string *error_msg) {
  std::string default_model_name;
  im::Ai_default_model_lookup lookup =
      im::Ai_model_cache::instance()->find_default_model(
          function_name, cached, &default_model_name);
  if (lookup == im::AI_DEFAULT_MODEL_FOUND) return false;

  if (lookup == im::AI_DEFAULT_MODEL_MISSING) {
    *error_msg = std::string(fn_name) + ": default model '" +
                 default_model_name + "' for " + function_name +
                 " not found in ai_model_config.";
  } else {
    *error_msg = std::string(fn_name) +
                 ": no default model configured for " + function_name + ".";
  }
  return true;
}

/**
  @defgroup AI_HTTP_Internals  HTTP helpers for AI functions
  @{
*/


/**
  Parse a URL into scheme, host, port, path.
  Supports http:// and https:// schemes.

  @param[in]  url   Full URL string
  @param[out] host  Extracted hostname
  @param[out] port  Extracted port (443 for https, 80 for http by default)
  @param[out] path  Extracted path (including leading /)
  @param[out] use_ssl  Whether to use SSL

  @retval false  Success
  @retval true   Parse error
*/
static bool parse_url(const char *url, std::string *host, int *port,
                      std::string *path, bool *use_ssl) {
  std::string url_str(url);
  size_t scheme_end = url_str.find("://");
  if (scheme_end == std::string::npos) return true;

  std::string scheme = url_str.substr(0, scheme_end);
  if (scheme == "https") {
    *use_ssl = true;
    *port = 443;
  } else if (scheme == "http") {
    *use_ssl = false;
    *port = 80;
  } else {
    return true;
  }

  size_t host_start = scheme_end + 3;
  size_t path_start = url_str.find('/', host_start);
  std::string host_port;
  if (path_start == std::string::npos) {
    host_port = url_str.substr(host_start);
    *path = "/";
  } else {
    host_port = url_str.substr(host_start, path_start - host_start);
    *path = url_str.substr(path_start);
  }

  size_t colon = host_port.find(':');
  if (colon != std::string::npos) {
    *host = host_port.substr(0, colon);
    const char *port_str = host_port.c_str() + colon + 1;
    char *endptr = nullptr;
    long lport = strtol(port_str, &endptr, 10);
    if (endptr == port_str || *endptr != '\0' || lport <= 0 || lport > 65535) {
      return true;  /* malformed port */
    }
    *port = static_cast<int>(lport);
  } else {
    *host = host_port;
  }
  return false;
}

/**
  Write callback that appends data to a std::string.
*/
struct HttpResponse {
  std::string body;
  bool headers_done = false;
  std::string header_buf;
  int status_code = 0;
};

/**
  Case-insensitive search for an HTTP header value.

  @param headers   The raw header string (everything before \r\n\r\n)
  @param name      Header name to search for (case-insensitive, without colon)
  @param value     [out] The header value if found

  @retval true   Header found
  @retval false  Header not found
*/
static bool find_header_value(const std::string &headers,
                              const char *name, std::string *value) {
  std::string lower_headers = headers;
  std::string lower_name = name;
  for (auto &c : lower_headers) c = std::tolower(static_cast<unsigned char>(c));
  for (auto &c : lower_name) c = std::tolower(static_cast<unsigned char>(c));
  lower_name.append(": ");

  size_t pos = lower_headers.find(lower_name);
  if (pos == std::string::npos) return false;

  size_t val_start = pos + lower_name.length();
  size_t val_end = headers.find("\r\n", val_start);
  if (val_end == std::string::npos) val_end = headers.length();
  *value = headers.substr(val_start, val_end - val_start);
  return true;
}

/**
  Decode a chunked transfer-encoded body.

  Chunked format:
    <chunk-size-hex>\r\n
    <chunk-data>\r\n
    ...
    0\r\n
    \r\n

  @param raw_body   The raw body with chunk framing
  @return           Decoded body content
*/
static std::string decode_chunked_body(const std::string &raw_body) {
  std::string decoded;
  size_t pos = 0;

  while (pos < raw_body.size()) {
    /* Find the end of the chunk size line */
    size_t line_end = raw_body.find("\r\n", pos);
    if (line_end == std::string::npos) break;

    /* Parse chunk size (hex) */
    std::string size_str = raw_body.substr(pos, line_end - pos);
    /* Chunk size may have extensions after ';', strip them */
    size_t semi = size_str.find(';');
    if (semi != std::string::npos) size_str = size_str.substr(0, semi);
    /* Trim whitespace */
    while (!size_str.empty() && std::isspace(static_cast<unsigned char>(size_str.back())))
      size_str.pop_back();

    unsigned long chunk_size = 0;
    try {
      chunk_size = std::stoul(size_str, nullptr, 16);
    } catch (...) {
      break;  /* Not a valid chunk size, stop */
    }

    if (chunk_size == 0) break;  /* Final chunk */

    size_t data_start = line_end + 2;  /* skip \r\n after size */
    if (data_start + chunk_size > raw_body.size()) {
      /* Incomplete chunk, take what we have */
      decoded.append(raw_body, data_start, raw_body.size() - data_start);
      break;
    }

    decoded.append(raw_body, data_start, chunk_size);
    pos = data_start + chunk_size + 2;  /* skip chunk data + \r\n */
  }

  return decoded;
}

/**
  Read all available data from BIO/socket until connection is closed or
  Content-Length is satisfied. Handles both Content-Length and
  Transfer-Encoding: chunked responses.

  @param bio        OpenSSL BIO object (SSL or plain socket)
  @param response   Output: parsed HTTP response

  @retval false  Success
  @retval true   Error
*/
static bool read_http_response(BIO *bio, HttpResponse *response) {
  char buf[4096];
  std::string raw;
  int content_length = -1;
  bool is_chunked = false;

  /* Read all data until EOF */
  for (;;) {
    int n = BIO_read(bio, buf, sizeof(buf));
    if (n <= 0) {
      if (BIO_should_retry(bio)) continue;
      break;
    }
    raw.append(buf, n);

    /* Check if we have complete headers */
    if (!response->headers_done) {
      size_t hdr_end = raw.find("\r\n\r\n");
      if (hdr_end != std::string::npos) {
        response->headers_done = true;
        response->header_buf = raw.substr(0, hdr_end);

        /* Parse status code from first line */
        size_t space1 = response->header_buf.find(' ');
        if (space1 != std::string::npos) {
          const char *code_str = response->header_buf.c_str() + space1 + 1;
          char *endptr = nullptr;
          long code = strtol(code_str, &endptr, 10);
          if (endptr != code_str && code > 0 && code < 1000)
            response->status_code = static_cast<int>(code);
        }

        /* Check Transfer-Encoding: chunked */
        std::string te_value;
        if (find_header_value(response->header_buf, "Transfer-Encoding",
                              &te_value)) {
          std::string lower_te = te_value;
          for (auto &c : lower_te)
            c = std::tolower(static_cast<unsigned char>(c));
          if (lower_te.find("chunked") != std::string::npos) {
            is_chunked = true;
          }
        }

        /* Parse Content-Length if present (only if not chunked) */
        if (!is_chunked) {
          std::string cl_value;
          if (find_header_value(response->header_buf, "Content-Length",
                                &cl_value)) {
            char *endptr = nullptr;
            long cl = strtol(cl_value.c_str(), &endptr, 10);
            if (endptr != cl_value.c_str() && cl >= 0)
              content_length = static_cast<int>(cl);
          }
        }

        response->body = raw.substr(hdr_end + 4);
      }
    } else {
      /* Accumulate body */
      size_t hdr_end = raw.find("\r\n\r\n");
      response->body = raw.substr(hdr_end + 4);
    }

    /* Check if we have read enough */
    if (response->headers_done) {
      if (is_chunked) {
        /* Simplified chunked termination check: scan for the final-chunk
           marker "\r\n0\r\n".  This is a heuristic that works for typical
           JSON API responses (LLM providers), where the body content won't
           contain a raw "\r\n0\r\n" byte sequence.  A fully correct
           implementation would require stateful chunk-boundary parsing. */
        if (response->body.find("\r\n0\r\n") != std::string::npos ||
            (response->body.size() >= 3 &&
             response->body.substr(0, 2) == "0\r")) {
          break;
        }
      } else if (content_length >= 0 &&
                 static_cast<int>(response->body.length()) >= content_length) {
        break;
      }
    }
  }

  if (!response->headers_done) return true;

  /* Decode chunked body if needed */
  if (is_chunked) {
    response->body = decode_chunked_body(response->body);
  }

  return false;
}

/**
  Perform an HTTPS (or HTTP) POST request using OpenSSL BIO.

  @param endpoint      Full URL (e.g. https://api.openai.com/v1/chat/completions)
  @param api_key       Bearer token
  @param request_body  JSON body string
  @param response_body [out] Response body string
  @param error_msg     [out] Error message on failure

  @retval false  Success (response_body populated)
  @retval true   Error (error_msg populated)
*/
/*
  Timeout settings for AI HTTP calls, aligned with CN's AiHttpClient.java:
  - CONNECT_TIMEOUT: max time to establish TCP + TLS connection (30s)
  - READ_TIMEOUT:    max idle time waiting for data on socket (120s)
  These are socket-level idle timeouts (SO_SNDTIMEO / SO_RCVTIMEO),
  NOT total request timeouts.
*/
static const int AI_CONNECT_TIMEOUT_SEC = 30;
static const int AI_READ_TIMEOUT_SEC = 120;

static void prefer_ipv4_connect(BIO *bio) {
  if (!bio) return;
#ifdef BIO_FAMILY_IPV4
  BIO *connect_bio = BIO_find_type(bio, BIO_TYPE_CONNECT);
  if (connect_bio) BIO_set_conn_ip_family(connect_bio, BIO_FAMILY_IPV4);
#else
  (void)bio;
#endif
}

static void load_ai_verify_locations(SSL_CTX *ctx) {
  SSL_CTX_set_default_verify_paths(ctx);

  const char *ca_files[] = {
      "/etc/pki/tls/certs/ca-bundle.crt",
      "/etc/ssl/certs/ca-certificates.crt",
  };
  for (const char *ca_file : ca_files) {
    if (access(ca_file, R_OK) == 0)
      SSL_CTX_load_verify_locations(ctx, ca_file, nullptr);
  }
}

static bool ai_http_post(const char *endpoint, const char *api_key,
                         const std::string &request_body,
                         std::string *response_body,
                         std::string *error_msg) {
  std::string host, path;
  int port = 0;
  bool use_ssl = false;

  if (parse_url(endpoint, &host, &port, &path, &use_ssl)) {
    *error_msg = "AI: invalid endpoint URL: ";
    error_msg->append(endpoint);
    return true;
  }

  BIO *bio = nullptr;
  SSL_CTX *ctx = nullptr;
  bool success = false;

  if (use_ssl) {
    ctx = SSL_CTX_new(
#ifdef HAVE_TLSv13
        TLS_client_method()
#else
        SSLv23_client_method()
#endif
    );
    if (!ctx) {
      *error_msg = "AI: SSL_CTX_new failed";
      return true;
    }
    load_ai_verify_locations(ctx);
    SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, nullptr);
    /* Force HTTP/1.1 via ALPN to prevent server from selecting HTTP/2.
       Our BIO-based HTTP client only speaks HTTP/1.1. */
    SSL_CTX_set_alpn_protos(ctx,
                            reinterpret_cast<const unsigned char *>("\x08http/1.1"),
                            9);

    std::string connect_str = host + ":" + std::to_string(port);
    bio = BIO_new_ssl_connect(ctx);
    prefer_ipv4_connect(bio);
    /* BIO_set_conn_hostname macro casts to (char*); use const_cast to avoid
       -Wcast-qual: the macro does not modify the string. */
    BIO_set_conn_hostname(bio, const_cast<char *>(connect_str.c_str()));

    /* Set SNI hostname for TLS */
    SSL *ssl = nullptr;
    BIO_get_ssl(bio, &ssl);
    if (ssl) {
      /* SSL_set_tlsext_host_name macro casts to (void*); same rationale. */
      SSL_set_tlsext_host_name(ssl, const_cast<char *>(host.c_str()));
    }
  } else {
    std::string connect_str = host + ":" + std::to_string(port);
    bio = BIO_new_connect(connect_str.c_str());
    prefer_ipv4_connect(bio);
  }

  if (!bio) {
    *error_msg = "AI: BIO creation failed";
    if (ctx) SSL_CTX_free(ctx);
    return true;
  }

  /* Set socket-level timeouts before connecting.
     BIO_do_connect will use the connect timeout (SO_SNDTIMEO),
     and subsequent BIO_read calls will use the read timeout (SO_RCVTIMEO).
     We must call BIO_do_connect once first in non-blocking fashion to
     create the underlying fd, but with BIO_new_ssl_connect / BIO_new_connect
     the fd is allocated lazily during BIO_do_connect.  Instead, set timeouts
     after a successful connect. For the connect phase itself, set SO_SNDTIMEO
     before BIO_do_connect. */
  {
    /* Force BIO to create its socket fd without actually connecting yet */
    int fd = -1;
    BIO_get_fd(bio, &fd);
    if (fd < 0) {
      /* For BIO_new_ssl_connect, the fd may not exist until connect is
         attempted.  Use BIO_set_conn_int_port + manual socket creation
         is complex; instead rely on OS default connect timeout (~75s)
         and set read timeout after connect succeeds. */
    } else {
      struct timeval tv_conn;
      tv_conn.tv_sec = AI_CONNECT_TIMEOUT_SEC;
      tv_conn.tv_usec = 0;
      setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv_conn, sizeof(tv_conn));
      setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv_conn, sizeof(tv_conn));
    }
  }

  if (BIO_do_connect(bio) <= 0) {
    *error_msg = "AI: connection to ";
    error_msg->append(host);
    error_msg->append(":");
    error_msg->append(std::to_string(port));
    error_msg->append(" failed");
    goto cleanup;
  }

  /* Set read timeout after connection is established.
     Now the fd is guaranteed to exist. */
  {
    int fd = -1;
    BIO_get_fd(bio, &fd);
    if (fd >= 0) {
      struct timeval tv_read;
      tv_read.tv_sec = AI_READ_TIMEOUT_SEC;
      tv_read.tv_usec = 0;
      setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv_read, sizeof(tv_read));
    }
  }

  /* Verify SSL handshake if using SSL */
  if (use_ssl) {
    if (BIO_do_handshake(bio) <= 0) {
      *error_msg = "AI: SSL handshake failed with ";
      error_msg->append(host);
      goto cleanup;
    }
  }

  {
    /* Build HTTP request */
    std::string http_request;
    http_request.reserve(1024 + request_body.size());
    http_request.append("POST ");
    http_request.append(path);
    http_request.append(" HTTP/1.1\r\n");
    http_request.append("Host: ");
    http_request.append(host);
    http_request.append("\r\n");
    http_request.append("Authorization: Bearer ");
    http_request.append(api_key);
    http_request.append("\r\n");
    http_request.append("Content-Type: application/json\r\n");
    http_request.append("Accept-Encoding: identity\r\n");
    http_request.append("Content-Length: ");
    http_request.append(std::to_string(request_body.size()));
    http_request.append("\r\n");
    http_request.append("Connection: close\r\n");
    http_request.append("\r\n");
    http_request.append(request_body);

    /* Send request */
    int total = static_cast<int>(http_request.size());
    int sent = 0;
    while (sent < total) {
      int n = BIO_write(bio, http_request.data() + sent, total - sent);
      if (n <= 0) {
        if (BIO_should_retry(bio)) continue;
        *error_msg = "AI: failed to send HTTP request";
        goto cleanup;
      }
      sent += n;
    }
    (void)BIO_flush(bio);

    /* Read response */
    HttpResponse resp;
    if (read_http_response(bio, &resp)) {
      *error_msg = "AI: failed to read HTTP response";
      goto cleanup;
    }

    if (resp.status_code < 200 || resp.status_code >= 300) {
      *error_msg = "AI: HTTP ";
      error_msg->append(std::to_string(resp.status_code));
      error_msg->append(" error from remote server");
      goto cleanup;
    }

    *response_body = std::move(resp.body);
    success = true;
  }

cleanup:
  BIO_free_all(bio);
  if (ctx) SSL_CTX_free(ctx);
  /* Clear any leftover OpenSSL errors from the thread-local error queue.
     If not cleared, a subsequent vio_ssl_write (when sending the MySQL error
     response back to the client over an SSL connection) will hit
     assert(ERR_peek_error() == 0) and crash. */
  ERR_clear_error();
  return !success;
}

/** @} end of AI_HTTP_Internals */

/**
  Helper: check whether a provider string means "openai" protocol.
*/
static inline bool is_openai_provider(const char *provider) {
  return provider && strcasecmp(provider, "openai") == 0;
}

/**
  Build a Chat request body JSON (supports DashScope native & OpenAI).

  Supports two endpoint styles:
  - text-generation: content is a plain string
  - multimodal-generation: content is [{"type":"text","text":"..."}]

  Format:
  {
    "model": "model-name",
    "input": {
      "messages": [
        {"role": "system", "content": "..."},
        {"role": "user", "content": "prompt"}
      ]
    },
    "parameters": {
      "result_format": "message",
      "temperature": 0.7,
      "enable_thinking": false
    }
  }

  @param model_name   Model identifier
  @param prompt       User prompt text
  @param options_json Optional JSON string with overrides
  @param endpoint     Endpoint URL (used to detect multimodal-generation)
  @param provider     Provider string ("dashscope" / "openai" / ...)

  @return  JSON string for the request body
*/
static std::string build_chat_request(const char *model_name,
                                      const char *prompt,
                                      const char *options_json,
                                      const char *endpoint,
                                      const char *provider) {
  rapidjson::StringBuffer sb;
  rapidjson::Writer<rapidjson::StringBuffer> w(sb);

  /* Parse options if provided */
  rapidjson::Document opts;
  bool has_opts = false;
  if (options_json && options_json[0] != '\0') {
    opts.Parse(options_json);
    if (!opts.HasParseError() && opts.IsObject()) {
      has_opts = true;
    }
  }

  bool openai = is_openai_provider(provider);

  /* Determine if the endpoint uses multimodal-generation protocol */
  bool is_multimodal = false;
  if (!openai && endpoint) {
    std::string ep(endpoint);
    is_multimodal = (ep.find("multimodal-generation") != std::string::npos);
  }

  w.StartObject();

  /* model */
  w.Key("model");
  w.String(model_name);

  if (openai) {
    /* ---- OpenAI compatible: flat structure ---- */
    w.Key("messages");
    w.StartArray();

    if (has_opts && opts.HasMember("system_prompt") &&
        opts["system_prompt"].IsString()) {
      w.StartObject();
      w.Key("role");
      w.String("system");
      w.Key("content");
      w.String(opts["system_prompt"].GetString());
      w.EndObject();
    }

    w.StartObject();
    w.Key("role");
    w.String("user");
    w.Key("content");
    w.String(prompt);
    w.EndObject();

    w.EndArray(); /* end messages */

    /* parameters at top level */
    if (has_opts) {
      if (opts.HasMember("temperature") && opts["temperature"].IsNumber()) {
        w.Key("temperature");
        w.Double(opts["temperature"].GetDouble());
      }
      if (opts.HasMember("max_tokens") && opts["max_tokens"].IsInt()) {
        w.Key("max_tokens");
        w.Int(opts["max_tokens"].GetInt());
      }
      if (opts.HasMember("top_p") && opts["top_p"].IsNumber()) {
        w.Key("top_p");
        w.Double(opts["top_p"].GetDouble());
      }
      if (opts.HasMember("stop")) {
        const auto &sv = opts["stop"];
        if (sv.IsString()) {
          w.Key("stop");
          w.String(sv.GetString());
        } else if (sv.IsArray()) {
          w.Key("stop");
          w.StartArray();
          for (auto it = sv.Begin(); it != sv.End(); ++it) {
            if (it->IsString()) w.String(it->GetString());
          }
          w.EndArray();
        }
      }
    }
  } else {
    /* ---- DashScope native: nested structure ---- */
    /* input.messages */
    w.Key("input");
    w.StartObject();
    w.Key("messages");
    w.StartArray();

  /* system prompt from options */
  if (has_opts && opts.HasMember("system_prompt") &&
      opts["system_prompt"].IsString()) {
    w.StartObject();
    w.Key("role");
    w.String("system");
    w.Key("content");
    if (is_multimodal) {
      w.StartArray();
      w.StartObject();
      w.Key("type");
      w.String("text");
      w.Key("text");
      w.String(opts["system_prompt"].GetString());
      w.EndObject();
      w.EndArray();
    } else {
      w.String(opts["system_prompt"].GetString());
    }
    w.EndObject();
  }

  /* user message */
  w.StartObject();
  w.Key("role");
  w.String("user");
  w.Key("content");
  if (is_multimodal) {
    w.StartArray();
    w.StartObject();
    w.Key("type");
    w.String("text");
    w.Key("text");
    w.String(prompt);
    w.EndObject();
    w.EndArray();
  } else {
    w.String(prompt);
  }
  w.EndObject();

  w.EndArray();
  w.EndObject(); /* end input */

  /* parameters */
  w.Key("parameters");
  w.StartObject();
  w.Key("result_format");
  w.String("message");

  if (has_opts) {
    if (opts.HasMember("temperature") && opts["temperature"].IsNumber()) {
      w.Key("temperature");
      w.Double(opts["temperature"].GetDouble());
    }
    if (opts.HasMember("max_tokens") && opts["max_tokens"].IsInt()) {
      w.Key("max_tokens");
      w.Int(opts["max_tokens"].GetInt());
    }
    if (opts.HasMember("top_p") && opts["top_p"].IsNumber()) {
      w.Key("top_p");
      w.Double(opts["top_p"].GetDouble());
    }
    if (opts.HasMember("stop")) {
      const auto &sv = opts["stop"];
      if (sv.IsString()) {
        w.Key("stop");
        w.String(sv.GetString());
      } else if (sv.IsArray()) {
        w.Key("stop");
        w.StartArray();
        for (auto it = sv.Begin(); it != sv.End(); ++it) {
          if (it->IsString()) w.String(it->GetString());
        }
        w.EndArray();
      }
    }
  }

  /* enable_thinking: Qwen3 chain-of-thought mode, default false */
  bool enable_thinking = false;
  if (has_opts && opts.HasMember("enable_thinking") &&
      opts["enable_thinking"].IsBool()) {
    enable_thinking = opts["enable_thinking"].GetBool();
  }
  w.Key("enable_thinking");
  w.Bool(enable_thinking);

  w.EndObject();   /* end parameters */
  }                /* end DashScope branch */

  w.EndObject();
  return std::string(sb.GetString(), sb.GetSize());
}

/**
  Extract the text content from a Chat API response.

  Supports two response formats:
  - text-generation: content is a plain string
  - multimodal-generation: content is [{"type":"text","text":"..."}]

  Expected format:
  {
    "output": {
      "choices": [
        {
          "message": {
            "content": "The generated text..."   // or [{...}] for multimodal
          }
        }
      ]
    }
  }

  @param response_json  Raw JSON response string
  @param content        [out] Extracted text content
  @param error_msg      [out] Error message on failure
  @param provider       Provider string ("dashscope" / "openai" / ...)

  @retval false  Success
  @retval true   Error
*/
static bool extract_chat_content(const std::string &response_json,
                                 std::string *content,
                                 std::string *error_msg,
                                 const char *provider) {
  rapidjson::Document doc;
  doc.Parse(response_json.c_str(), response_json.size());

  if (doc.HasParseError()) {
    *error_msg = "AI_PROMPT: failed to parse API response as JSON";
    return true;
  }

  /* Check for DashScope native error: {"code":"...", "message":"..."} */
  if (doc.HasMember("code") && doc["code"].IsString()) {
    *error_msg = "AI_PROMPT: API error: ";
    if (doc.HasMember("message") && doc["message"].IsString())
      error_msg->append(doc["message"].GetString());
    else
      error_msg->append(doc["code"].GetString());
    return true;
  }

  /* Check for OpenAI error: {"error":{"message":"..."}} */
  if (doc.HasMember("error") && doc["error"].IsObject()) {
    *error_msg = "AI_PROMPT: API error: ";
    const auto &err = doc["error"];
    if (err.HasMember("message") && err["message"].IsString())
      error_msg->append(err["message"].GetString());
    else
      error_msg->append("unknown error");
    return true;
  }

  bool openai = is_openai_provider(provider);

  if (openai) {
    /* ---- OpenAI: choices[0].message.content (top-level) ---- */
    if (!doc.HasMember("choices") || !doc["choices"].IsArray() ||
        doc["choices"].Size() == 0) {
      *error_msg = "AI_PROMPT: API returned empty choices";
      return true;
    }
    const auto &first_choice = doc["choices"][0];
    if (!first_choice.HasMember("message") ||
        !first_choice["message"].IsObject()) {
      *error_msg = "AI_PROMPT: API returned no message in first choice";
      return true;
    }
    const auto &message = first_choice["message"];
    if (!message.HasMember("content") || !message["content"].IsString()) {
      *error_msg = "AI_PROMPT: API returned no content in message";
      return true;
    }
    *content = message["content"].GetString();
    return false;
  }

  /* ---- DashScope native: output.choices[0].message.content ---- */
  if (!doc.HasMember("output") || !doc["output"].IsObject()) {
    *error_msg = "AI_PROMPT: API returned no output";
    return true;
  }

  const auto &output = doc["output"];
  if (!output.HasMember("choices") || !output["choices"].IsArray() ||
      output["choices"].Size() == 0) {
    *error_msg = "AI_PROMPT: API returned empty choices";
    return true;
  }

  const auto &first_choice = output["choices"][0];
  if (!first_choice.HasMember("message") ||
      !first_choice["message"].IsObject()) {
    *error_msg = "AI_PROMPT: API returned no message in first choice";
    return true;
  }

  const auto &message = first_choice["message"];
  if (!message.HasMember("content")) {
    *error_msg = "AI_PROMPT: API returned no content in message";
    return true;
  }

  /* content can be a plain String (text-generation) or a JSONArray
     (multimodal-generation: [{"type":"text","text":"..."}]) */
  const auto &content_val = message["content"];
  if (content_val.IsString()) {
    *content = content_val.GetString();
  } else if (content_val.IsArray()) {
    std::string result;
    for (rapidjson::SizeType i = 0; i < content_val.Size(); ++i) {
      if (content_val[i].IsObject() &&
          content_val[i].HasMember("text") &&
          content_val[i]["text"].IsString()) {
        result.append(content_val[i]["text"].GetString());
      } else if (content_val[i].IsString()) {
        result.append(content_val[i].GetString());
      }
    }
    *content = result;
  } else {
    *error_msg = "AI_PROMPT: API returned unexpected content type";
    return true;
  }

  return false;
}

/* ================================================================
   Shared LLM (Chat Completions) call helper
   ================================================================ */

/**
  Common implementation for AI functions that call the Chat Completions API.
  Used by AI_PROMPT and future LLM-wrapper functions.

  @param args        Item argument array
  @param arg_count   Number of arguments
  @param fn_name     SQL function name (for error messages)
  @param null_value  [out] Set to true on error
  @param tmp_value   [out] Buffer for the return value

  @return  Pointer to tmp_value on success, nullptr on error
*/
static String *ai_llm_call(Item **args, uint arg_count,
                           const char *fn_name,
                           bool *null_value, String *tmp_value) {
  /* ---- Arg 0: prompt (required) ---- */
  String prompt_buf;
  String *prompt_str = args[0]->val_str(&prompt_buf);
  /* NULL input → NULL output (SQL scalar function convention). */
  if (ai_arg_is_null(args[0], null_value)) return nullptr;
  if (!prompt_str) {
    my_error(ER_WRONG_ARGUMENTS, MYF(0), fn_name);
    *null_value = true;
    return nullptr;
  }
  if (prompt_str->length() == 0) {
    my_printf_error(ER_WRONG_ARGUMENTS,
                    "%s: prompt cannot be empty", MYF(0), fn_name);
    *null_value = true;
    return nullptr;
  }

  /* ---- Arg 1: model config name (optional) ---- */
  const char *config_name = nullptr;
  String model_buf;
  if (arg_count >= 2 && !args[1]->null_value) {
    String *m = args[1]->val_str(&model_buf);
    if (m && m->length() > 0) {
      config_name = m->c_ptr_safe();
    }
  }

  /* ---- Arg 2: options JSON (optional) ---- */
  const char *options_json = nullptr;
  String opts_buf;
  if (arg_count >= 3 && !args[2]->null_value) {
    String *o = args[2]->val_str(&opts_buf);
    if (o && o->length() > 0) {
      options_json = o->c_ptr_safe();
      /* Validate JSON format */
      rapidjson::Document opts_check;
      opts_check.Parse(options_json);
      if (opts_check.HasParseError() || !opts_check.IsObject()) {
        my_printf_error(ER_WRONG_ARGUMENTS,
                        "%s: invalid options JSON", MYF(0), fn_name);
        *null_value = true;
        return nullptr;
      }
    }
  }

  /* ---- Resolve model from in-memory cache ---- */
  im::Ai_cached_model cached;
  bool found = false;

  if (config_name && config_name[0] != '\0') {
    found = im::Ai_model_cache::instance()->find_by_name(config_name, &cached);
    if (!found) {
      my_printf_error(ER_WRONG_ARGUMENTS,
                      "%s: model '%s' not found in ai_model_config. "
                      "Register it first with CALL dbms_ai.register_model(...).",
                      MYF(0), fn_name, config_name);
      *null_value = true;
      return nullptr;
    }
  } else {
    if (find_default_ai_model(fn_name, "AI_PROMPT", &cached, null_value))
      return nullptr;
  }

  const std::string resolved_ep = cached.endpoint;
  const char *endpoint = resolved_ep.c_str();
  const std::string &effective_key = resolve_api_key(cached);
  /* Use the underlying model name for the API request */
  const char *model_name = cached.model.empty()
                               ? cached.model_name.c_str()
                               : cached.model.c_str();

  if (resolved_ep.empty()) {
    my_printf_error(ER_WRONG_ARGUMENTS,
                    "%s: model '%s' has no endpoint configured.",
                    MYF(0), fn_name, cached.model_name.c_str());
    *null_value = true;
    return nullptr;
  }

  if (effective_key.empty()) {
    my_printf_error(ER_WRONG_ARGUMENTS,
                    "%s: model '%s' has no api_key configured. %s",
                    MYF(0), fn_name, cached.model_name.c_str(),
                    im::get_no_api_key_hint().c_str());
    *null_value = true;
    return nullptr;
  }

  /* ---- Build request ---- */
  std::string request_body =
      build_chat_request(model_name, prompt_str->c_ptr_safe(), options_json,
                         endpoint, cached.provider.c_str());

  /* ---- Execute HTTP POST ---- */
  std::string response_body;
  std::string error_msg;
  if (ai_http_post(endpoint, effective_key.c_str(), request_body,
                   &response_body, &error_msg)) {
    my_printf_error(ER_WRONG_ARGUMENTS, "%s", MYF(0), error_msg.c_str());
    *null_value = true;
    return nullptr;
  }

  /* ---- Parse response ---- */
  std::string content;
  if (extract_chat_content(response_body, &content, &error_msg,
                           cached.provider.c_str())) {
    my_printf_error(ER_WRONG_ARGUMENTS, "%s", MYF(0), error_msg.c_str());
    *null_value = true;
    return nullptr;
  }

  /* ---- Return result ---- */
  *null_value = false;
  tmp_value->copy(content.c_str(), content.length(),
                  &my_charset_utf8mb4_bin);
  return tmp_value;
}

/* ================================================================
   Item_func_ai_prompt implementation
   ================================================================ */

bool Item_func_ai_prompt::resolve_type(THD *thd) {
  if (param_type_is_default(thd, 0, -1)) return true;
  set_data_type_string(uint32{65535}, &my_charset_utf8mb4_bin);
  set_nullable(true);
  return false;
}

String *Item_func_ai_prompt::val_str(String *str [[maybe_unused]]) {
  DBUG_TRACE;
  assert(fixed);
  return ai_llm_call(args, arg_count, func_name(), &null_value, &tmp_value);
}

/* ================================================================
   AI_EMBEDDING helpers
   ================================================================ */

/**
  Build an Embedding API request body JSON.

  DashScope native format:
  {
    "model": "text-embedding-v3",
    "input": { "texts": ["text to embed"] },
    "parameters": { "dimension": 1024 }      // optional
  }

  OpenAI compatible format:
  {
    "model": "text-embedding-v3",
    "input": "text to embed",
    "dimensions": 1024                        // optional
  }

  @param model_name   Model identifier (e.g. "text-embedding-v3")
  @param text         Input text to embed
  @param dimension    Embedding dimension (0 = use model default)

  @return  JSON string for the request body
*/
static std::string build_embedding_request(const char *model_name,
                                           const char *text,
                                           int dimension,
                                           const char *provider) {
  rapidjson::StringBuffer sb;
  rapidjson::Writer<rapidjson::StringBuffer> w(sb);

  bool openai = is_openai_provider(provider);

  w.StartObject();
  w.Key("model");
  w.String(model_name);

  if (openai) {
    /* OpenAI: input is a plain string at top level */
    w.Key("input");
    w.String(text);
    if (dimension > 0) {
      w.Key("dimensions");
      w.Int(dimension);
    }
  } else {
    /* DashScope: input.texts array */
    w.Key("input");
    w.StartObject();
    w.Key("texts");
    w.StartArray();
    w.String(text);
    w.EndArray();
    w.EndObject();

    /* parameters (optional) */
    if (dimension > 0) {
      w.Key("parameters");
      w.StartObject();
      w.Key("dimension");
      w.Int(dimension);
      w.EndObject();
    }
  }

  w.EndObject();

  return std::string(sb.GetString(), sb.GetSize());
}

/**
  Extract the embedding vector from an Embedding API response
    (DashScope native or OpenAI compatible).

  Expected format:
  {
    "output": {
      "embeddings": [
        {
          "embedding": [0.123, -0.456, ...]
        }
      ]
    }
  }

  The embedding array is serialised back to a JSON array string for the caller.

  @param response_json  Raw JSON response string
  @param vector_json    [out] JSON array string, e.g. "[0.123,-0.456,...]"
  @param error_msg      [out] Error message on failure

  @retval false  Success
  @retval true   Error
*/
static bool extract_embedding_vector(const std::string &response_json,
                                     std::string *vector_json,
                                     std::string *error_msg,
                                     const char *provider) {
  rapidjson::Document doc;
  doc.Parse(response_json.c_str(), response_json.size());

  if (doc.HasParseError()) {
    *error_msg = "AI_EMBEDDING: failed to parse API response as JSON";
    return true;
  }

  /* Check for DashScope native error: {"code":"...", "message":"..."} */
  if (doc.HasMember("code") && doc["code"].IsString()) {
    *error_msg = "AI_EMBEDDING: API error: ";
    if (doc.HasMember("message") && doc["message"].IsString())
      error_msg->append(doc["message"].GetString());
    else
      error_msg->append(doc["code"].GetString());
    return true;
  }

  /* Check for OpenAI error: {"error":{"message":"..."}} */
  if (doc.HasMember("error") && doc["error"].IsObject()) {
    *error_msg = "AI_EMBEDDING: API error: ";
    const auto &err = doc["error"];
    if (err.HasMember("message") && err["message"].IsString())
      error_msg->append(err["message"].GetString());
    else
      error_msg->append("unknown error");
    return true;
  }

  const rapidjson::Value *emb_ptr = nullptr;

  if (is_openai_provider(provider)) {
    /* OpenAI: data[0].embedding */
    if (!doc.HasMember("data") || !doc["data"].IsArray() ||
        doc["data"].Size() == 0) {
      *error_msg = "AI_EMBEDDING: API returned empty data array";
      return true;
    }
    const auto &first = doc["data"][0];
    if (!first.HasMember("embedding") || !first["embedding"].IsArray()) {
      *error_msg = "AI_EMBEDDING: API returned no embedding in first element";
      return true;
    }
    emb_ptr = &first["embedding"];
  } else {
    /* DashScope native: output.embeddings[0].embedding */
    if (!doc.HasMember("output") || !doc["output"].IsObject()) {
      *error_msg = "AI_EMBEDDING: API returned no output";
      return true;
    }
    const auto &output = doc["output"];
    if (!output.HasMember("embeddings") || !output["embeddings"].IsArray() ||
        output["embeddings"].Size() == 0) {
      *error_msg = "AI_EMBEDDING: API returned empty embeddings array";
      return true;
    }
    const auto &first = output["embeddings"][0];
    if (!first.HasMember("embedding") || !first["embedding"].IsArray()) {
      *error_msg = "AI_EMBEDDING: API returned no embedding in first element";
      return true;
    }
    emb_ptr = &first["embedding"];
  }

  /* Serialise the embedding array back to a JSON string */
  rapidjson::StringBuffer sb;
  rapidjson::Writer<rapidjson::StringBuffer> w(sb);
  emb_ptr->Accept(w);

  *vector_json = std::string(sb.GetString(), sb.GetSize());
  return false;
}

/* ================================================================
   Item_func_ai_embedding implementation
   ================================================================ */

bool Item_func_ai_embedding::resolve_type(THD *thd) {
  if (param_type_is_default(thd, 0, -1)) return true;
  /*
    Embedding vectors can be large (e.g. 1024-dim or more).
    Use max longtext-like length but still return STRING_RESULT.
  */
  set_data_type_string(uint32{65535}, &my_charset_utf8mb4_bin);
  set_nullable(true);
  return false;
}

String *Item_func_ai_embedding::val_str(String *str) {
  DBUG_TRACE;
  assert(fixed);

  /* ---- Arg 0: text (required) ---- */
  String *text_str = args[0]->val_str(str);
  /* NULL input → NULL output (SQL scalar function convention). */
  if (ai_arg_is_null(args[0], &null_value)) return nullptr;
  if (!text_str) {
    my_error(ER_WRONG_ARGUMENTS, MYF(0), func_name());
    null_value = true;
    return nullptr;
  }
  /* Empty string → push warning and return NULL so a single empty row does
     not abort the surrounding batch (the upstream embedding service rejects
     empty input). */
  if (text_str->length() == 0) {
    ai_warn_empty_input(current_thd, func_name());
    null_value = true;
    return nullptr;
  }

  /* ---- Arg 1: model config name (optional) ---- */
  const char *config_name = nullptr;
  String model_buf;
  if (arg_count >= 2 && !args[1]->null_value) {
    String *m = args[1]->val_str(&model_buf);
    if (m && m->length() > 0) {
      config_name = m->c_ptr_safe();
    }
  }

  /* ---- Arg 2: options JSON (optional) - supports "dimension" ---- */
  int dimension = 0;
  String opts_buf;
  if (arg_count >= 3 && !args[2]->null_value) {
    String *o = args[2]->val_str(&opts_buf);
    if (o && o->length() > 0) {
      const char *options_cstr = o->c_ptr_safe();
      rapidjson::Document opts_check;
      opts_check.Parse(options_cstr);
      if (opts_check.HasParseError() || !opts_check.IsObject()) {
        my_printf_error(ER_WRONG_ARGUMENTS,
                        "AI_EMBEDDING: invalid options JSON", MYF(0));
        null_value = true;
        return nullptr;
      }
      if (opts_check.HasMember("dimension") &&
          opts_check["dimension"].IsInt()) {
        dimension = opts_check["dimension"].GetInt();
      }
    }
  }

  /* ---- Resolve model from in-memory cache ---- */
  im::Ai_cached_model cached;
  bool found = false;

  if (config_name && config_name[0] != '\0') {
    found = im::Ai_model_cache::instance()->find_by_name(config_name, &cached);
    if (!found) {
      my_printf_error(ER_WRONG_ARGUMENTS,
                      "AI_EMBEDDING: model '%s' not found in ai_model_config. "
                      "Register it first with CALL dbms_ai.register_model(...).",
                      MYF(0), config_name);
      null_value = true;
      return nullptr;
    }
  } else {
    if (find_default_ai_model("AI_EMBEDDING", "AI_EMBEDDING", &cached,
                              &null_value))
      return nullptr;
  }

  const std::string resolved_ep = cached.endpoint;
  const char *endpoint = resolved_ep.c_str();
  const std::string &effective_key = resolve_api_key(cached);
  /* Use the underlying model name for the API request */
  const char *model_name = cached.model.empty()
                               ? cached.model_name.c_str()
                               : cached.model.c_str();

  if (resolved_ep.empty()) {
    my_printf_error(ER_WRONG_ARGUMENTS,
                    "AI_EMBEDDING: model '%s' has no endpoint configured.",
                    MYF(0), cached.model_name.c_str());
    null_value = true;
    return nullptr;
  }

  if (effective_key.empty()) {
    my_printf_error(ER_WRONG_ARGUMENTS,
                    "AI_EMBEDDING: model '%s' has no api_key configured. %s",
                    MYF(0), cached.model_name.c_str(),
                    im::get_no_api_key_hint().c_str());
    null_value = true;
    return nullptr;
  }

  /* ---- Build request ---- */
  std::string request_body = build_embedding_request(
      model_name, text_str->c_ptr_safe(), dimension, cached.provider.c_str());

  /* ---- Execute HTTP POST ---- */
  std::string response_body;
  std::string error_msg;
  if (ai_http_post(endpoint, effective_key.c_str(), request_body,
                   &response_body, &error_msg)) {
    my_printf_error(ER_WRONG_ARGUMENTS, "%s", MYF(0), error_msg.c_str());
    null_value = true;
    return nullptr;
  }

  /* ---- Parse response ---- */
  std::string vector_json;
  if (extract_embedding_vector(response_body, &vector_json, &error_msg,
                               cached.provider.c_str())) {
    my_printf_error(ER_WRONG_ARGUMENTS, "%s", MYF(0), error_msg.c_str());
    null_value = true;
    return nullptr;
  }

  /* ---- Return result ---- */
  null_value = false;
  tmp_value.copy(vector_json.c_str(), vector_json.length(),
                 &my_charset_utf8mb4_bin);
  return &tmp_value;
}

/* ================================================================
   Item_func_ai_classify implementation
   ================================================================ */

bool Item_func_ai_classify::resolve_type(THD *thd) {
  if (param_type_is_default(thd, 0, -1)) return true;
  /* Classification labels are short strings */
  set_data_type_string(uint32{1024}, &my_charset_utf8mb4_bin);
  set_nullable(true);
  return false;
}

String *Item_func_ai_classify::val_str(String *str [[maybe_unused]]) {
  DBUG_TRACE;
  assert(fixed);

  /* ---- Arg 0: text (required) ---- */
  String text_buf;
  String *text_str = args[0]->val_str(&text_buf);
  /* NULL input → NULL output (SQL scalar function convention). */
  if (ai_arg_is_null(args[0], &null_value)) return nullptr;
  if (!text_str || text_str->length() == 0) {
    my_error(ER_WRONG_ARGUMENTS, MYF(0), func_name());
    null_value = true;
    return nullptr;
  }

  /* ---- Arg 1: categories JSON array (required) ---- */
  String cat_buf;
  String *cat_str = args[1]->val_str(&cat_buf);
  if (ai_arg_is_null(args[1], &null_value)) return nullptr;
  if (!cat_str || cat_str->length() == 0) {
    my_printf_error(ER_WRONG_ARGUMENTS,
                    "AI_CLASSIFY: categories argument is required "
                    "(JSON array of candidate labels).",
                    MYF(0));
    null_value = true;
    return nullptr;
  }

  /* ---- Arg 2: model config name (optional) ---- */
  const char *config_name = nullptr;
  String model_buf;
  if (arg_count >= 3 && !args[2]->null_value) {
    String *m = args[2]->val_str(&model_buf);
    if (m && m->length() > 0) {
      config_name = m->c_ptr_safe();
    }
  }

  /* ---- Arg 3: options JSON (optional) ---- */
  bool multi_label = false;
  double opt_temperature = 0.0;
  int opt_max_tokens = 100;
  if (arg_count >= 4 && !args[3]->null_value) {
    String opts_buf;
    String *opts_str = args[3]->val_str(&opts_buf);
    if (opts_str && opts_str->length() > 0) {
      rapidjson::Document opts_doc;
      opts_doc.Parse(opts_str->c_ptr_safe(), opts_str->length());
      if (!opts_doc.HasParseError() && opts_doc.IsObject()) {
        if (opts_doc.HasMember("multi_label") &&
            opts_doc["multi_label"].IsBool()) {
          multi_label = opts_doc["multi_label"].GetBool();
        }
        if (opts_doc.HasMember("temperature") &&
            opts_doc["temperature"].IsNumber())
          opt_temperature = opts_doc["temperature"].GetDouble();
        if (opts_doc.HasMember("max_tokens") &&
            opts_doc["max_tokens"].IsInt())
          opt_max_tokens = opts_doc["max_tokens"].GetInt();
      }
    }
  }

  /* ---- Resolve model from in-memory cache ---- */
  im::Ai_cached_model cached;
  bool found = false;

  if (config_name && config_name[0] != '\0') {
    found = im::Ai_model_cache::instance()->find_by_name(config_name, &cached);
    if (!found) {
      my_printf_error(ER_WRONG_ARGUMENTS,
                      "AI_CLASSIFY: model '%s' not found in ai_model_config.",
                      MYF(0), config_name);
      null_value = true;
      return nullptr;
    }
  } else {
    if (find_default_ai_model("AI_CLASSIFY", "AI_CLASSIFY", &cached,
                              &null_value))
      return nullptr;
  }

  if (cached.endpoint.empty()) {
    my_printf_error(ER_WRONG_ARGUMENTS,
                    "AI_CLASSIFY: model '%s' has no endpoint configured.",
                    MYF(0), cached.model_name.c_str());
    null_value = true;
    return nullptr;
  }

  const std::string resolved_ep = cached.endpoint;
  const std::string &effective_key = resolve_api_key(cached);
  if (effective_key.empty()) {
    my_printf_error(ER_WRONG_ARGUMENTS,
                    "AI_CLASSIFY: model '%s' has no api_key configured. %s",
                    MYF(0), cached.model_name.c_str(),
                    im::get_no_api_key_hint().c_str());
    null_value = true;
    return nullptr;
  }

  const char *endpoint = resolved_ep.c_str();
  const char *api_key = effective_key.c_str();
  const char *model_name = cached.model.empty()
                               ? cached.model_name.c_str()
                               : cached.model.c_str();

  /* ---- Build request with classification system prompt ---- */
  std::string system_prompt;
  if (multi_label) {
    system_prompt =
        "You are a text classification assistant. "
        "Classify the following text into one or more of the given categories. "
        "Reply with ONLY a JSON array of matching category labels, nothing else. "
        "Do not add explanation.\n"
        "Categories: ";
  } else {
    system_prompt =
        "You are a text classification assistant. "
        "Classify the following text into exactly one of the given categories. "
        "Reply with ONLY the category label, nothing else. "
        "Do not add quotes, punctuation, or explanation.\n"
        "Categories: ";
  }
  system_prompt.append(cat_str->c_ptr_safe());

  /* Build options JSON with the system_prompt injected */
  std::string opts_json = "{\"system_prompt\":";
  /* Escape the system prompt for JSON */
  {
    rapidjson::StringBuffer sb;
    rapidjson::Writer<rapidjson::StringBuffer> w(sb);
    w.String(system_prompt.c_str(),
             static_cast<rapidjson::SizeType>(system_prompt.length()));
    opts_json.append(sb.GetString(), sb.GetSize());
  }
  opts_json.append(",\"temperature\":");
  opts_json.append(std::to_string(opt_temperature));
  opts_json.append(",\"max_tokens\":");
  opts_json.append(std::to_string(opt_max_tokens));
  opts_json.append("}");

  std::string request_body =
      build_chat_request(model_name, text_str->c_ptr_safe(), opts_json.c_str(),
                         endpoint, cached.provider.c_str());

  /* ---- Execute HTTP POST ---- */
  std::string response_body;
  std::string error_msg;
  if (ai_http_post(endpoint, api_key, request_body, &response_body,
                   &error_msg)) {
    my_printf_error(ER_WRONG_ARGUMENTS, "%s", MYF(0), error_msg.c_str());
    null_value = true;
    return nullptr;
  }

  /* ---- Parse response ---- */
  std::string content;
  if (extract_chat_content(response_body, &content, &error_msg,
                           cached.provider.c_str())) {
    my_printf_error(ER_WRONG_ARGUMENTS, "%s", MYF(0), error_msg.c_str());
    null_value = true;
    return nullptr;
  }

  /* Trim whitespace from the LLM response */
  while (!content.empty() &&
         std::isspace(static_cast<unsigned char>(content.front())))
    content.erase(content.begin());
  while (!content.empty() &&
         std::isspace(static_cast<unsigned char>(content.back())))
    content.pop_back();

  null_value = false;
  tmp_value.copy(content.c_str(), content.length(),
                 &my_charset_utf8mb4_bin);
  return &tmp_value;
}

/**
  Strip markdown code-block wrapper from LLM response.
  LLMs frequently return JSON wrapped in ```json ... ``` blocks.
  This helper extracts the inner content.
*/
static void strip_markdown_json_wrapper(std::string &content) {
  /* Must start with ``` to be considered a code block */
  if (content.size() < 6 || content.compare(0, 3, "```") != 0) return;

  /* Find end of opening ``` line (skip optional language tag like "json") */
  size_t line_end = content.find('\n', 3);
  if (line_end == std::string::npos) return;

  /* Find closing ``` */
  size_t close = content.rfind("```");
  if (close == std::string::npos || close <= 3) return;

  /* Extract content between opening line and closing ``` */
  content = content.substr(line_end + 1, close - line_end - 1);

  /* Trim whitespace */
  while (!content.empty() &&
         std::isspace(static_cast<unsigned char>(content.front())))
    content.erase(content.begin());
  while (!content.empty() &&
         std::isspace(static_cast<unsigned char>(content.back())))
    content.pop_back();
}

/* ================================================================
   Shared LLM-with-system-prompt helper
   ================================================================ */

/**
  Common helper for AI functions that use a system prompt with the LLM.
  Variant taking an already-resolved model snapshot: callers that issue
  multiple requests for one function call (e.g. AI_TEXT2SQL pre-filter +
  generation) pass the same snapshot so all rounds use one consistent
  model configuration even if the cache is reloaded concurrently.

  @param fn_name       SQL function name (for error messages)
  @param user_prompt   The user message text
  @param system_prompt The system instruction
  @param cached        Resolved model configuration snapshot
  @param null_value    [out] Set to true on error
  @param tmp_value     [out] Buffer for the return value
  @param temperature   Temperature for the LLM call (default 0.0)
  @param max_tokens    Max tokens for the LLM response (0 = model default)

  @return  Pointer to tmp_value on success, nullptr on error
*/
static String *ai_llm_with_system_prompt_model(
    const char *fn_name, const char *user_prompt,
    const std::string &system_prompt, const im::Ai_cached_model &cached,
    bool *null_value, String *tmp_value,
    double temperature = 0.0, int max_tokens = 0) {
  if (cached.endpoint.empty()) {
    my_printf_error(ER_WRONG_ARGUMENTS,
                    "%s: model '%s' has no endpoint configured.",
                    MYF(0), fn_name, cached.model_name.c_str());
    *null_value = true;
    return nullptr;
  }

  const std::string resolved_ep = cached.endpoint;
  const std::string &effective_key = resolve_api_key(cached);
  if (effective_key.empty()) {
    my_printf_error(ER_WRONG_ARGUMENTS,
                    "%s: model '%s' has no api_key configured. %s",
                    MYF(0), fn_name, cached.model_name.c_str(),
                    im::get_no_api_key_hint().c_str());
    *null_value = true;
    return nullptr;
  }

  const char *model_name = cached.model.empty()
                               ? cached.model_name.c_str()
                               : cached.model.c_str();

  /* Build options JSON with the system_prompt */
  std::string opts_json = "{\"system_prompt\":";
  {
    rapidjson::StringBuffer sb;
    rapidjson::Writer<rapidjson::StringBuffer> w(sb);
    w.String(system_prompt.c_str(),
             static_cast<rapidjson::SizeType>(system_prompt.length()));
    opts_json.append(sb.GetString(), sb.GetSize());
  }
  opts_json.append(",\"temperature\":");
  opts_json.append(std::to_string(temperature));
  if (max_tokens > 0) {
    opts_json.append(",\"max_tokens\":");
    opts_json.append(std::to_string(max_tokens));
  }
  opts_json.append("}");

  std::string request_body =
      build_chat_request(model_name, user_prompt, opts_json.c_str(),
                         resolved_ep.c_str(), cached.provider.c_str());

  /* ---- Execute HTTP POST ---- */
  std::string response_body;
  std::string error_msg;
  if (ai_http_post(resolved_ep.c_str(), effective_key.c_str(),
                   request_body, &response_body, &error_msg)) {
    my_printf_error(ER_WRONG_ARGUMENTS, "%s", MYF(0), error_msg.c_str());
    *null_value = true;
    return nullptr;
  }

  /* ---- Parse response ---- */
  std::string content;
  if (extract_chat_content(response_body, &content, &error_msg,
                           cached.provider.c_str())) {
    my_printf_error(ER_WRONG_ARGUMENTS, "%s", MYF(0), error_msg.c_str());
    *null_value = true;
    return nullptr;
  }

  /* Trim whitespace */
  while (!content.empty() &&
         std::isspace(static_cast<unsigned char>(content.front())))
    content.erase(content.begin());
  while (!content.empty() &&
         std::isspace(static_cast<unsigned char>(content.back())))
    content.pop_back();

  *null_value = false;
  tmp_value->copy(content.c_str(), content.length(),
                  &my_charset_utf8mb4_bin);
  return tmp_value;
}

/**
  Common helper for AI functions that use a system prompt with the LLM.
  Resolves the LLM model from the in-memory cache, then delegates to
  ai_llm_with_system_prompt_model().

  @param fn_name       SQL function name (for error messages)
  @param user_prompt   The user message text
  @param system_prompt The system instruction
  @param config_name   Model config name (nullable, uses default LLM if null)
  @param null_value    [out] Set to true on error
  @param tmp_value     [out] Buffer for the return value
  @param temperature   Temperature for the LLM call (default 0.0)
  @param max_tokens    Max tokens for the LLM response (0 = model default)

  @return  Pointer to tmp_value on success, nullptr on error
*/
static String *ai_llm_with_system_prompt(
    const char *fn_name, const char *user_prompt,
    const std::string &system_prompt, const char *config_name,
    bool *null_value, String *tmp_value,
    double temperature = 0.0, int max_tokens = 0) {
  /* ---- Resolve model from in-memory cache ---- */
  im::Ai_cached_model cached;
  bool found = false;

  if (config_name && config_name[0] != '\0') {
    found = im::Ai_model_cache::instance()->find_by_name(config_name, &cached);
    if (!found) {
      my_printf_error(ER_WRONG_ARGUMENTS,
                      "%s: model '%s' not found in ai_model_config.",
                      MYF(0), fn_name, config_name);
      *null_value = true;
      return nullptr;
    }
  } else {
    if (find_default_ai_model(fn_name, fn_name, &cached, null_value))
      return nullptr;
  }

  return ai_llm_with_system_prompt_model(fn_name, user_prompt, system_prompt,
                                         cached, null_value, tmp_value,
                                         temperature, max_tokens);
}

/* ================================================================
   Item_func_ai_extract implementation
   ================================================================ */

bool Item_func_ai_extract::resolve_type(THD *thd) {
  if (param_type_is_default(thd, 0, -1)) return true;
  set_data_type_string(uint32{65535}, &my_charset_utf8mb4_bin);
  set_nullable(true);
  return false;
}

String *Item_func_ai_extract::val_str(String *str [[maybe_unused]]) {
  DBUG_TRACE;
  assert(fixed);

  /* Arg 0: text (required) */
  String text_buf;
  String *text_str = args[0]->val_str(&text_buf);
  /* NULL input → NULL output (SQL scalar function convention). */
  if (ai_arg_is_null(args[0], &null_value)) return nullptr;
  if (!text_str || text_str->length() == 0) {
    my_error(ER_WRONG_ARGUMENTS, MYF(0), func_name());
    null_value = true;
    return nullptr;
  }

  /* Arg 1: schema JSON (required) */
  String schema_buf;
  String *schema_str = args[1]->val_str(&schema_buf);
  if (ai_arg_is_null(args[1], &null_value)) return nullptr;
  if (!schema_str || schema_str->length() == 0) {
    my_printf_error(ER_WRONG_ARGUMENTS,
                    "AI_EXTRACT: schema argument is required.", MYF(0));
    null_value = true;
    return nullptr;
  }

  /* Arg 2: model (optional) */
  const char *config_name = nullptr;
  String model_buf;
  if (arg_count >= 3 && !args[2]->null_value) {
    String *m = args[2]->val_str(&model_buf);
    if (m && m->length() > 0) config_name = m->c_ptr_safe();
  }

  /* Arg 3: options JSON (optional) -- supports temperature, max_tokens */
  double opt_temperature = 0.0;
  int opt_max_tokens = 1000;
  if (arg_count >= 4 && !args[3]->null_value) {
    String opts_buf;
    String *opts_str = args[3]->val_str(&opts_buf);
    if (opts_str && opts_str->length() > 0) {
      rapidjson::Document opts_doc;
      opts_doc.Parse(opts_str->c_ptr_safe(), opts_str->length());
      if (!opts_doc.HasParseError() && opts_doc.IsObject()) {
        if (opts_doc.HasMember("temperature") &&
            opts_doc["temperature"].IsNumber())
          opt_temperature = opts_doc["temperature"].GetDouble();
        if (opts_doc.HasMember("max_tokens") &&
            opts_doc["max_tokens"].IsInt())
          opt_max_tokens = opts_doc["max_tokens"].GetInt();
      }
    }
  }

  std::string sys_prompt =
      "You are an information extraction assistant. "
      "Extract the requested fields from the text and return a JSON object. "
      "Use the provided schema to determine what to extract. "
      "Reply with ONLY the JSON object, no explanation.\n"
      "Schema: ";
  sys_prompt.append(schema_str->c_ptr_safe());

  String *result = ai_llm_with_system_prompt(
      func_name(), text_str->c_ptr_safe(), sys_prompt, config_name,
      &null_value, &tmp_value, opt_temperature, opt_max_tokens);
  if (!result) return nullptr;

  /* Strip markdown code-block wrapper (LLM may return ```json ... ```) */
  std::string content(result->ptr(), result->length());
  strip_markdown_json_wrapper(content);
  tmp_value.copy(content.c_str(), content.length(),
                 &my_charset_utf8mb4_bin);
  return &tmp_value;
}

/* ================================================================
   Item_func_ai_summarize implementation
   ================================================================ */

bool Item_func_ai_summarize::resolve_type(THD *thd) {
  if (param_type_is_default(thd, 0, -1)) return true;
  set_data_type_string(uint32{65535}, &my_charset_utf8mb4_bin);
  set_nullable(true);
  return false;
}

String *Item_func_ai_summarize::val_str(String *str [[maybe_unused]]) {
  DBUG_TRACE;
  assert(fixed);

  /* Arg 0: text (required) */
  String text_buf;
  String *text_str = args[0]->val_str(&text_buf);
  /* NULL input → NULL output (SQL scalar function convention). */
  if (ai_arg_is_null(args[0], &null_value)) return nullptr;
  if (!text_str || text_str->length() == 0) {
    my_error(ER_WRONG_ARGUMENTS, MYF(0), func_name());
    null_value = true;
    return nullptr;
  }

  /* Arg 1: max_length (optional, INT) */
  long long max_length = 200;  /* default */
  if (arg_count >= 2 && !args[1]->null_value) {
    max_length = args[1]->val_int();
    if (max_length <= 0) max_length = 200;
  }

  /* Arg 2: model (optional) */
  const char *config_name = nullptr;
  String model_buf;
  if (arg_count >= 3 && !args[2]->null_value) {
    String *m = args[2]->val_str(&model_buf);
    if (m && m->length() > 0) config_name = m->c_ptr_safe();
  }

  /* Arg 3: options JSON (optional) -- supports "language", "style",
     "temperature", "max_tokens" */
  std::string language;
  std::string style;
  double opt_temperature = 0.3;
  int opt_max_tokens = static_cast<int>(std::max(200LL, max_length * 2));
  if (arg_count >= 4 && !args[3]->null_value) {
    String opts_buf;
    String *opts_str = args[3]->val_str(&opts_buf);
    if (opts_str && opts_str->length() > 0) {
      rapidjson::Document opts_doc;
      opts_doc.Parse(opts_str->c_ptr_safe(), opts_str->length());
      if (!opts_doc.HasParseError() && opts_doc.IsObject()) {
        if (opts_doc.HasMember("language") &&
            opts_doc["language"].IsString())
          language = opts_doc["language"].GetString();
        if (opts_doc.HasMember("style") &&
            opts_doc["style"].IsString())
          style = opts_doc["style"].GetString();
        if (opts_doc.HasMember("temperature") &&
            opts_doc["temperature"].IsNumber())
          opt_temperature = opts_doc["temperature"].GetDouble();
        if (opts_doc.HasMember("max_tokens") &&
            opts_doc["max_tokens"].IsInt())
          opt_max_tokens = opts_doc["max_tokens"].GetInt();
      }
    }
  }

  std::string sys_prompt =
      "You are a text summarization assistant. "
      "Summarize the following text concisely. "
      "The summary should be no longer than ";
  sys_prompt.append(std::to_string(max_length));
  sys_prompt.append(" characters.");
  if (style == "bullet_points") {
    sys_prompt.append(" Use concise bullet points.");
  }
  if (!language.empty()) {
    sys_prompt.append(" Write the summary in ");
    sys_prompt.append(language);
    sys_prompt.append(".");
  }
  sys_prompt.append(
      " Reply with ONLY the summary text, no explanation or prefix.");

  return ai_llm_with_system_prompt(func_name(), text_str->c_ptr_safe(),
                                   sys_prompt, config_name, &null_value,
                                   &tmp_value, opt_temperature, opt_max_tokens);
}

/* ================================================================
   Item_func_ai_similarity implementation
   ================================================================ */

#include <cmath>
#include <vector>

/**
  Parse a JSON array of numbers into a std::vector<double>.

  @param json_str  JSON string, e.g. "[0.1, -0.2, 0.3]"
  @param vec       [out] Parsed vector
  @param error_msg [out] Error message on failure

  @retval false  Success
  @retval true   Error
*/
static bool parse_json_vector(const char *json_str,
                              std::vector<double> *vec,
                              std::string *error_msg) {
  rapidjson::Document doc;
  doc.Parse(json_str);

  if (doc.HasParseError() || !doc.IsArray()) {
    *error_msg = "AI_SIMILARITY: argument is not a valid JSON array";
    return true;
  }

  vec->clear();
  vec->reserve(doc.Size());
  for (rapidjson::SizeType i = 0; i < doc.Size(); i++) {
    if (!doc[i].IsNumber()) {
      *error_msg = "AI_SIMILARITY: JSON array contains non-numeric element";
      return true;
    }
    vec->push_back(doc[i].GetDouble());
  }
  return false;
}

/**
  Compute cosine similarity between two vectors.

  @param vec1  First vector
  @param vec2  Second vector (must be same size)

  @return  Cosine similarity in [-1, 1]
*/
static double cosine_similarity(const std::vector<double> &vec1,
                                const std::vector<double> &vec2) {
  double dot = 0.0, norm1 = 0.0, norm2 = 0.0;
  for (size_t i = 0; i < vec1.size(); i++) {
    dot += vec1[i] * vec2[i];
    norm1 += vec1[i] * vec1[i];
    norm2 += vec2[i] * vec2[i];
  }
  double denom = std::sqrt(norm1) * std::sqrt(norm2);
  return (denom > 0.0) ? (dot / denom) : 0.0;
}

/**
  Compute Euclidean distance: sqrt(sum((a[i]-b[i])^2)).
  Returns a non-negative value, where 0 means identical vectors.
*/
static double euclidean_distance(const std::vector<double> &vec1,
                                 const std::vector<double> &vec2) {
  double sum_sq = 0.0;
  for (size_t i = 0; i < vec1.size(); i++) {
    double diff = vec1[i] - vec2[i];
    sum_sq += diff * diff;
  }
  return std::sqrt(sum_sq);
}

/**
  Compute dot product: sum(a[i]*b[i]).
*/
static double dot_product(const std::vector<double> &vec1,
                          const std::vector<double> &vec2) {
  double result = 0.0;
  for (size_t i = 0; i < vec1.size(); i++) {
    result += vec1[i] * vec2[i];
  }
  return result;
}

/**
  Call the Embedding API and return the vector as a JSON string.
  This is a reusable helper extracted from AI_EMBEDDING logic.

  @param text         Input text to embed
  @param config_name  Model config name (nullable, uses default EMBEDDING)
  @param fn_name      Calling function name (for error messages)
  @param vector_json  [out] JSON array string of the embedding vector
  @param error_msg    [out] Error message on failure

  @retval false  Success
  @retval true   Error
*/
static bool ai_get_embedding_vector(const char *text,
                                    const char *config_name,
                                    const char *fn_name,
                                    std::string *vector_json,
                                    std::string *error_msg) {
  im::Ai_cached_model cached;
  bool found = false;

  if (config_name && config_name[0] != '\0') {
    found = im::Ai_model_cache::instance()->find_by_name(config_name, &cached);
    if (!found) {
      *error_msg = std::string(fn_name) + ": model '" + config_name +
                   "' not found in ai_model_config.";
      return true;
    }
  } else {
    if (find_default_ai_model(fn_name, fn_name, &cached, error_msg))
      return true;
  }

  if (cached.endpoint.empty()) {
    *error_msg = std::string(fn_name) + ": model '" + cached.model_name +
                 "' has no endpoint configured.";
    return true;
  }

  const std::string resolved_ep = cached.endpoint;
  const std::string &effective_key = resolve_api_key(cached);
  if (effective_key.empty()) {
    *error_msg = std::string(fn_name) + ": model '" + cached.model_name +
                 "' has no api_key configured. " +
                 im::get_no_api_key_hint();
    return true;
  }

  const char *model_name = cached.model.empty() ? cached.model_name.c_str()
                                                : cached.model.c_str();

  std::string request_body =
      build_embedding_request(model_name, text, 0, cached.provider.c_str());

  std::string response_body;
  if (ai_http_post(resolved_ep.c_str(), effective_key.c_str(),
                   request_body, &response_body, error_msg)) {
    return true;
  }

  if (extract_embedding_vector(response_body, vector_json, error_msg,
                               cached.provider.c_str())) {
    return true;
  }

  return false;
}

bool Item_func_ai_similarity::resolve_type(THD *thd) {
  if (param_type_is_default(thd, 0, -1)) return true;
  set_data_type_double();
  set_nullable(true);
  return false;
}

double Item_func_ai_similarity::val_real() {
  DBUG_TRACE;
  assert(fixed);

  /* Arg 0: vector or text (required) */
  String v1_buf;
  String *v1_str = args[0]->val_str(&v1_buf);
  /* NULL input → NULL output (SQL scalar function convention). */
  if (ai_arg_is_null(args[0], &null_value)) return 0.0;
  if (!v1_str || v1_str->length() == 0) {
    my_error(ER_WRONG_ARGUMENTS, MYF(0), func_name());
    null_value = true;
    return 0.0;
  }

  /* Arg 1: vector or text (required) */
  String v2_buf;
  String *v2_str = args[1]->val_str(&v2_buf);
  if (ai_arg_is_null(args[1], &null_value)) return 0.0;
  if (!v2_str || v2_str->length() == 0) {
    my_error(ER_WRONG_ARGUMENTS, MYF(0), func_name());
    null_value = true;
    return 0.0;
  }

  /*
    Arg 2: similarity_type (optional, default "cosine")
    Accepted values: cosine, euclidean, dot
  */
  enum class Sim_mode { COSINE, EUCLIDEAN, DOT };
  Sim_mode sim_mode = Sim_mode::COSINE;
  String sim_buf;

  if (arg_count >= 3 && !args[2]->null_value) {
    String *sim_str = args[2]->val_str(&sim_buf);
    if (sim_str && sim_str->length() > 0) {
      const char *s = sim_str->c_ptr_safe();
      if (!my_strcasecmp(system_charset_info, s, "cosine"))
        sim_mode = Sim_mode::COSINE;
      else if (!my_strcasecmp(system_charset_info, s, "euclidean"))
        sim_mode = Sim_mode::EUCLIDEAN;
      else if (!my_strcasecmp(system_charset_info, s, "dot"))
        sim_mode = Sim_mode::DOT;
      else {
        my_printf_error(ER_WRONG_ARGUMENTS,
                        "AI_SIMILARITY: unsupported similarity type '%s'. "
                        "Supported types: cosine, euclidean, dot",
                        MYF(0), s);
        null_value = true;
        return 0.0;
      }
    }
  }

  /*
    Arg 3: model config name for EMBEDDING (optional)
  */
  const char *config_name = nullptr;
  String arg3_buf;

  if (arg_count >= 4 && !args[3]->null_value) {
    String *a3 = args[3]->val_str(&arg3_buf);
    if (a3 && a3->length() > 0) config_name = a3->c_ptr_safe();
  }

  /*
    Detect whether arguments are pre-stored JSON vector arrays
    (starts with '[') or plain text.

    Step 1: recognize VECTOR-typed binary values (e.g. VECTOR(N) columns or
    VEC_FROMTEXT(...) results).  Their packed float32 binary first byte is
    not '[' so the legacy text-only check would misroute them through the
    embedding API; instead convert them to the textual JSON-array form
    upfront and reuse the existing vector-vs-text branches below.

    Step 2: fall back to the legacy '[' first-char heuristic so JSON-text
    vectors authored as string literals (e.g. '[1.0,0.0]') keep working.
  */
  String v1_vec_buf, v2_vec_buf;
  if (try_extract_vector_text(args[0], v1_str, &v1_vec_buf)) {
    v1_str = &v1_vec_buf;
  }
  if (try_extract_vector_text(args[1], v2_str, &v2_vec_buf)) {
    v2_str = &v2_vec_buf;
  }

  const char *arg1_cstr = v1_str->c_ptr_safe();
  const char *arg2_cstr = v2_str->c_ptr_safe();
  /* Trim leading whitespace */
  while (*arg1_cstr && std::isspace(static_cast<unsigned char>(*arg1_cstr)))
    arg1_cstr++;
  while (*arg2_cstr && std::isspace(static_cast<unsigned char>(*arg2_cstr)))
    arg2_cstr++;
  bool arg1_is_vector = (*arg1_cstr == '[');
  bool arg2_is_vector = (*arg2_cstr == '[');

  /*
    When both args are vectors, arg 2 (if given) is treated as model name,
    but no embedding is needed — report it as an error.
  */
  if (arg1_is_vector && arg2_is_vector && config_name) {
    my_printf_error(ER_WRONG_ARGUMENTS,
                    "AI_SIMILARITY: both arguments are vectors; "
                    "model name '%s' is not applicable.",
                    MYF(0), config_name);
    null_value = true;
    return 0.0;
  }

  std::vector<double> query_vec, candidate_vec;
  std::string error_msg;

  if (arg1_is_vector && arg2_is_vector) {
    /* Both are JSON vectors — direct compute, no API call */
    if (parse_json_vector(v1_str->c_ptr_safe(), &query_vec, &error_msg)) {
      my_printf_error(ER_WRONG_ARGUMENTS, "%s", MYF(0), error_msg.c_str());
      null_value = true;
      return 0.0;
    }
    if (parse_json_vector(v2_str->c_ptr_safe(), &candidate_vec, &error_msg)) {
      my_printf_error(ER_WRONG_ARGUMENTS, "%s", MYF(0), error_msg.c_str());
      null_value = true;
      return 0.0;
    }
  } else if (arg2_is_vector) {
    /* Second arg is a JSON vector — parse it directly */
    if (parse_json_vector(v2_str->c_ptr_safe(), &candidate_vec, &error_msg)) {
      my_printf_error(ER_WRONG_ARGUMENTS, "%s", MYF(0), error_msg.c_str());
      null_value = true;
      return 0.0;
    }
    /* Embed the query text */
    std::string query_vec_json;
    if (ai_get_embedding_vector(v1_str->c_ptr_safe(), config_name,
                                func_name(), &query_vec_json, &error_msg)) {
      my_printf_error(ER_WRONG_ARGUMENTS, "%s", MYF(0), error_msg.c_str());
      null_value = true;
      return 0.0;
    }
    if (parse_json_vector(query_vec_json.c_str(), &query_vec, &error_msg)) {
      my_printf_error(ER_WRONG_ARGUMENTS, "%s", MYF(0), error_msg.c_str());
      null_value = true;
      return 0.0;
    }
  } else {
    /* Both are plain text — embed both */
    std::string vec1_json, vec2_json;
    if (ai_get_embedding_vector(v1_str->c_ptr_safe(), config_name,
                                func_name(), &vec1_json, &error_msg)) {
      my_printf_error(ER_WRONG_ARGUMENTS, "%s", MYF(0), error_msg.c_str());
      null_value = true;
      return 0.0;
    }
    if (ai_get_embedding_vector(v2_str->c_ptr_safe(), config_name,
                                func_name(), &vec2_json, &error_msg)) {
      my_printf_error(ER_WRONG_ARGUMENTS, "%s", MYF(0), error_msg.c_str());
      null_value = true;
      return 0.0;
    }
    if (parse_json_vector(vec1_json.c_str(), &query_vec, &error_msg)) {
      my_printf_error(ER_WRONG_ARGUMENTS, "%s", MYF(0), error_msg.c_str());
      null_value = true;
      return 0.0;
    }
    if (parse_json_vector(vec2_json.c_str(), &candidate_vec, &error_msg)) {
      my_printf_error(ER_WRONG_ARGUMENTS, "%s", MYF(0), error_msg.c_str());
      null_value = true;
      return 0.0;
    }
  }

  if (query_vec.size() != candidate_vec.size()) {
    my_printf_error(ER_WRONG_ARGUMENTS,
                    "AI_SIMILARITY: vectors have different dimensions "
                    "(%zu vs %zu).",
                    MYF(0), query_vec.size(), candidate_vec.size());
    null_value = true;
    return 0.0;
  }

  if (query_vec.empty()) {
    my_printf_error(ER_WRONG_ARGUMENTS,
                    "AI_SIMILARITY: vectors are empty.", MYF(0));
    null_value = true;
    return 0.0;
  }

  null_value = false;
  switch (sim_mode) {
    case Sim_mode::EUCLIDEAN:
      return euclidean_distance(query_vec, candidate_vec);
    case Sim_mode::DOT:
      return dot_product(query_vec, candidate_vec);
    case Sim_mode::COSINE:
    default:
      return cosine_similarity(query_vec, candidate_vec);
  }
}

/* ================================================================
   AI_RANK helpers
   ================================================================ */

/**
  Build a DashScope native Rerank API request body JSON.

  Format (for gte-rerank-v2 / qwen3-vl-rerank):
  {
    "model": "gte-rerank-v2",
    "input": {
      "query": "search query",
      "documents": ["candidate text"]
    },
    "parameters": {
      "top_n": 1
    }
  }

  @param model_name  Model identifier
  @param query       Query text
  @param candidate   Candidate document text

  @return  JSON string for the request body
*/
static std::string build_rerank_request(const char *model_name,
                                        const char *query,
                                        const char *candidate,
                                        const char *provider) {
  rapidjson::StringBuffer sb;
  rapidjson::Writer<rapidjson::StringBuffer> w(sb);

  bool openai = is_openai_provider(provider);

  w.StartObject();
  w.Key("model");
  w.String(model_name);

  if (openai) {
    /* OpenAI: flat structure */
    w.Key("query");
    w.String(query);
    w.Key("documents");
    w.StartArray();
    w.String(candidate);
    w.EndArray();
    w.Key("top_n");
    w.Int(1);
    w.Key("return_documents");
    w.Bool(false);
  } else {
    /* DashScope: nested input + parameters */
    w.Key("input");
    w.StartObject();
    w.Key("query");
    w.String(query);
    w.Key("documents");
    w.StartArray();
    w.String(candidate);
    w.EndArray();
    w.EndObject();
    w.Key("parameters");
    w.StartObject();
    w.Key("top_n");
    w.Int(1);
    w.EndObject();
  }

  w.EndObject();

  return std::string(sb.GetString(), sb.GetSize());
}

/**
  Extract the relevance score from a DashScope native Rerank API response.

  Expected format:
  {
    "output": {
      "results": [
        {
          "index": 0,
          "relevance_score": 0.89,
          "document": {"text": "..."}
        }
      ]
    },
    "usage": { "total_tokens": 79 },
    "request_id": "..."
  }

  @param response_json  Raw JSON response string
  @param score          [out] Extracted relevance score
  @param error_msg      [out] Error message on failure

  @retval false  Success
  @retval true   Error
*/
static bool extract_rerank_score(const std::string &response_json,
                                 double *score,
                                 std::string *error_msg,
                                 const char *provider) {
  rapidjson::Document doc;
  doc.Parse(response_json.c_str(), response_json.size());

  if (doc.HasParseError()) {
    *error_msg = "AI_RANK: failed to parse API response as JSON";
    return true;
  }

  /* Check for error response (DashScope native error format) */
  if (doc.HasMember("code") && doc["code"].IsString()) {
    *error_msg = "AI_RANK: API error: ";
    if (doc.HasMember("message") && doc["message"].IsString())
      error_msg->append(doc["message"].GetString());
    else
      error_msg->append(doc["code"].GetString());
    return true;
  }

  /* Check for OpenAI error: {"error":{"message":"..."}} */
  if (doc.HasMember("error") && doc["error"].IsObject()) {
    *error_msg = "AI_RANK: API error: ";
    const auto &err = doc["error"];
    if (err.HasMember("message") && err["message"].IsString())
      error_msg->append(err["message"].GetString());
    else
      error_msg->append("unknown error");
    return true;
  }

  const rapidjson::Value *results_arr = nullptr;

  if (is_openai_provider(provider)) {
    /* OpenAI: results at top level */
    if (!doc.HasMember("results") || !doc["results"].IsArray() ||
        doc["results"].Size() == 0) {
      *error_msg = "AI_RANK: API returned empty results array";
      return true;
    }
    results_arr = &doc["results"];
  } else {
    /* DashScope native: output.results */
    if (!doc.HasMember("output") || !doc["output"].IsObject()) {
      *error_msg = "AI_RANK: API response missing 'output' object";
      return true;
    }
    const auto &output = doc["output"];
    if (!output.HasMember("results") || !output["results"].IsArray() ||
        output["results"].Size() == 0) {
      *error_msg = "AI_RANK: API returned empty results array";
      return true;
    }
    results_arr = &output["results"];
  }

  const auto &first = (*results_arr)[0];
  if (!first.HasMember("relevance_score") ||
      !first["relevance_score"].IsNumber()) {
    *error_msg = "AI_RANK: API returned no relevance_score in first result";
    return true;
  }

  *score = first["relevance_score"].GetDouble();
  return false;
}

/* ================================================================
   Item_func_ai_rank implementation
   ================================================================ */

bool Item_func_ai_rank::resolve_type(THD *thd) {
  if (param_type_is_default(thd, 0, -1)) return true;
  set_data_type_double();
  set_nullable(true);
  return false;
}

double Item_func_ai_rank::val_real() {
  DBUG_TRACE;
  assert(fixed);

  /* Arg 0: query (required) */
  String query_buf;
  String *query_str = args[0]->val_str(&query_buf);
  /* NULL input → NULL output (SQL scalar function convention). */
  if (ai_arg_is_null(args[0], &null_value)) return 0.0;
  if (!query_str || query_str->length() == 0) {
    my_error(ER_WRONG_ARGUMENTS, MYF(0), func_name());
    null_value = true;
    return 0.0;
  }

  /* Arg 1: candidate (required) */
  String cand_buf;
  String *cand_str = args[1]->val_str(&cand_buf);
  if (ai_arg_is_null(args[1], &null_value)) return 0.0;
  if (!cand_str || cand_str->length() == 0) {
    my_error(ER_WRONG_ARGUMENTS, MYF(0), func_name());
    null_value = true;
    return 0.0;
  }

  /* Arg 2: model config name (optional) */
  const char *config_name = nullptr;
  String model_buf;
  if (arg_count >= 3 && !args[2]->null_value) {
    String *m = args[2]->val_str(&model_buf);
    if (m && m->length() > 0) config_name = m->c_ptr_safe();
  }

  /* Arg 3: options JSON (optional, reserved) */

  /* ---- Resolve model from in-memory cache ---- */
  im::Ai_cached_model cached;
  bool found = false;

  if (config_name && config_name[0] != '\0') {
    found = im::Ai_model_cache::instance()->find_by_name(config_name, &cached);
    if (!found) {
      my_printf_error(ER_WRONG_ARGUMENTS,
                      "AI_RANK: model '%s' not found in ai_model_config.",
                      MYF(0), config_name);
      null_value = true;
      return 0.0;
    }
  } else {
    if (find_default_ai_model("AI_RANK", "AI_RANK", &cached, &null_value))
      return 0.0;
  }

  if (cached.endpoint.empty()) {
    my_printf_error(ER_WRONG_ARGUMENTS,
                    "AI_RANK: model '%s' has no endpoint configured.",
                    MYF(0), cached.model_name.c_str());
    null_value = true;
    return 0.0;
  }

  const std::string resolved_ep = cached.endpoint;
  const std::string &effective_key = resolve_api_key(cached);
  if (effective_key.empty()) {
    my_printf_error(ER_WRONG_ARGUMENTS,
                    "AI_RANK: model '%s' has no api_key configured. %s",
                    MYF(0), cached.model_name.c_str(),
                    im::get_no_api_key_hint().c_str());
    null_value = true;
    return 0.0;
  }

  const char *model_name = cached.model.empty()
                               ? cached.model_name.c_str()
                               : cached.model.c_str();

  /* ---- Build request ---- */
  std::string request_body =
      build_rerank_request(model_name, query_str->c_ptr_safe(),
                           cand_str->c_ptr_safe(),
                           cached.provider.c_str());

  /* ---- Execute HTTP POST ---- */
  std::string response_body;
  std::string error_msg;
  if (ai_http_post(resolved_ep.c_str(), effective_key.c_str(),
                   request_body, &response_body, &error_msg)) {
    my_printf_error(ER_WRONG_ARGUMENTS, "%s", MYF(0), error_msg.c_str());
    null_value = true;
    return 0.0;
  }

  /* ---- Parse response ---- */
  double score = 0.0;
  if (extract_rerank_score(response_body, &score, &error_msg,
                           cached.provider.c_str())) {
    my_printf_error(ER_WRONG_ARGUMENTS, "%s", MYF(0), error_msg.c_str());
    null_value = true;
    return 0.0;
  }

  null_value = false;
  return score;
}

/* ================================================================
   Item_func_ai_parse_document implementation
   ================================================================ */

/**
  Build a DashScope native request for document parsing.
  Uses the DashScope native endpoint with doc_url content type.

  qwen-doc-turbo supports file URL via the DashScope native format:
  {
    "model": "qwen-doc-turbo",
    "input": {
      "messages": [
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": [
          {"type": "text", "text": "..."},
          {"type": "doc_url", "doc_url": ["https://..."]}
        ]}
      ]
    }
  }

  @param model_name  Model identifier
  @param file_url    Public URL of the document

  @return  JSON string for the request body
*/
static std::string build_doc_parse_request(const char *model_name,
                                           const char *file_url) {
  rapidjson::StringBuffer sb;
  rapidjson::Writer<rapidjson::StringBuffer> w(sb);

  w.StartObject();
  w.Key("model");
  w.String(model_name);
  w.Key("input");
  w.StartObject();
  w.Key("messages");
  w.StartArray();

  /* system message */
  w.StartObject();
  w.Key("role");
  w.String("system");
  w.Key("content");
  w.String("You are a helpful assistant that extracts and returns the full "
           "text content of documents. Return only the plain text content, "
           "no explanations.");
  w.EndObject();

  /* user message with doc_url */
  w.StartObject();
  w.Key("role");
  w.String("user");
  w.Key("content");
  w.StartArray();
  /* text instruction */
  w.StartObject();
  w.Key("type");
  w.String("text");
  w.Key("text");
  w.String("Please extract and return all the text content from this "
           "document as plain text. Preserve the original structure.");
  w.EndObject();
  /* doc_url reference */
  w.StartObject();
  w.Key("type");
  w.String("doc_url");
  w.Key("doc_url");
  w.StartArray();
  w.String(file_url);
  w.EndArray();
  w.EndObject();
  w.EndArray();
  w.EndObject();

  w.EndArray();
  w.EndObject(); /* end input */
  w.EndObject();
  return std::string(sb.GetString(), sb.GetSize());
}

bool Item_func_ai_parse_document::resolve_type(THD *thd) {
  if (param_type_is_default(thd, 0, -1)) return true;
  set_data_type_string(uint32{65535}, &my_charset_utf8mb4_bin);
  set_nullable(true);
  return false;
}

String *Item_func_ai_parse_document::val_str(String *str [[maybe_unused]]) {
  DBUG_TRACE;
  assert(fixed);

  /* Arg 0: URL (required) */
  String url_buf;
  String *url_str = args[0]->val_str(&url_buf);
  /* NULL input → NULL output (SQL scalar function convention). */
  if (ai_arg_is_null(args[0], &null_value)) return nullptr;
  if (!url_str || url_str->length() == 0) {
    my_error(ER_WRONG_ARGUMENTS, MYF(0), func_name());
    null_value = true;
    return nullptr;
  }

  /* Arg 1: input_format (optional, default "auto")
     NOTE: input_format is parsed and validated here but NOT yet passed to the
     API request (build_doc_parse_request does not accept it).  This is a
     reserved parameter for future use — aligns with CN's 4-arg signature. */
  std::string input_format = "auto";
  if (arg_count >= 2 && !args[1]->null_value) {
    String fmt_buf;
    String *fmt_str = args[1]->val_str(&fmt_buf);
    if (fmt_str && fmt_str->length() > 0) {
      std::string normalized(fmt_str->c_ptr_safe());
      /* Convert to lowercase */
      for (auto &c : normalized) c = std::tolower(static_cast<unsigned char>(c));
      /* Valid API strategies */
      if (normalized == "auto" || normalized == "text_only" ||
          normalized == "text_and_images") {
        input_format = normalized;
      } else if (normalized == "pdf" || normalized == "word" ||
                 normalized == "doc" || normalized == "docx" ||
                 normalized == "ppt" || normalized == "pptx" ||
                 normalized == "txt" || normalized == "image" ||
                 normalized == "markdown" || normalized == "md" ||
                 normalized == "html") {
        /* File-type aliases map to "auto" */
        input_format = "auto";
      } else {
        my_printf_error(ER_WRONG_ARGUMENTS,
                        "AI_PARSE_DOCUMENT: invalid input_format '%s'. "
                        "Supported: auto, text_only, text_and_images, "
                        "or file-type aliases (pdf, word, image, etc.)",
                        MYF(0), fmt_str->c_ptr_safe());
        null_value = true;
        return nullptr;
      }
    }
  }

  /* Arg 2: model config name (optional) */
  const char *config_name = nullptr;
  String model_buf;
  if (arg_count >= 3 && !args[2]->null_value) {
    String *model_str = args[2]->val_str(&model_buf);
    if (model_str && model_str->length() > 0)
      config_name = model_str->c_ptr_safe();
  }

  /* Arg 3: options_json (optional, reserved for future use) */
  /* Currently accepted but not used — matches CN's 4th arg */
  if (arg_count >= 4 && !args[3]->null_value) {
    String opts_buf;
    String *opts_str = args[3]->val_str(&opts_buf);
    if (opts_str && opts_str->length() > 0) {
      rapidjson::Document opts_doc;
      opts_doc.Parse(opts_str->c_ptr_safe(), opts_str->length());
      if (opts_doc.HasParseError() || !opts_doc.IsObject()) {
        my_printf_error(ER_WRONG_ARGUMENTS,
                        "AI_PARSE_DOCUMENT: invalid options JSON", MYF(0));
        null_value = true;
        return nullptr;
      }
    }
  }

  /* ---- Resolve DOCUMENT_PARSE model ---- */
  im::Ai_cached_model cached;
  bool found = false;

  if (config_name && config_name[0] != '\0') {
    found = im::Ai_model_cache::instance()->find_by_name(config_name, &cached);
    if (!found) {
      my_printf_error(ER_WRONG_ARGUMENTS,
                      "AI_PARSE_DOCUMENT: model '%s' not found in "
                      "ai_model_config.",
                      MYF(0), config_name);
      null_value = true;
      return nullptr;
    }
  } else {
    if (find_default_ai_model("AI_PARSE_DOCUMENT", "AI_PARSE_DOCUMENT",
                              &cached, &null_value))
      return nullptr;
  }

  if (cached.endpoint.empty()) {
    my_printf_error(ER_WRONG_ARGUMENTS,
                    "AI_PARSE_DOCUMENT: model '%s' has no endpoint configured.",
                    MYF(0), cached.model_name.c_str());
    null_value = true;
    return nullptr;
  }

  /* PARSE_DOCUMENT uses DashScope-specific format; OpenAI not supported */
  if (is_openai_provider(cached.provider.c_str())) {
    my_printf_error(ER_WRONG_ARGUMENTS,
                    "AI_PARSE_DOCUMENT: OpenAI provider is not supported. "
                    "Use a DashScope model instead.",
                    MYF(0));
    null_value = true;
    return nullptr;
  }

  const std::string resolved_ep = cached.endpoint;
  const std::string &effective_key = resolve_api_key(cached);
  if (effective_key.empty()) {
    my_printf_error(ER_WRONG_ARGUMENTS,
                    "AI_PARSE_DOCUMENT: model '%s' has no api_key configured. %s",
                    MYF(0), cached.model_name.c_str(),
                    im::get_no_api_key_hint().c_str());
    null_value = true;
    return nullptr;
  }

  const char *model_name = cached.model.empty() ? cached.model_name.c_str()
                                                : cached.model.c_str();

  /* ---- Build request ---- */
  std::string request_body =
      build_doc_parse_request(model_name, url_str->c_ptr_safe());

  /* ---- Execute HTTP POST ---- */
  std::string response_body;
  std::string error_msg;
  if (ai_http_post(resolved_ep.c_str(), effective_key.c_str(),
                   request_body, &response_body, &error_msg)) {
    my_printf_error(ER_WRONG_ARGUMENTS, "%s", MYF(0), error_msg.c_str());
    null_value = true;
    return nullptr;
  }

  /* ---- Parse response (DashScope native format: output.choices[0].message.content) ---- */
  std::string content;
  {
    rapidjson::Document doc;
    doc.Parse(response_body.c_str(), response_body.size());
    if (doc.HasParseError()) {
      my_printf_error(ER_WRONG_ARGUMENTS,
                      "AI_PARSE_DOCUMENT: failed to parse API response as JSON",
                      MYF(0));
      null_value = true;
      return nullptr;
    }
    /* Check error */
    if (doc.HasMember("code") && doc["code"].IsString()) {
      std::string errmsg = "AI_PARSE_DOCUMENT: API error: ";
      if (doc.HasMember("message") && doc["message"].IsString())
        errmsg += doc["message"].GetString();
      else
        errmsg += doc["code"].GetString();
      my_printf_error(ER_WRONG_ARGUMENTS, "%s", MYF(0), errmsg.c_str());
      null_value = true;
      return nullptr;
    }
    /* DashScope native: output.choices[0].message.content */
    bool found_content = false;
    if (doc.HasMember("output") && doc["output"].IsObject()) {
      const auto &output = doc["output"];
      if (output.HasMember("choices") && output["choices"].IsArray() &&
          output["choices"].Size() > 0) {
        const auto &choice = output["choices"][0];
        if (choice.HasMember("message") && choice["message"].IsObject() &&
            choice["message"].HasMember("content") &&
            choice["message"]["content"].IsString()) {
          content = choice["message"]["content"].GetString();
          found_content = true;
        }
      }
    }
    /* Fallback: OpenAI format choices[0].message.content */
    if (!found_content) {
      if (extract_chat_content(response_body, &content, &error_msg,
                           cached.provider.c_str())) {
        my_printf_error(ER_WRONG_ARGUMENTS, "%s", MYF(0), error_msg.c_str());
        null_value = true;
        return nullptr;
      }
    }
  }

  null_value = false;
  tmp_value.copy(content.c_str(), content.length(),
                 &my_charset_utf8mb4_bin);
  return &tmp_value;
}

/* ================================================================
   Item_func_ai_vl_embedding implementation
   ================================================================ */

/**
  Build a DashScope multimodal embedding request body JSON.

  DashScope native format (image example):
  {
    "model": "qwen3-vl-embedding",
    "input": {
      "contents": [
        {"image": "https://example.com/image.jpg"}
      ]
    }
  }

  Text example:  {"text": "plain text content"}
  Video example: {"video": "https://example.com/demo.mp4"}

  @param model_name    Model identifier
  @param content       Text string, image URL, or video URL
  @param content_type  "text", "image", or "video"
  @param dimension     Embedding dimension (0 = default)
  @param fps           Video frame rate (0.0 = omit)

  @return  JSON string for the request body
*/
static std::string build_vl_embedding_request(const char *model_name,
                                              const char *content,
                                              const char *content_type,
                                              int dimension = 0,
                                              double fps = 0.0) {
  rapidjson::StringBuffer sb;
  rapidjson::Writer<rapidjson::StringBuffer> w(sb);

  w.StartObject();
  w.Key("model");
  w.String(model_name);
  w.Key("input");
  w.StartObject();
  w.Key("contents");
  w.StartArray();
  w.StartObject();
  w.Key(content_type);  /* "text", "image", or "video" */
  w.String(content);
  w.EndObject();
  w.EndArray();
  w.EndObject();

  /* parameters: dimension, fps (video only) */
  bool need_params = (dimension > 0) || (fps > 0.0);
  if (need_params) {
    w.Key("parameters");
    w.StartObject();
    if (dimension > 0) {
      w.Key("dimension");
      w.Int(dimension);
    }
    if (fps > 0.0) {
      w.Key("fps");
      w.Double(fps);
    }
    w.EndObject();
  }
  w.EndObject();

  return std::string(sb.GetString(), sb.GetSize());
}

/**
  Detect content type ("text", "image", or "video") from input content.

  Priority:
  1. Explicit content_type in options overrides everything.
  2. Base64 Data URI: data:image/... => "image", data:video/... => "video".
  3. HTTP(S) URL: detect by file extension, default to "image".
  4. Plain text (non-URL, non-data-URI): "text".
*/
static std::string detect_vl_content_type(const char *content) {
  std::string lower(content);
  /* Only lowercase a small prefix for scheme detection (avoid full tolower
     on large Base64 data URIs). */
  size_t prefix_len = std::min(lower.size(), size_t(16));
  std::string prefix(lower, 0, prefix_len);
  std::transform(prefix.begin(), prefix.end(), prefix.begin(), ::tolower);

  /* Base64 Data URI */
  if (prefix.compare(0, 5, "data:") == 0) {
    if (prefix.compare(0, 11, "data:image/") == 0) return "image";
    if (prefix.compare(0, 11, "data:video/") == 0) return "video";
    return "image";  /* unknown data URI media type */
  }

  /* HTTP(S) URL — detect by file extension */
  if (prefix.compare(0, 7, "http://") == 0 ||
      prefix.compare(0, 8, "https://") == 0) {
    /* Need full lowercase for extension detection */
    std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);

    /* Strip query string */
    auto qpos = lower.find('?');
    if (qpos != std::string::npos) lower.resize(qpos);

    /* Extract extension */
    auto dot = lower.rfind('.');
    if (dot != std::string::npos && dot + 1 < lower.size()) {
      std::string ext = lower.substr(dot + 1);

      /* Image extensions (matching CN) */
      static const char *img_exts[] = {"jpg",  "jpeg", "png", "webp", "bmp",
                                       "tiff", "tif",  "ico", "dib",  "icns",
                                       "sgi",  nullptr};
      for (const char **p = img_exts; *p; ++p) {
        if (ext == *p) return "image";
      }

      /* Video extensions (matching CN) */
      static const char *vid_exts[] = {"mp4", "avi", "mov", nullptr};
      for (const char **p = vid_exts; *p; ++p) {
        if (ext == *p) return "video";
      }
    }

    return "image";  /* URL without recognized extension defaults to image */
  }

  /* Plain text: not a URL and not a data URI */
  return "text";
}

/**
  Extract the embedding vector from a DashScope multimodal embedding response.

  Expected format:
  {
    "output": {
      "embeddings": [
        {
          "embedding": [0.021, -0.045, ...],
          "index": 0,
          "type": "image"
        }
      ]
    }
  }

  @param response_json  Raw JSON response string
  @param vector_json    [out] JSON array string
  @param error_msg      [out] Error message on failure

  @retval false  Success
  @retval true   Error
*/
static bool extract_dashscope_vl_embedding(const std::string &response_json,
                                           std::string *vector_json,
                                           std::string *error_msg) {
  rapidjson::Document doc;
  doc.Parse(response_json.c_str(), response_json.size());

  if (doc.HasParseError()) {
    *error_msg = "AI_VL_EMBEDDING: failed to parse API response as JSON";
    return true;
  }

  /* Check for error response */
  if (doc.HasMember("code") && doc["code"].IsString()) {
    *error_msg = "AI_VL_EMBEDDING: API error: ";
    if (doc.HasMember("message") && doc["message"].IsString()) {
      error_msg->append(doc["message"].GetString());
    } else {
      error_msg->append(doc["code"].GetString());
    }
    return true;
  }

  /* DashScope native: output.embeddings[0].embedding */
  if (doc.HasMember("output") && doc["output"].IsObject()) {
    const auto &output = doc["output"];
    if (output.HasMember("embeddings") && output["embeddings"].IsArray() &&
        output["embeddings"].Size() > 0) {
      const auto &first = output["embeddings"][0];
      if (first.HasMember("embedding") && first["embedding"].IsArray()) {
        const auto &emb = first["embedding"];
        rapidjson::StringBuffer sb;
        rapidjson::Writer<rapidjson::StringBuffer> w(sb);
        emb.Accept(w);
        *vector_json = std::string(sb.GetString(), sb.GetSize());
        return false;
      }
    }
  }

  *error_msg = "AI_VL_EMBEDDING: API returned no embedding in response";
  return true;
}

bool Item_func_ai_vl_embedding::resolve_type(THD *thd) {
  if (param_type_is_default(thd, 0, -1)) return true;
  set_data_type_string(uint32{65535}, &my_charset_utf8mb4_bin);
  set_nullable(true);
  return false;
}

String *Item_func_ai_vl_embedding::val_str(String *str [[maybe_unused]]) {
  DBUG_TRACE;
  assert(fixed);

  /* Arg 0: content — text string or image/video URL (required) */
  String url_buf;
  String *url_str = args[0]->val_str(&url_buf);
  /* NULL input → NULL output (SQL scalar function convention). */
  if (ai_arg_is_null(args[0], &null_value)) return nullptr;
  if (!url_str || url_str->length() == 0) {
    my_error(ER_WRONG_ARGUMENTS, MYF(0), func_name());
    null_value = true;
    return nullptr;
  }

  /* Arg 1: model config name (optional) */
  const char *config_name = nullptr;
  String model_buf;
  if (arg_count >= 2 && !args[1]->null_value) {
    String *m = args[1]->val_str(&model_buf);
    if (m && m->length() > 0) config_name = m->c_ptr_safe();
  }

  /* Arg 2: options JSON (optional) - supports "dimension", "content_type",
     "fps" */
  int dimension = 0;
  double fps = 0.0;
  std::string opt_content_type;  /* explicit override from options */
  String opts_buf;
  if (arg_count >= 3 && !args[2]->null_value) {
    String *o = args[2]->val_str(&opts_buf);
    if (o && o->length() > 0) {
      rapidjson::Document opts;
      opts.Parse(o->c_ptr_safe());
      if (!opts.HasParseError() && opts.IsObject()) {
        if (opts.HasMember("dimension") && opts["dimension"].IsInt())
          dimension = opts["dimension"].GetInt();
        if (opts.HasMember("content_type") &&
            opts["content_type"].IsString()) {
          std::string ct = opts["content_type"].GetString();
          std::transform(ct.begin(), ct.end(), ct.begin(), ::tolower);
          if (ct == "text" || ct == "image" || ct == "video") {
            opt_content_type = ct;
          } else {
            my_printf_error(
                ER_WRONG_ARGUMENTS,
                "AI_VL_EMBEDDING: invalid content_type '%s'. "
                "Must be 'text', 'image' or 'video'.",
                MYF(0), ct.c_str());
            null_value = true;
            return nullptr;
          }
        }
        if (opts.HasMember("fps") && opts["fps"].IsNumber())
          fps = opts["fps"].GetDouble();
      }
    }
  }

  /* Determine content type: explicit option > auto-detect from URL */
  std::string content_type = opt_content_type.empty()
                                 ? detect_vl_content_type(url_str->c_ptr_safe())
                                 : opt_content_type;
  /* fps only applies to video */
  if (content_type != "video") fps = 0.0;
  im::Ai_cached_model cached;
  bool found = false;

  if (config_name && config_name[0] != '\0') {
    found = im::Ai_model_cache::instance()->find_by_name(config_name, &cached);
    if (!found) {
      my_printf_error(ER_WRONG_ARGUMENTS,
                      "AI_VL_EMBEDDING: model '%s' not found in "
                      "ai_model_config.",
                      MYF(0), config_name);
      null_value = true;
      return nullptr;
    }
  } else {
    if (find_default_ai_model("AI_VL_EMBEDDING", "AI_VL_EMBEDDING",
                              &cached, &null_value))
      return nullptr;
  }

  if (cached.endpoint.empty()) {
    my_printf_error(ER_WRONG_ARGUMENTS,
                    "AI_VL_EMBEDDING: model '%s' has no endpoint configured.",
                    MYF(0), cached.model_name.c_str());
    null_value = true;
    return nullptr;
  }

  /* VL_EMBEDDING uses DashScope-specific multimodal format; OpenAI not supported */
  if (is_openai_provider(cached.provider.c_str())) {
    my_printf_error(ER_WRONG_ARGUMENTS,
                    "AI_VL_EMBEDDING: OpenAI provider is not supported. "
                    "Use a DashScope model instead.",
                    MYF(0));
    null_value = true;
    return nullptr;
  }

  const std::string resolved_ep = cached.endpoint;
  const std::string &effective_key = resolve_api_key(cached);
  if (effective_key.empty()) {
    my_printf_error(ER_WRONG_ARGUMENTS,
                    "AI_VL_EMBEDDING: model '%s' has no api_key configured. %s",
                    MYF(0), cached.model_name.c_str(),
                    im::get_no_api_key_hint().c_str());
    null_value = true;
    return nullptr;
  }

  const char *model_name = cached.model.empty() ? cached.model_name.c_str()
                                                : cached.model.c_str();

  /* ---- Build request ---- */
  std::string request_body = build_vl_embedding_request(
      model_name, url_str->c_ptr_safe(), content_type.c_str(), dimension, fps);

  /* ---- Execute HTTP POST ---- */
  std::string response_body;
  std::string error_msg;
  if (ai_http_post(resolved_ep.c_str(), effective_key.c_str(),
                   request_body, &response_body, &error_msg)) {
    my_printf_error(ER_WRONG_ARGUMENTS, "%s", MYF(0), error_msg.c_str());
    null_value = true;
    return nullptr;
  }

  /* ---- Parse response ---- */
  std::string vector_json;
  if (extract_dashscope_vl_embedding(response_body, &vector_json,
                                     &error_msg)) {
    my_printf_error(ER_WRONG_ARGUMENTS, "%s", MYF(0), error_msg.c_str());
    null_value = true;
    return nullptr;
  }

  null_value = false;
  tmp_value.copy(vector_json.c_str(), vector_json.length(),
                 &my_charset_utf8mb4_bin);
  return &tmp_value;
}

/* ================================================================
   AI_TEXT2SQL helpers
   ================================================================ */

/* Keep aligned with the CN AiText2SqlFunction implementation. */
static const size_t AI_TEXT2SQL_MAX_RELEVANT_TABLES = 10;

/**
  Schema context of one base table collected from the data dictionary.
  The strings are rendered eagerly while the dictionary objects are
  valid, so that no dictionary object or MDL is held across the HTTP
  calls issued later.
*/
struct Ai_text2sql_table_def {
  std::string name;    /* table name */
  std::string summary; /* compact "name(col1, col2, ...)" line */
  std::string ddl;     /* CREATE TABLE style definition */
};

/**
  Append an identifier quoted for the current sql_mode, reusing the
  SHOW CREATE TABLE quoting rules (embedded quote characters doubled).
*/
static void ai_text2sql_append_identifier(const THD *thd, std::string *out,
                                          const dd::String_type &name) {
  StringBuffer<STRING_BUFFER_USUAL_SIZE> buf;
  append_identifier(thd, &buf, name.c_str(), name.length());
  out->append(buf.ptr(), buf.length());
}

/**
  Append a string as a single-quoted SQL literal, reusing the SHOW
  CREATE TABLE escaping rules (append_unescaped handles quotes,
  backslashes and control characters).  The rendered DDL is only LLM
  context, but a stray quote in a comment must not distort the
  statement structure.
*/
static void ai_text2sql_append_quoted(std::string *out,
                                      const dd::String_type &text) {
  StringBuffer<STRING_BUFFER_USUAL_SIZE> buf;
  append_unescaped(&buf, text.c_str(), text.length());
  out->append(buf.ptr(), buf.length());
}

/**
  Render a CREATE TABLE style definition from a dd::Table object.

  The definition is rebuilt from dictionary metadata only: the real SHOW
  CREATE TABLE path (store_create_info) requires an opened TABLE, which
  must not happen inside scalar function evaluation.  Columns, keys and
  comments are sufficient schema context for the LLM.
*/
static void ai_text2sql_render_table(THD *thd, const char *db_name,
                                     const dd::Table *table,
                                     GRANT_INFO *grant,
                                     bool table_level_select,
                                     Ai_text2sql_table_def *def) {
  def->name.assign(table->name().c_str(), table->name().length());

  /* When the table-level grant does not fully cover SELECT (the caller
     reached here through a column-level grant), drop every column the
     user cannot read: unauthorized column names, types and comments
     must not be sent to the LLM.  get_column_grant() is the error-free
     lookup used by SHOW COLUMNS; it never touches the diagnostics
     area.  The GRANT_INFO passed in is the one populated by
     check_table_access(), so its table-level privileges and the
     already-loaded grant cache are honored. */

  std::string cols; /* "col1, col2" for the compact summary */
  std::string body; /* column and key lines of the DDL */

  for (const dd::Column *col : table->columns()) {
    /* Skip columns invisible to users (SE internals, functional index
       columns).  INVISIBLE user columns stay queryable, keep them. */
    if (col->hidden() != dd::Column::enum_hidden_type::HT_VISIBLE &&
        col->hidden() != dd::Column::enum_hidden_type::HT_HIDDEN_USER)
      continue;

    if (!table_level_select) {
      const bool granted =
          (get_column_grant(thd, grant, db_name, table->name().c_str(),
                            col->name().c_str()) &
           SELECT_ACL) != 0;
      if (!granted) continue;
    }

    if (!cols.empty()) cols.append(", ");
    cols.append(col->name().c_str());

    if (!body.empty()) body.append(",\n");
    body.append("  ");
    ai_text2sql_append_identifier(thd, &body, col->name());
    body.append(" ");
    body.append(col->column_type_utf8().c_str());
    if (!col->is_nullable()) body.append(" NOT NULL");
    if (col->is_auto_increment()) body.append(" AUTO_INCREMENT");
    if (!col->comment().empty()) {
      body.append(" COMMENT ");
      ai_text2sql_append_quoted(&body, col->comment());
    }
  }

  for (const dd::Index *idx : table->indexes()) {
    if (idx->is_hidden()) continue;

    std::string key_cols;
    bool all_cols_visible = true;
    for (const dd::Index_element *el : idx->elements()) {
      if (el->is_hidden()) continue;
      const dd::Column &idx_col = el->column();
      /* An index exposing through a column the user cannot read would
         leak that column, skip the whole index. */
      if (!table_level_select) {
        const bool granted =
            (get_column_grant(thd, grant, db_name, table->name().c_str(),
                              idx_col.name().c_str()) &
             SELECT_ACL) != 0;
        if (!granted) {
          all_cols_visible = false;
          break;
        }
      }
      if (!key_cols.empty()) key_cols.append(", ");
      ai_text2sql_append_identifier(thd, &key_cols, idx_col.name());
    }
    if (!all_cols_visible || key_cols.empty()) continue;

    if (!body.empty()) body.append(",\n");
    switch (idx->type()) {
      case dd::Index::IT_PRIMARY:
        body.append("  PRIMARY KEY (").append(key_cols).append(")");
        break;
      case dd::Index::IT_UNIQUE:
        body.append("  UNIQUE KEY ");
        ai_text2sql_append_identifier(thd, &body, idx->name());
        body.append(" (").append(key_cols).append(")");
        break;
      default:
        body.append("  KEY ");
        ai_text2sql_append_identifier(thd, &body, idx->name());
        body.append(" (").append(key_cols).append(")");
        break;
    }
  }

  def->summary.assign(def->name);
  def->summary.append("(").append(cols).append(")");

  def->ddl.assign("CREATE TABLE ");
  ai_text2sql_append_identifier(thd, &def->ddl, table->name());
  def->ddl.append(" (\n").append(body).append("\n)");
  if (!table->comment().empty()) {
    def->ddl.append(" COMMENT=");
    ai_text2sql_append_quoted(&def->ddl, table->comment());
  }
  def->ddl.append(";\n");
}

/**
  Collect the schema context of the visible base tables in the current
  database.  Tables on which the current user has no SELECT privilege
  at any level (global, database, table or column) are skipped
  silently.

  The schema MDL and the dictionary objects are released on return; only
  the rendered strings escape this function, so no dictionary state is
  held across the HTTP calls issued by the caller.

  @retval false  Success
  @retval true   Error (reported)
*/
static bool ai_text2sql_collect_tables(
    THD *thd, std::vector<Ai_text2sql_table_def> *defs) {
  if (thd->db().str == nullptr || thd->db().length == 0) {
    my_error(ER_NO_DB_ERROR, MYF(0));
    return true;
  }
  const std::string db_name(thd->db().str, thd->db().length);

  /* ACL code assumes that in lower_case_table_names > 0 modes the
     schema and table names passed to it are lower case (see
     check_table_and_trigger_access() / CAN_ACCESS_TABLE).  Build an
     ACL-only copy; the original name keeps being used for rendering. */
  std::string acl_db;
  if (lower_case_table_names == 2) {
    char db_buf[NAME_LEN + 1];
    memcpy(db_buf, thd->db().str, thd->db().length);
    db_buf[thd->db().length] = '\0';
    my_casedn_str(files_charset_info, db_buf);
    acl_db = db_buf;
  }
  const char *acl_db_name =
      (lower_case_table_names == 2) ? acl_db.c_str() : db_name.c_str();

  dd::Schema_MDL_locker mdl_handler(thd);
  dd::cache::Dictionary_client::Auto_releaser releaser(thd->dd_client());

  const dd::Schema *sch = nullptr;
  std::vector<const dd::Table *> tables;
  if (mdl_handler.ensure_locked(db_name.c_str()) ||
      thd->dd_client()->acquire(db_name.c_str(), &sch)) {
    /* Error has been reported by the dictionary subsystem. */
    return true;
  }
  if (sch == nullptr) {
    my_error(ER_BAD_DB_ERROR, MYF(0), db_name.c_str());
    return true;
  }
  if (thd->dd_client()->fetch_schema_components(sch, &tables)) return true;

  for (const dd::Table *table : tables) {
    if (table->hidden() != dd::Abstract_table::HT_VISIBLE) continue;

    /* Silently skip tables the user is not allowed to read.
       any_combination_of_privileges_will_do must be false: SELECT is
       required, satisfiable at any level including a column-level
       grant (check_grant lets column ACLs cover the remainder).  With
       true, any unrelated grant (e.g. INSERT-only) would pass.
       A table-level SELECT is recorded so the renderer knows whether
       every column is readable or a per-column check is needed. */
    Table_ref table_ref(db_name.c_str(), table->name().c_str(), TL_READ);
    std::string acl_table;
    if (lower_case_table_names == 2) {
      char tbl_buf[NAME_LEN + 1];
      strmake(tbl_buf, table->name().c_str(), NAME_LEN);
      my_casedn_str(files_charset_info, tbl_buf);
      acl_table = tbl_buf;
      table_ref.db = acl_db_name;
      table_ref.db_length = strlen(acl_db_name);
      table_ref.table_name = acl_table.c_str();
      table_ref.table_name_length = acl_table.length();
    }
    if (check_table_access(thd, SELECT_ACL, &table_ref, false, 1, true))
      continue;
    const bool table_level_select =
        !(~table_ref.grant.privilege & SELECT_ACL);

    Ai_text2sql_table_def def;
    ai_text2sql_render_table(thd, acl_db_name, table,
                             &table_ref.grant, table_level_select, &def);
    defs->push_back(std::move(def));
  }
  return false;
}

/**
  Ask the LLM which tables are relevant to the question.  Used when the
  database holds more than AI_TEXT2SQL_MAX_RELEVANT_TABLES tables, with
  only the compact "name(col1, col2, ...)" summaries to keep the request
  small.

  Mirrors the CN AiText2SqlFunction behavior: a failure of this
  pre-filter round must fall back to the first tables instead of failing
  the statement, so no error is raised here.

  @return  Indexes into defs of the selected tables; empty on failure.
*/
static std::vector<size_t> ai_text2sql_filter_tables(
    const im::Ai_cached_model &cached, const std::string &api_key,
    const char *prompt, const std::vector<Ai_text2sql_table_def> &defs) {
  std::vector<size_t> selected;

  std::string filter_prompt =
      "Given the following numbered database tables and their columns:\n\n";
  for (size_t i = 0; i < defs.size(); i++) {
    filter_prompt.append(std::to_string(i))
        .append(": ")
        .append(defs[i].summary)
        .append("\n");
  }
  filter_prompt.append("\nUser question: ").append(prompt);
  filter_prompt.append(
      "\n\nWhich tables are needed to answer this question? Reply with "
      "ONLY a JSON array of the table numbers shown above, nothing "
      "else, at most ");
  filter_prompt.append(std::to_string(AI_TEXT2SQL_MAX_RELEVANT_TABLES));
  filter_prompt.append(" numbers. Example response: [1,4,7]");

  const char *model_name = cached.model.empty() ? cached.model_name.c_str()
                                                : cached.model.c_str();
  std::string request_body = build_chat_request(
      model_name, filter_prompt.c_str(),
      "{\"temperature\":0,\"max_tokens\":200}", cached.endpoint.c_str(),
      cached.provider.c_str());

  std::string response_body;
  std::string error_msg;
  if (ai_http_post(cached.endpoint.c_str(), api_key.c_str(), request_body,
                   &response_body, &error_msg))
    return selected;

  std::string content;
  if (extract_chat_content(response_body, &content, &error_msg,
                           cached.provider.c_str()))
    return selected;

  /* The reply must be a JSON array of the table numbers given in the
     filter prompt.  Numbers are unambiguous and cannot collide the way
     free-text names do (a table named `a,b` could never be told apart
     from two tables `a` and `b`), unknown names cannot match as
     prefixes of real ones, and duplicates are dropped.  A malformed
     reply yields no selection and the caller falls back to the first
     tables. */
  strip_markdown_json_wrapper(content);
  size_t open = content.find('[');
  size_t close = content.rfind(']');
  if (open == std::string::npos || close == std::string::npos ||
      close < open)
    return selected;

  rapidjson::Document nums;
  nums.Parse(content.c_str() + open, close - open + 1);
  if (nums.HasParseError() || !nums.IsArray()) return selected;

  std::vector<bool> seen(defs.size(), false);
  for (const auto &v : nums.GetArray()) {
    if (!v.IsInt()) continue;
    const int idx = v.GetInt();
    if (idx < 0 || static_cast<size_t>(idx) >= defs.size()) continue;
    if (seen[idx]) continue;
    seen[idx] = true;
    selected.push_back(static_cast<size_t>(idx));
    if (selected.size() >= AI_TEXT2SQL_MAX_RELEVANT_TABLES) break;
  }
  return selected;
}

/* ================================================================
   Item_func_ai_text2sql implementation
   ================================================================ */

bool Item_func_ai_text2sql::resolve_type(THD *thd) {
  if (param_type_is_default(thd, 0, -1)) return true;
  set_data_type_string(uint32{65535}, &my_charset_utf8mb4_bin);
  set_nullable(true);
  return false;
}

String *Item_func_ai_text2sql::val_str(String *str [[maybe_unused]]) {
  DBUG_TRACE;
  assert(fixed);

  THD *thd = current_thd;

  /* ---- Arg 0: prompt (required) ---- */
  String prompt_buf;
  String *prompt_str = args[0]->val_str(&prompt_buf);
  /* A failed child evaluation (e.g. error_str() semantics with a
     non-nullable child) must abort the function: do not touch the
     dictionary or issue network requests for a statement that is
     already dead. */
  if (thd->is_error()) {
    null_value = true;
    return nullptr;
  }
  /* NULL input → NULL output (SQL scalar function convention). */
  if (ai_arg_is_null(args[0], &null_value)) return nullptr;
  if (!prompt_str) {
    my_error(ER_WRONG_ARGUMENTS, MYF(0), func_name());
    null_value = true;
    return nullptr;
  }
  if (prompt_str->length() == 0) {
    my_printf_error(ER_WRONG_ARGUMENTS, "AI_TEXT2SQL: prompt cannot be empty",
                    MYF(0));
    null_value = true;
    return nullptr;
  }

  /* ---- Arg 1: model config name (optional) ----
     Evaluate first, then test the fresh NULL state: args[i]->null_value
     is only updated by val_str(), so testing it beforehand would reuse
     the previous row's state (a NULL in row N would make row N+1 skip
     its non-NULL model argument). */
  const char *config_name = nullptr;
  String model_buf;
  if (arg_count >= 2) {
    String *m = args[1]->val_str(&model_buf);
    if (thd->is_error()) {
      null_value = true;
      return nullptr;
    }
    if (m && !args[1]->null_value && m->length() > 0)
      config_name = m->c_ptr_safe();
  }

  /* ---- Arg 2: options JSON (optional) -- temperature, max_tokens ---- */
  double opt_temperature = 0.0;
  int opt_max_tokens = 0; /* 0 = model default */
  if (arg_count >= 3) {
    String opts_buf;
    String *opts_str = args[2]->val_str(&opts_buf);
    if (thd->is_error()) {
      null_value = true;
      return nullptr;
    }
    if (opts_str && !args[2]->null_value && opts_str->length() > 0) {
      rapidjson::Document opts_doc;
      opts_doc.Parse(opts_str->c_ptr_safe(), opts_str->length());
      if (opts_doc.HasParseError() || !opts_doc.IsObject()) {
        my_printf_error(ER_WRONG_ARGUMENTS,
                        "AI_TEXT2SQL: invalid options JSON", MYF(0));
        null_value = true;
        return nullptr;
      }
      if (opts_doc.HasMember("temperature") &&
          opts_doc["temperature"].IsNumber())
        opt_temperature = opts_doc["temperature"].GetDouble();
      if (opts_doc.HasMember("max_tokens") && opts_doc["max_tokens"].IsInt())
        opt_max_tokens = opts_doc["max_tokens"].GetInt();
    }
  }

  /* ---- Resolve model from in-memory cache ----
     Resolved before collecting tables because the pre-filter round needs
     the endpoint and credentials. */
  im::Ai_cached_model cached;
  if (config_name && config_name[0] != '\0') {
    if (!im::Ai_model_cache::instance()->find_by_name(config_name, &cached)) {
      my_printf_error(ER_WRONG_ARGUMENTS,
                      "AI_TEXT2SQL: model '%s' not found in ai_model_config.",
                      MYF(0), config_name);
      null_value = true;
      return nullptr;
    }
  } else {
    if (find_default_ai_model("AI_TEXT2SQL", "AI_TEXT2SQL", &cached,
                              &null_value))
      return nullptr;
  }

  if (cached.endpoint.empty()) {
    my_printf_error(ER_WRONG_ARGUMENTS,
                    "AI_TEXT2SQL: model '%s' has no endpoint configured.",
                    MYF(0), cached.model_name.c_str());
    null_value = true;
    return nullptr;
  }

  const std::string effective_key = resolve_api_key(cached);
  if (effective_key.empty()) {
    my_printf_error(ER_WRONG_ARGUMENTS,
                    "AI_TEXT2SQL: model '%s' has no api_key configured. %s",
                    MYF(0), cached.model_name.c_str(),
                    im::get_no_api_key_hint().c_str());
    null_value = true;
    return nullptr;
  }

  /* ---- Collect table definitions of the current database ---- */
  std::vector<Ai_text2sql_table_def> defs;
  if (ai_text2sql_collect_tables(thd, &defs)) {
    null_value = true;
    return nullptr;
  }

  /* When there are too many tables, ask the LLM to pick the relevant
     ones first; fall back to the first tables if nothing was selected. */
  std::vector<size_t> selected;
  if (defs.size() > AI_TEXT2SQL_MAX_RELEVANT_TABLES) {
    selected = ai_text2sql_filter_tables(cached, effective_key,
                                         prompt_str->c_ptr_safe(), defs);
    if (selected.empty()) {
      for (size_t i = 0; i < AI_TEXT2SQL_MAX_RELEVANT_TABLES; i++)
        selected.push_back(i);
    }
  } else {
    for (size_t i = 0; i < defs.size(); i++) selected.push_back(i);
  }

  /* ---- Build the text2sql system prompt ---- */
  std::string system_prompt =
      "You are a SQL expert. Convert natural language queries into valid "
      "SQL statements.\n\n"
      "Rules:\n"
      "1. Only output the SQL statement, no explanation or markdown "
      "formatting.\n"
      "2. Use only the tables and columns provided below.\n"
      "3. Use standard SQL syntax compatible with MySQL.\n"
      "4. If the query is ambiguous, make reasonable assumptions.\n"
      "5. Only generate a single read-only SELECT statement.\n\n";
  if (!selected.empty()) {
    system_prompt.append("Available table schemas:\n\n");
    for (size_t i : selected) {
      system_prompt.append(defs[i].ddl).append("\n");
    }
  }

  /* Both rounds (pre-filter above, generation here) use the same
     resolved snapshot so a concurrent cache reload cannot split them
     across different endpoints or credentials. */
  String *result = ai_llm_with_system_prompt_model(
      func_name(), prompt_str->c_ptr_safe(), system_prompt, cached,
      &null_value, &tmp_value, opt_temperature, opt_max_tokens);
  if (!result) return nullptr;

  /* Strip the markdown code fence (```sql ... ```) the LLM may wrap the
     generated SQL in. */
  std::string content(result->ptr(), result->length());
  strip_markdown_json_wrapper(content);
  /* String::copy() reports allocation failure; the error is already
     recorded by the allocator, do not return stale buffer content. */
  if (tmp_value.copy(content.c_str(), content.length(),
                     &my_charset_utf8mb4_bin)) {
    null_value = true;
    return nullptr;
  }
  return &tmp_value;
}
