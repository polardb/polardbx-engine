
#include "../global_defines.h"
#ifndef MYSQL8
#define MYSQL_SERVER
#endif
#include "sql/protocol.h"
#include "sql/sql_class.h"

#include "log.h"
#include "protocol.h"

namespace rpc_executor {

int Protocol::write_metadata(InternalDataSet &dataset) {
  int ret = 0;
  ExecTable *exec_table = dataset.table();
  TABLE *table = exec_table->table();
  if (xprotocol_.start_result_metadata(
          table->s->fields, ::Protocol::SEND_NUM_ROWS | ::Protocol::SEND_EOF,
          &my_charset_utf8mb4_general_ci)) {
    ret = 1;
    log_exec_error("write metadata failed");
  } else {
#ifdef MYSQL8
    auto &table_name_lex = table->s->table_name;
    auto &schema_name_lex = table->s->db;
#else
    MYSQL_LEX_STRING &table_name_lex = table->s->table_name;
    MYSQL_LEX_STRING &schema_name_lex = table->s->db;
#endif
    std::string table_name(table_name_lex.str, table_name_lex.length);
    std::string schema_name(schema_name_lex.str, schema_name_lex.length);

    Send_field f;
    if (dataset.is_aggr()) {
      auto aggr_info = dataset.get_aggr_info();
      const char *field_name = aggr_info.first.data();
      auto aggr_expr = aggr_info.second;
      auto *protocol_charset = aggr_expr->charset_for_protocol();
      aggr_expr->make_field(&f);
      f.col_name = f.org_col_name = field_name;
      if (xprotocol_.send_field_metadata(&f, protocol_charset)) {
        ret = 1;
        log_exec_error("send_field_metadata from aggr_expr failed");
      }
    } else if (dataset.has_project()) {
      for (auto &expr : dataset.get_project_exprs()) {
        expr.second->make_field(&f);
        f.col_name = f.org_col_name = expr.first.data();
        auto *protocol_charset = expr.second->charset_for_protocol();
        if (xprotocol_.send_field_metadata(&f, protocol_charset)) {
          ret = 1;
          log_exec_error("send_field_metadata from expr failed 2");
        }
      }
    } else {
      Field **project_fields = nullptr;
      uint32_t field_count = 0;
      dataset.get_all_fields(project_fields, field_count);
      for (uint32_t i = 0; i < field_count; ++i) {
        Field *this_field = project_fields[i];

#ifdef MYSQL8PLUS
        this_field->make_send_field(&f);
#else
        this_field->make_field(&f);
#endif
        f.db_name = schema_name.data();
        f.table_name = table_name.data();
        f.org_table_name = f.db_name;

        if (xprotocol_.send_field_metadata(&f, this_field->charset())) {
          ret = 1;
          log_exec_error("send_field_metadata from field failed");
        }
      }
    }
    if (xprotocol_.end_result_metadata()) {
      log_exec_error("end_result_metadata failed");
      ret = 1;
    }
  }
  return ret;
}

int Protocol::write_row(InternalDataSet &dataset) {
  int ret = 0;
  xprotocol_.start_row();

  THD *thd = current_thd;

  if (dataset.is_aggr()) {
    String buffer;
    auto aggr_expr = dataset.get_aggr_info().second;
    aggr_expr->send(&xprotocol_, &buffer);
    if (thd->get_stmt_da()->is_error()) {
      ret = (int)thd->get_stmt_da()->mysql_errno();
      log_exec_error("Agg Item send failed, reason: %s",
                     thd->get_stmt_da()->message_text());
    }
  } else if (dataset.has_project()) {
    String buffer;
    for (auto &expr : dataset.get_project_exprs()) {
      expr.second->send(&xprotocol_, &buffer);
      if (thd->get_stmt_da()->is_error()) {
        ret = (int)thd->get_stmt_da()->mysql_errno();
        log_exec_error("Item send failed, reason: %s",
                       thd->get_stmt_da()->message_text());
        break;
      }
    }
  } else {
    Field **project_fields = nullptr;
    uint32_t field_count = 0;
    dataset.get_all_fields(project_fields, field_count);
    for (uint32_t i = 0; i < field_count; ++i) {
      Field *this_field = project_fields[i];
      // xprotocol_.store(this_field->ptr, this_field->pack_length(),
      // this_field->charset());
#ifdef MYSQL8
      xprotocol_.store_field(this_field);
#else
      xprotocol_.store(this_field);
#endif
    }
  }
  // TODO: check again if we need to end_row when failed
  xprotocol_.end_row();
  /// add row sent
  thd->inc_sent_row_count(1);
  return ret;
}

int Protocol::send_and_flush() {
  int ret = 0;
  xprotocol_.send_eof(2, 0);
  return ret;
}

}  // namespace rpc_executor
