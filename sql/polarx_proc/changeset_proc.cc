//
// Created by wumu on 2022/10/19.
//
#include "changeset_proc.h"

namespace im {
/**
 * polarx.changeset_start
 */
Proc *Changeset_proc_start::instance() {
  static Proc *proc = new Changeset_proc_start(key_memory_package);
  return proc;
}

Sql_cmd *Changeset_proc_start::evoke_cmd(THD *thd,
                                         mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

void Sql_cmd_changeset_proc_start::send_result(THD *thd, bool error) {
  if (!opt_enable_changeset) {
    my_error(ER_CHANGESET_COMMAND_ERROR, MYF(0),
             "call changeset proc failed, the changeset proc is not support");
    return;
  }
  Protocol *proto = thd->get_protocol();
  if (error) {
    my_eof(thd);
    return;
  }

  // process parameters
  auto it = m_list->begin();
  Item_string *table_name_item = dynamic_cast<Item_string *>(*(it++));
  if (table_name_item == nullptr) {
    my_error(ER_CHANGESET_COMMAND_ERROR, MYF(0),
             "call changeset start failed, table name is illegal");
    return;
  }

  u_int64_t memory_limit = MAX_MEMORY_SIZE;
  if (it != m_list->end()) {
    Item_int *memory_limit_item = dynamic_cast<Item_int *>(*(it++));
    memory_limit = (u_int64_t)memory_limit_item->value;
  }

  std::string table_name(table_name_item->val_str(nullptr)->ptr(),
                         table_name_item->val_str(nullptr)->length());

  sql_print_information("changeset.start(%s)", table_name.data());

  if (gChangesetManager.start_track(table_name, memory_limit)) {
    my_error(ER_CHANGESET_COMMAND_ERROR, MYF(0), "changeset start failed");
    return;
  }

  // send result
  if (m_proc->send_result_metadata(thd)) return;
  proto->start_row();
  proto->store(table_name.data(), &my_charset_utf8mb3_bin);
  proto->end_row();

  my_eof(thd);
}

/**
 * polarx.changeset_fetch
 */
Proc *Changeset_proc_fetch::instance() {
  static Proc *proc = new Changeset_proc_fetch(key_memory_package);
  return proc;
}

Sql_cmd *Changeset_proc_fetch::evoke_cmd(THD *thd,
                                         mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

bool Changeset_proc_fetch::my_send_result_metadata(
    THD *thd, Proc::Columns columns, std::list<Field *> fields) const {
  mem_root_deque<Item *> field_list(thd->mem_root);
  Item *item;
  DBUG_ENTER("Proc::send_result_metadata");
  for (Columns::const_iterator it = columns.begin(); it != columns.end();
       it++) {
    /* TODO: Support more field type */
    switch ((*it).type) {
      case MYSQL_TYPE_LONGLONG:
        field_list.push_back(
            item = new Item_int(Name_string((*it).name, (*it).name_len),
                                (*it).size, MY_INT64_NUM_DECIMAL_DIGITS));
        item->set_nullable(true);
        break;
      case MYSQL_TYPE_VARCHAR:
        field_list.push_back(item =
                                 new Item_empty_string((*it).name, (*it).size));
        item->set_nullable(true);
        break;
      default:
        assert(0);
    }
  }
  for (const auto &field : fields) {
    switch (field->type()) {
      case MYSQL_TYPE_BIT:
      case MYSQL_TYPE_GEOMETRY:
      case MYSQL_TYPE_YEAR:
      case MYSQL_TYPE_TINY:
      case MYSQL_TYPE_LONG:
      case MYSQL_TYPE_SHORT:
      case MYSQL_TYPE_LONGLONG:
      case MYSQL_TYPE_INT24:
        if (!(item = new Item_return_int(field->field_name, field->field_length,
                                         field->type()))) {
          DBUG_RETURN(0);
        }
        item->unsigned_flag = (field->all_flags() & UNSIGNED_FLAG);
        break;
      case MYSQL_TYPE_DATE:
      case MYSQL_TYPE_TIME:
      case MYSQL_TYPE_TIMESTAMP:
      case MYSQL_TYPE_DATETIME: {
        const Name_string field_name(field->field_name,
                                     strlen(field->field_name));
        if (!(item = new Item_temporal(field->type(), field_name, 0, 0)))
          DBUG_RETURN(0);
        if (field->type() == MYSQL_TYPE_TIMESTAMP ||
            field->type() == MYSQL_TYPE_DATETIME)
          item->decimals = field->field_length;
        break;
      }
      case MYSQL_TYPE_FLOAT:
      case MYSQL_TYPE_DOUBLE: {
        const Name_string field_name(field->field_name,
                                     strlen(field->field_name));
        if ((item = new Item_float(field_name, 0.0, DECIMAL_NOT_SPECIFIED,
                                   field->field_length)) == nullptr)
          DBUG_RETURN(NULL);
        break;
      }
      case MYSQL_TYPE_DECIMAL:
      case MYSQL_TYPE_NEWDECIMAL:
        if (!(item = new Item_decimal((longlong)0L, false))) {
          DBUG_RETURN(0);
        }
        item->unsigned_flag = (field->all_flags() & MY_I_S_UNSIGNED);
        item->decimals = field->field_length % 10;
        item->max_length = (field->field_length / 100) % 100;
        if (item->unsigned_flag == 0) item->max_length += 1;
        if (item->decimals > 0) item->max_length += 1;
        item->item_name.copy(field->field_name);
        break;
      case MYSQL_TYPE_TINY_BLOB:
      case MYSQL_TYPE_MEDIUM_BLOB:
      case MYSQL_TYPE_LONG_BLOB:
      case MYSQL_TYPE_BLOB:
        if (!(item = new Item_blob(field->field_name, field->field_length))) {
          DBUG_RETURN(0);
        }
        break;
      case MYSQL_TYPE_VARCHAR:
        if (!(item = new Item_empty_string(
                  field->field_name, field->char_length(), field->charset()))) {
          DBUG_RETURN(0);
        }
        break;
      default:
        /* Don't let unimplemented types pass through. Could be a grave error.
         */
        assert(field->type() == MYSQL_TYPE_STRING);
        if (!(item = new Item_return_int(field->field_name, field->field_length,
                                         field->type()))) {
          DBUG_RETURN(0);
        }
        break;
    }
    field_list.push_back(item);
    item->set_nullable(field->all_flags() & MY_I_S_MAYBE_NULL);
  }
  if (thd->send_result_metadata(field_list,
                                Protocol::SEND_NUM_ROWS | Protocol::SEND_EOF))
    DBUG_RETURN(true);
  DBUG_RETURN(false);
}

void Sql_cmd_changeset_proc_fetch::send_result(THD *thd, bool error) {
  if (!opt_enable_changeset) {
    my_error(ER_CHANGESET_COMMAND_ERROR, MYF(0),
             "call changeset proc failed, the changeset proc is not support");
    return;
  }
  Protocol *proto = thd->get_protocol();
  if (error) {
    my_eof(thd);
    return;
  }

  // process parameters
  auto it = m_list->begin();
  Item_string *table_name_item = dynamic_cast<Item_string *>(*(it++));
  if (table_name_item == nullptr) {
    my_error(ER_CHANGESET_COMMAND_ERROR, MYF(0),
             "call changeset fetch failed, table name is illegal");
    return;
  }

  bool delete_last_cs = true;
  if (it != m_list->end()) {
    Item_int *fetch_type_item = dynamic_cast<Item_int *>(*(it++));
    delete_last_cs = fetch_type_item == nullptr || fetch_type_item->value == 0;
  }

  std::string table_name(table_name_item->val_str(nullptr)->ptr(),
                         table_name_item->val_str(nullptr)->length());

  sql_print_information("changeset.fetch(%s)", table_name.data());

  // get table
  TABLE *table;
  gChangesetManager.open_table(table_name, &table, TL_READ);

  std::vector<ChangesetResult *> changes;
  if (gChangesetManager.fetch_change(table_name, delete_last_cs, changes,
                                     table->s)) {
    my_printf_error(ER_CHANGESET_COMMAND_ERROR, "changeset fetch failed", 0);
    return;
  }

  // add column
  if (!changes.empty()) {
    auto begin = *changes.begin();
    auto m_columns = im::Proc::Columns(0);
    m_columns.push_back({MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("OP"), 24});
    if ((dynamic_cast<const Changeset_proc_fetch *>(m_proc))
            ->my_send_result_metadata(thd, m_columns,
                                      begin->pk_field_list))
      return;
  } else {
    // send result
    if (m_proc->send_result_metadata(thd)) return;
  }

  for (auto row : changes) {
    proto->start_row();
    proto->store(row->get_op_string().data(), &my_charset_utf8mb3_bin);
    for (auto pk : row->pk_field_list) {
      pk->table = table;
      pk->send_to_protocol(proto);
    }
    proto->end_row();
    delete row;
  }

  my_eof(thd);
}

/**
 * polarx.changeset_finish
 */
Proc *Changeset_proc_finish::instance() {
  static Proc *proc = new Changeset_proc_finish(key_memory_package);
  return proc;
}

Sql_cmd *Changeset_proc_finish::evoke_cmd(THD *thd,
                                          mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

void Sql_cmd_changeset_proc_finish::send_result(THD *thd, bool error) {
  if (error) {
    my_eof(thd);
    return;
  }

  // process parameters
  // finish_changeset(table_name)
  auto it = m_list->begin();
  Item_string *table_name_item = dynamic_cast<Item_string *>(*(it++));
  if (table_name_item == nullptr) {
    my_error(ER_CHANGESET_COMMAND_ERROR, MYF(0),
             "call changeset finish failed, table name is illegal");
    return;
  }

  std::string table_name(table_name_item->val_str(nullptr)->ptr(),
                         table_name_item->val_str(nullptr)->length());

  sql_print_information("changeset.finish(%s)", table_name.data());

  if (gChangesetManager.stop_track(table_name)) {
    my_printf_error(ER_CHANGESET_COMMAND_ERROR, "changeset not exists", 0);
    return;
  }

  if (m_proc->send_result_metadata(thd)) return;

  my_eof(thd);
}

///////////////////////// polarx.changeset_stop ///////////////////////////////
Proc *Changeset_proc_stop::instance() {
  static Proc *proc = new Changeset_proc_stop(key_memory_package);
  return proc;
}

Sql_cmd *Changeset_proc_stop::evoke_cmd(THD *thd,
                                        mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

void Sql_cmd_changeset_proc_stop::send_result(THD *thd, bool error) {
  if (error) {
    return;
  }

  // process parameters
  auto it = m_list->begin();
  Item_string *table_name_item = dynamic_cast<Item_string *>(*(it++));
  if (table_name_item == nullptr) {
    my_error(ER_CHANGESET_COMMAND_ERROR, MYF(0),
             "call changeset stop failed, table name is illegal");
    return;
  }

  std::string table_name(table_name_item->val_str(NULL)->ptr(),
                         table_name_item->val_str(nullptr)->length());

  sql_print_information("changeset.stop(%s)", table_name.data());

  if (gChangesetManager.fence_change(table_name)) {
    my_printf_error(ER_CHANGESET_COMMAND_ERROR, "changeset not exists", 0);
    return;
  }

  if (m_proc->send_result_metadata(thd)) return;

  my_eof(thd);
}

///////////////////////// polarx.changeset_stats ///////////////////////////////
Proc *Changeset_proc_stats::instance() {
  static Proc *proc = new Changeset_proc_stats(key_memory_package);
  return proc;
}

Sql_cmd *Changeset_proc_stats::evoke_cmd(THD *thd,
                                         mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

void Sql_cmd_changeset_proc_stats::send_result(THD *thd, bool error) {
  Protocol *proto = thd->get_protocol();
  if (error) {
    return;
  }

  // process parameters
  sql_print_information("polarx.changeset_stats()");

  // execute command
  if (m_proc->send_result_metadata(thd)) return;

  std::map<DBTableName, Changeset::Stats> change_counts;
  gChangesetManager.stats(&change_counts);

  for (auto &item : change_counts) {
    auto &full_table_name = item.first;
    auto &stats = item.second;

    proto->start_row();
    proto->store(full_table_name.db_name.data(), &my_charset_utf8mb3_bin);
    proto->store(full_table_name.tb_name.data(), &my_charset_utf8mb3_bin);
    proto->store_longlong(stats.insert_count, false);
    proto->store_longlong(stats.update_count, false);
    proto->store_longlong(stats.delete_count, false);
    proto->store_longlong(stats.file_count, false);
    proto->store_longlong(stats.memory_size, false);
    proto->end_row();
  }

  my_eof(thd);
}

//////////////////////////// polarx.changeset_times
///////////////////////////////////

Proc *Changeset_proc_times::instance() {
  static Proc *proc = new Changeset_proc_times(key_memory_package);
  return proc;
}

Sql_cmd *Changeset_proc_times::evoke_cmd(THD *thd,
                                         mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

void Sql_cmd_changeset_proc_times::send_result(THD *thd, bool error) {
  if (!opt_enable_changeset) {
    my_error(ER_CHANGESET_COMMAND_ERROR, MYF(0),
             "call changeset proc failed, the changeset proc is not support");
    return;
  }
  Protocol *proto = thd->get_protocol();
  if (error) {
    return;
  }

  // process parameters
  // finish_changeset(table_name)
  auto it = m_list->begin();
  Item_string *table_name_item = dynamic_cast<Item_string *>(*(it++));
  if (table_name_item == nullptr) {
    my_error(ER_CHANGESET_COMMAND_ERROR, MYF(0),
             "call changeset times failed, table name is illegal");
    return;
  }

  std::string table_name(table_name_item->val_str(nullptr)->ptr(),
                         table_name_item->val_str(nullptr)->length());

  sql_print_information("changeset.times(%s)", table_name.data());

  Changeset::Stats stats;
  if (gChangesetManager.fetch_times(table_name, stats)) {
    my_printf_error(ER_CHANGESET_COMMAND_ERROR, "call changeset times failed",
                    0);
    return;
  }

  // execute command
  if (m_proc->send_result_metadata(thd)) return;

  proto->start_row();
  proto->store(table_name.data(), &my_charset_utf8mb3_bin);
  proto->store_longlong(stats.file_count + 2, false);
  proto->end_row();

  my_eof(thd);
}

bool Sql_cmd_changeset_proc::check_parameter() {
  // TODO
  return false;
}

bool Sql_cmd_changeset_proc::check_access(THD *) { return false; }

//////////////////////////// polarx.changeset_end
///////////////////////////////////

}  // namespace im
