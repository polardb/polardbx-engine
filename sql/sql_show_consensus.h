/* Copyright (c) 2018, 2021, Alibaba and/or its affiliates. All rights reserved.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.
   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL/PolarDB-X Engine hereby grant you an
   additional permission to link the program and your derivative works with the
   separately licensed software that they have included with
   MySQL/PolarDB-X Engine.
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.
   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#ifndef SQL_SHOW_CONSENSUS_INCLUDE
#define SQL_SHOW_CONSENSUS_INCLUDE

extern ST_FIELD_INFO consensus_commit_pos_info[];
extern ST_FIELD_INFO alisql_cluster_global_fields_info[];
extern ST_FIELD_INFO alisql_cluster_local_fields_info[];
extern ST_FIELD_INFO alisql_cluster_health_fields_info[];
extern ST_FIELD_INFO alisql_cluster_learner_source_fields_info[];
extern ST_FIELD_INFO alisql_cluster_prefetch_channel_info[];
extern ST_FIELD_INFO alisql_cluster_consensus_status_fields_info[];
extern ST_FIELD_INFO alisql_cluster_consensus_membership_change_fields_info[];

extern int fill_consensus_commit_pos(THD *thd, Table_ref *tables, Item *);
extern int fill_alisql_cluster_global(THD *thd, Table_ref *tables, Item *);
extern int fill_alisql_cluster_local(THD *thd, Table_ref *tables, Item *);
extern int fill_alisql_cluster_health(THD *thd, Table_ref *tables, Item *);
extern int fill_alisql_cluster_learner_source(THD *thd, Table_ref *tables,
                                              Item *);
extern int fill_alisql_cluster_prefetch_channel(THD *thd, Table_ref *tables,
                                                Item *);
extern int fill_alisql_cluster_consensus_status(THD *thd, Table_ref *tables,
                                                Item *);
extern int fill_alisql_cluster_consensus_membership_change(THD *thd,
                                                           Table_ref *tables,
                                                           Item *);

#endif
