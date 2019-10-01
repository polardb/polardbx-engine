var common_stmts = require("common_statements");
var gr_memberships = require("gr_memberships");

var gr_members =
  gr_memberships.members(mysqld.global.gr_members);

var options = {
    innodb_cluster_name: mysqld.global.cluster_name,
    replication_group_members:  gr_members
};

var common_responses = common_stmts.prepare_statement_responses([
  "router_start_transaction",
  "router_commit",
  "router_replication_group_members",
  "router_select_cluster_instances_v2",
  "router_select_cluster_instance_addresses_v2",
  "router_select_view_id_bootstrap_ar",
], options);

var common_responses_regex = common_stmts.prepare_statement_responses_regex([
  "router_insert_into_routers",
  "router_create_user_if_not_exists",
  "router_grant_on_metadata_db",
  "router_grant_on_pfs_db",
  "router_grant_on_routers",
  "router_grant_on_v2_routers",
  "router_update_routers_in_metadata",
], options);

({
  stmts: function (stmt) {
    if (common_responses.hasOwnProperty(stmt)) {
      return common_responses[stmt];
    }
    else if ((res = common_stmts.handle_regex_stmt(stmt, common_responses_regex)) !== undefined) {
      return res;
    }
    else {
      return common_stmts.unknown_statement_response(stmt);
    }
  }
})
