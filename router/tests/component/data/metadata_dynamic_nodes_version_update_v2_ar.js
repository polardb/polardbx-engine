    /**
 * run 1 node on the current host
 *
 * - 1 PRIMARY
 *
 * via HTTP interface
 *
 * - md_query_count
 */

var common_stmts = require("common_statements");
var gr_memberships = require("gr_memberships");

var gr_node_host = "127.0.0.1";

if(mysqld.global.gr_id === undefined){
    mysqld.global.gr_id = "00-000";
}

if(mysqld.global.gr_nodes === undefined){
    mysqld.global.gr_nodes = [];
}

if(mysqld.global.md_query_count === undefined){
    mysqld.global.md_query_count = 0;
}

if(mysqld.global.primary_id === undefined){
    mysqld.global.primary_id = 0;
}

if(mysqld.global.view_id === undefined){
    mysqld.global.view_id = 0;
}

if(mysqld.global.update_version_count === undefined){
    mysqld.global.update_version_count = 0;
}

if(mysqld.global.update_last_check_in_count === undefined){
    mysqld.global.update_last_check_in_count = 0;
}

if(mysqld.global.router_version === undefined){
    mysqld.global.router_version = "";
}

if(mysqld.global.perm_error_on_version_update === undefined){
    mysqld.global.perm_error_on_version_update = 0;
}

if(mysqld.global.upgrade_in_progress === undefined){
    mysqld.global.upgrade_in_progress = 0;
}

if(mysqld.global.first_query === undefined){
  mysqld.global.first_query = "";
}

if(mysqld.global.second_query === undefined){
  mysqld.global.second_query = "";
}

var nodes = function(host, port_and_state) {
  return port_and_state.map(function (current_value) {
    return [ current_value[0], host, current_value[0], current_value[1], current_value[2]];
  });
};

({
  stmts: function (stmt) {
    // let's grab first queries for the verification
    if (mysqld.global.first_query === "") {
      mysqld.global.first_query = stmt;
    }
    else if (mysqld.global.second_query === "") {
      mysqld.global.second_query = stmt;
    }

    var group_replication_membership_online =
      nodes(gr_node_host, mysqld.global.gr_nodes, mysqld.global.gr_id);

    var metadata_version = (mysqld.global.upgrade_in_progress === 1) ? [0, 0, 0] : [2, 0, 0];
    var options = {
      metadata_schema_version: metadata_version,
      group_replication_membership: group_replication_membership_online,
      cluster_id: mysqld.global.gr_id,
      view_id: mysqld.global.view_id,
      primary_port: group_replication_membership_online[mysqld.global.primary_id][2],
      cluster_type: "ar",
      innodb_cluster_name: "test",
      router_version: mysqld.global.router_version,
    };

    // first node is PRIMARY
    options.group_replication_primary_member = options.group_replication_membership[mysqld.global.primary_id][0];

    // prepare the responses for common statements
    var common_responses = common_stmts.prepare_statement_responses([
      "router_start_transaction",
      "router_commit",
      "router_select_schema_version",
      "router_select_cluster_type_v2",
      "router_select_view_id_v2_ar",
      "router_update_last_check_in_v2",
      "select_port"
    ], options);


    var router_select_metadata =
        common_stmts.get("router_select_metadata_v2_ar", options);

    var router_update_version_strict_v2 =
        common_stmts.get("router_update_version_strict_v2", options);

    var router_update_last_check_in_v2 =
    common_stmts.get("router_update_last_check_in_v2", options);

    if (common_responses.hasOwnProperty(stmt)) {
      return common_responses[stmt];
    }
    else if (stmt === router_update_version_strict_v2.stmt) {
      mysqld.global.update_version_count++;
      if (mysqld.global.perm_error_on_version_update === 1) {
        return {
          error: {
            code: 1142,
            sql_state: "HY001",
            message: "UPDATE command denied to user 'user'@'localhost' for table 'v2_routers'"
          }
        }
      } else return router_update_version_strict_v2;
    }
    else if (stmt === router_update_last_check_in_v2.stmt) {
      mysqld.global.update_last_check_in_count++;
      return router_update_last_check_in_v2;
    }
    else if (stmt === router_select_metadata.stmt) {
      mysqld.global.md_query_count++;
      return router_select_metadata;
    }
    else {
      return common_stmts.unknown_statement_response(stmt);
    }
  },
  notices: (function() {
      return mysqld.global.notices;
  })()
})
