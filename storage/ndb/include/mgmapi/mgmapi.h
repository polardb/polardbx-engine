/*
   Copyright (c) 2003, 2010, Oracle and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

#ifndef MGMAPI_H
#define MGMAPI_H

#include "mgmapi_config_parameters.h"
#include "ndb_logevent.h"
#include "mgmapi_error.h"

#define MGM_LOGLEVELS CFG_MAX_LOGLEVEL - CFG_MIN_LOGLEVEL + 1
#define NDB_MGM_MAX_LOGLEVEL 15

/**
 * @section MySQL Cluster Management API
 *
 * The MySQL Cluster Management API (MGM API) is a C language API
 * that is used for:
 * - Starting and stopping database nodes (ndbd processes)
 * - Starting and stopping Cluster backups
 * - Controlling the NDB Cluster log
 * - Performing other administrative tasks
 *
 * @section  secMgmApiGeneral General Concepts
 *
 * Each MGM API function needs a management server handle
 * of type @ref NdbMgmHandle.
 * This handle is created by calling the function 
 * function ndb_mgm_create_handle() and freed by calling 
 * ndb_mgm_destroy_handle().
 *
 * A function can return any of the following:
 *  -# An integer value, with
 *     a value of <b>-1</b> indicating an error.
 *  -# A non-constant pointer value.  A <var>NULL</var> value indicates an error;
 *     otherwise, the return value must be freed
 *     by the programmer
 *  -# A constant pointer value, with a <var>NULL</var> value indicating an error.
 *     The returned value should <em>not</em> be freed.
 *
 * Error conditions can be identified by using the appropriate
 * error-reporting functions ndb_mgm_get_latest_error() and 
 * @ref ndb_mgm_error.
 *
 * Here is an example using the MGM API (without error handling for brevity's sake).
 * @code
 *   NdbMgmHandle handle= ndb_mgm_create_handle();
 *   ndb_mgm_connect(handle,0,0,0);
 *   struct ndb_mgm_cluster_state *state= ndb_mgm_get_status(handle);
 *   for(int i=0; i < state->no_of_nodes; i++) 
 *   {
 *     struct ndb_mgm_node_state *node_state= &state->node_states[i];
 *     printf("node with ID=%d ", node_state->node_id);
 *     if(node_state->version != 0)
 *       printf("connected\n");
 *     else
 *       printf("not connected\n");
 *   }
 *   free((void*)state);
 *   ndb_mgm_destroy_handle(&handle);
 * @endcode
 *
 * @section secLogEvents  Log Events
 *
 * The database nodes and management server(s) regularly and on specific
 * occations report on various log events that occurs in the cluster. These
 * log events are written to the cluster log.  Optionally a mgmapi client
 * may listen to these events by using the method ndb_mgm_listen_event().
 * Each log event belongs to a category, @ref ndb_mgm_event_category, and
 * has a severity, @ref ndb_mgm_event_severity, associated with it.  Each
 * log event also has a level (0-15) associated with it.
 *
 * Which log events that come out is controlled with ndb_mgm_listen_event(),
 * ndb_mgm_set_clusterlog_loglevel(), and 
 * ndb_mgm_set_clusterlog_severity_filter().
 *
 * Below is an example of how to listen to events related to backup.
 *
 * @code
 *   int filter[] = { 15, NDB_MGM_EVENT_CATEGORY_BACKUP, 0 };
 *   int fd = ndb_mgm_listen_event(handle, filter);
 * @endcode
 *
 *
 * @section secSLogEvents  Structured Log Events
 *
 * The following steps are involved:
 * - Create a NdbEventLogHandle using ndb_mgm_create_logevent_handle()
 * - Wait and store log events using ndb_logevent_get_next()
 * - The log event data is available in the struct ndb_logevent. The
 *   data which is specific to a particular event is stored in a union
 *   between structs so use ndb_logevent::type to decide which struct
 *   is valid.
 *
 * Sample code for listening to Backup related events.  The availaable log
 * events are listed in @ref ndb_logevent.h
 *
 * @code
 *   int filter[] = { 15, NDB_MGM_EVENT_CATEGORY_BACKUP, 0 };
 *   NdbEventLogHandle le_handle= ndb_mgm_create_logevent_handle(handle, filter);
 *   struct ndb_logevent le;
 *   int r= ndb_logevent_get_next(le_handle,&le,0);
 *   if (r < 0) error
 *   else if (r == 0) no event
 *
 *   switch (le.type)
 *   {
 *   case NDB_LE_BackupStarted:
 *     ... le.BackupStarted.starting_node;
 *     ... le.BackupStarted.backup_id;
 *     break;
 *   case NDB_LE_BackupFailedToStart:
 *     ... le.BackupFailedToStart.error;
 *     break;
 *   case NDB_LE_BackupCompleted:
 *     ... le.BackupCompleted.stop_gci;
 *     break;
 *   case NDB_LE_BackupAborted:
 *     ... le.BackupStarted.backup_id;
 *     break;
 *   default:
 *     break;
 *   }
 * @endcode
 */

/*
 * @page ndb_logevent.h ndb_logevent.h
 * @include ndb_logevent.h
 */

/** @addtogroup MGM_C_API
 *  @{
 */

#include <stdio.h>
#include <ndb_types.h>
#include "ndb_logevent.h"
#include "mgmapi_config_parameters.h"

#ifdef __cplusplus
extern "C" {
#endif

  /**
   * The NdbMgmHandle.
   */
  typedef struct ndb_mgm_handle * NdbMgmHandle;

  /**
   *   NDB Cluster node types
   */
  enum ndb_mgm_node_type {
    NDB_MGM_NODE_TYPE_UNKNOWN = -1  /** Node type not known*/
    ,NDB_MGM_NODE_TYPE_API    /** An application (NdbApi) node */
#ifndef DOXYGEN_SHOULD_SKIP_INTERNAL
    = NODE_TYPE_API
#endif
    ,NDB_MGM_NODE_TYPE_NDB    /** A database node */
#ifndef DOXYGEN_SHOULD_SKIP_INTERNAL
    = NODE_TYPE_DB
#endif
    ,NDB_MGM_NODE_TYPE_MGM    /** A management server node */
#ifndef DOXYGEN_SHOULD_SKIP_INTERNAL
    = NODE_TYPE_MGM
#endif
#ifndef DOXYGEN_SHOULD_SKIP_INTERNAL
    ,NDB_MGM_NODE_TYPE_MIN     = 0          /** Min valid value*/
    ,NDB_MGM_NODE_TYPE_MAX     = 3          /** Max valid value*/
#endif
  };

  /**
   *   Database node status
   */
  enum ndb_mgm_node_status {
    /** Node status not known*/
    NDB_MGM_NODE_STATUS_UNKNOWN       = 0,
    /** No contact with node*/
    NDB_MGM_NODE_STATUS_NO_CONTACT    = 1,
    /** Has not run starting protocol*/
    NDB_MGM_NODE_STATUS_NOT_STARTED   = 2,
    /** Is running starting protocol*/
    NDB_MGM_NODE_STATUS_STARTING      = 3,
    /** Running*/
    NDB_MGM_NODE_STATUS_STARTED       = 4,
    /** Is shutting down*/
    NDB_MGM_NODE_STATUS_SHUTTING_DOWN = 5,
    /** Is restarting*/
    NDB_MGM_NODE_STATUS_RESTARTING    = 6,
    /** Maintenance mode*/
    NDB_MGM_NODE_STATUS_SINGLEUSER    = 7,
    /** Resume mode*/
    NDB_MGM_NODE_STATUS_RESUME        = 8,
    /** Node is connected */
    NDB_MGM_NODE_STATUS_CONNECTED     = 9,
#ifndef DOXYGEN_SHOULD_SKIP_INTERNAL
    /** Min valid value*/
    NDB_MGM_NODE_STATUS_MIN           = 0,
    /** Max valid value*/
    NDB_MGM_NODE_STATUS_MAX           = 9
#endif
  };

  /**
   *   Status of a node in the cluster.
   *
   *   Sub-structure in enum ndb_mgm_cluster_state
   *   returned by ndb_mgm_get_status().
   *
   *   @note <var>node_status</var>, <var>start_phase</var>,
   *         <var>dynamic_id</var> 
   *         and <var>node_group</var> are relevant only for database nodes,
   *         i.e. <var>node_type</var> == @ref NDB_MGM_NODE_TYPE_NDB.
   */
  struct NDB_EXPORT ndb_mgm_node_state {
    /** NDB Cluster node ID*/
    int node_id;
    /** Type of NDB Cluster node*/
    enum ndb_mgm_node_type   node_type;
   /** State of node*/
    enum ndb_mgm_node_status node_status;
    /** Start phase.
     *
     *  @note Start phase is only valid if the <var>node_type</var> is
     *        NDB_MGM_NODE_TYPE_NDB and the <var>node_status</var> is 
     *        NDB_MGM_NODE_STATUS_STARTING
     */
    int start_phase;
    /** ID for heartbeats and master take-over (only valid for DB nodes)
     */
    int dynamic_id;
    /** Node group of node (only valid for DB nodes)*/
    int node_group;
    /** Internal version number*/
    int version;
    /** Number of times node has connected or disconnected to the 
     *  management server
     */
    int connect_count;
    /** IP address of node when it connected to the management server.
     *  @note This value will be empty if the management server has restarted
     *        since the node last connected.
     */
    char connect_address[
#ifndef DOXYGEN_SHOULD_SKIP_INTERNAL
			 sizeof("000.000.000.000")+1
#endif
    ];

    /** MySQL version number */
    int mysql_version;
  };
  
  /**
   *   State of all nodes in the cluster; returned from 
   *   ndb_mgm_get_status()
   */
  struct NDB_EXPORT ndb_mgm_cluster_state {
    /** Number of entries in the node_states array */
    int no_of_nodes;
    /** An array with node_states*/
    struct ndb_mgm_node_state node_states[
#ifndef DOXYGEN_SHOULD_SKIP_INTERNAL
					  1
#endif
    ];
  };

  /**
   *   Default reply from the server (reserved for future use)
   */
  struct NDB_EXPORT ndb_mgm_reply {
    /** 0 if successful, otherwise error code. */
    int return_code;
    /** Error or reply message.*/
    char message[256];
  };

#ifndef DOXYGEN_SHOULD_SKIP_INTERNAL
  /**
   *   Default information types
   */
  enum ndb_mgm_info {
    /** ?*/
    NDB_MGM_INFO_CLUSTER,
    /** Cluster log*/
    NDB_MGM_INFO_CLUSTERLOG
  };

  /**
   *   Signal log modes
   *   (Used only in the development of NDB Cluster.)
   */
  enum ndb_mgm_signal_log_mode {
    /** Log receiving signals */
    NDB_MGM_SIGNAL_LOG_MODE_IN,
    /** Log sending signals*/
    NDB_MGM_SIGNAL_LOG_MODE_OUT,
    /** Log both sending/receiving*/
    NDB_MGM_SIGNAL_LOG_MODE_INOUT,
    /** Log off*/
    NDB_MGM_SIGNAL_LOG_MODE_OFF
  };
#endif

  struct NDB_EXPORT ndb_mgm_severity {
    enum ndb_mgm_event_severity category;
    unsigned int value;
  };

  struct NDB_EXPORT ndb_mgm_loglevel {
    enum ndb_mgm_event_category category;
    unsigned int value;
  };


  /***************************************************************************/
  /**
   * @name Functions: Error Handling
   * @{
   */

  /**
   *  Get the most recent error associated with the management server whose handle 
   *  is used as the value of <var>handle</var>.
   *
   * @param   handle        Management handle
   * @return                Latest error code
   */
  NDB_EXPORT int ndb_mgm_get_latest_error(const NdbMgmHandle handle);

  /**
   * Get the most recent general error message associated with a handle
   *
   * @param   handle        Management handle.
   * @return                Latest error message
   */
  NDB_EXPORT const char * ndb_mgm_get_latest_error_msg(const NdbMgmHandle handle);

  /**
   * Get the most recent error description associated with a handle
   *
   * The error description gives some additional information regarding
   * the error message.
   *
   * @param   handle        Management handle.
   * @return                Latest error description
   */
  NDB_EXPORT const char * ndb_mgm_get_latest_error_desc(const NdbMgmHandle handle);

#ifndef DOXYGEN_SHOULD_SKIP_DEPRECATED
  /**
   * Get the most recent internal source code error line associated with a handle
   *
   * @param   handle        Management handle.
   * @return                Latest internal source code line of latest error
   * @deprecated
   */
  NDB_EXPORT int ndb_mgm_get_latest_error_line(const NdbMgmHandle handle);
#endif

  /**
   * Set error stream
   */
  NDB_EXPORT void ndb_mgm_set_error_stream(NdbMgmHandle, FILE *);


  /** @} *********************************************************************/
  /**
   * @name Functions: Create/Destroy Management Server Handles
   * @{
   */

  /**
   * Create a handle to a management server.
   *
   * @return                 A management handle<br>
   *                         or <var>NULL</var> if no management handle could be created.
   */
  NDB_EXPORT NdbMgmHandle ndb_mgm_create_handle();

  /**
   * Destroy a management server handle.
   *
   * @param   handle        Management handle
   */
  NDB_EXPORT void ndb_mgm_destroy_handle(NdbMgmHandle * handle);

  /**
   * Set a name of the handle.  Name is reported in cluster log.
   *
   * @param   handle        Management handle
   * @param   name          Name
   */
  NDB_EXPORT void ndb_mgm_set_name(NdbMgmHandle handle, const char *name);

  /**
   * Set 'ignore_sigpipe' behaviour
   *
   * The mgmapi will by default install a signal handler
   * that ignores all SIGPIPE signals that might occur when
   * writing to an already closed or reset socket. An application
   * that wish to use its own handler for SIGPIPE should call this
   * function after 'ndb_mgm_create_handle' and before
   * 'ndb_mgm_connect'(where the signal handler is installed)
   *
   * @param   handle        Management handle
   * @param   val           Value
   *                        0 - Don't ignore SIGPIPE
   *                        1 - Ignore SIGPIPE(default)
   */
  NDB_EXPORT int ndb_mgm_set_ignore_sigpipe(NdbMgmHandle handle, int val);

  /** @} *********************************************************************/
  /**
   * @name Functions: Connect/Disconnect Management Server
   * @{
   */

  /**
   * Sets the connectstring for a management server
   *
   * @param   handle         Management handle
   * @param   connect_string Connect string to the management server,
   *
   * @return                -1 on error.
   *
   * @code
   * <connectstring> := [<nodeid-specification>,]<host-specification>[,<host-specification>]
   * <nodeid-specification> := nodeid=<id>
   * <host-specification> := <host>[:<port>]
   * <id> is an integer greater than 0 identifying a node in config.ini
   * <port> is an integer referring to a regular unix port
   * <host> is a string containing a valid network host address
   * @endcode
   */
  NDB_EXPORT int ndb_mgm_set_connectstring(NdbMgmHandle handle,
				const char *connect_string);

  /**
   * Returns the number of management servers in the connect string
   * (as set by ndb_mgm_set_connectstring()). This can be used
   * to help work out how long the maximum amount of time that
   * ndb_mgm_connect can take.
   *
   * @param   handle         Management handle
   *
   * @return                < 0 on error
   */
  NDB_EXPORT int ndb_mgm_number_of_mgmd_in_connect_string(NdbMgmHandle handle);

  NDB_EXPORT int ndb_mgm_set_configuration_nodeid(NdbMgmHandle handle, int nodeid);

  /**
   * Set local bindaddress
   * @param arg - Srting of form "host[:port]"
   * @note must be called before connect
   * @note Error on binding local address will not be reported until connect
   * @return 0 on success
   */
  NDB_EXPORT int ndb_mgm_set_bindaddress(NdbMgmHandle, const char * arg);

  /**
   * Gets the connectstring used for a connection
   *
   * @note This function returns the default connectstring if no call to 
   *       ndb_mgm_set_connectstring() has been performed. Also, the
   *       returned connectstring may be formatted differently.
   *
   * @param   handle         Management handle
   * @param   buf            Buffer to hold result
   * @param   buf_sz         Size of buffer.
   *
   * @return                 connectstring (same as <var>buf</var>)
   */
  NDB_EXPORT const char *ndb_mgm_get_connectstring(NdbMgmHandle handle, char *buf, int buf_sz);

  /**
   * DEPRECATED: use ndb_mgm_set_timeout instead.
   *
   * @param handle  NdbMgmHandle
   * @param seconds number of seconds
   * @return non-zero on success
   */
  NDB_EXPORT int ndb_mgm_set_connect_timeout(NdbMgmHandle handle, unsigned int seconds);

  /**
   * Sets the number of milliseconds for timeout of network operations
   * Default is 60 seconds.
   * Only increments of 1000 ms are supported. No function is gaurenteed
   * to return in a fraction of a second.
   *
   * @param handle  NdbMgmHandle
   * @param timeout_ms number of milliseconds
   * @return zero on success
   */
  NDB_EXPORT int ndb_mgm_set_timeout(NdbMgmHandle handle, unsigned int timeout_ms);

  /**
   * Connects to a management server. Connectstring is set by
   * ndb_mgm_set_connectstring().
   *
   * The timeout value is for connect to each management server.
   * Use ndb_mgm_number_of_mgmd_in_connect_string to work out
   * the approximate maximum amount of time that could be spent in this
   * function.
   *
   * @param   handle        Management handle.
   * @param   no_retries    Number of retries to connect
   *                        (0 means connect once).
   * @param   retry_delay_in_seconds
   *                        How long to wait until retry is performed.
   * @param   verbose       Make printout regarding connect retries.
   *
   * @return                -1 on error.
   */
  NDB_EXPORT int ndb_mgm_connect(NdbMgmHandle handle, int no_retries,
		      int retry_delay_in_seconds, int verbose);
  /**
   * Return true if connected.
   *
   * @param   handle        Management handle
   * @return  0 if not connected, non-zero if connected.
   */
  NDB_EXPORT int ndb_mgm_is_connected(NdbMgmHandle handle);

  /**
   * Disconnects from a management server
   *
   * @param  handle         Management handle.
   * @return                -1 on error.
   */
  NDB_EXPORT int ndb_mgm_disconnect(NdbMgmHandle handle);

  /**
   * Gets connection node ID
   *
   * @param   handle         Management handle
   *
   * @return                 Node ID; 0 indicates that no node ID has been
   *                         specified
   */
  NDB_EXPORT int ndb_mgm_get_configuration_nodeid(NdbMgmHandle handle);

  /**
   * Gets connection port
   *
   * @param   handle         Management handle
   *
   * @return                 port
   */
  NDB_EXPORT int ndb_mgm_get_connected_port(NdbMgmHandle handle);

  /**
   * Gets connection host
   *
   * @param   handle         Management handle
   *
   * @return                 hostname
   */
  NDB_EXPORT const char *ndb_mgm_get_connected_host(NdbMgmHandle handle);

  /**
   * Gets connection bind address
   *
   * @param   handle         Management handle
   *
   * @return                 hostname
   */
  NDB_EXPORT const char *ndb_mgm_get_connected_bind_address(NdbMgmHandle handle);

  /**
   * Get the version of the mgm server we're talking to.
   *
   * @param   handle         Management handle
   * @param   major          Returns the major version number for NDB
   * @param   minor          Returns the minor version number for NDB
   * @param   build          Returns the build version number for NDB
   * @param   len            Specifies the max size of the buffer
   *                         available to return version string in
   * @param   str            Pointer to buffer where to return the
   *                         version string which is in the
   *                         form "mysql-X.X.X ndb-Y.Y.Y-status"
   *
   * @return  0 for error and 1 for success
   */
  NDB_EXPORT int ndb_mgm_get_version(NdbMgmHandle handle,
                          int *major, int *minor, int* build,
                          int len, char* str);

#ifndef DOXYGEN_SHOULD_SKIP_INTERNAL
  /** @} *********************************************************************/
  /**
   * @name Functions: Used to convert between different data formats
   * @{
   */

  /**
   * Converts a string to an <var>ndb_mgm_node_type</var> value
   *
   * @param   type          Node type as string.
   * @return                NDB_MGM_NODE_TYPE_UNKNOWN if invalid string.
   */
  NDB_EXPORT enum ndb_mgm_node_type ndb_mgm_match_node_type(const char * type);

  /**
   * Converts an ndb_mgm_node_type to a string
   *
   * @param   type          Node type.
   * @return                <var>NULL</var> if invalid ID.
   */
  NDB_EXPORT const char * ndb_mgm_get_node_type_string(enum ndb_mgm_node_type type);

  /**
   * Converts an ndb_mgm_node_type to a alias string
   *
   * @param   type          Node type.
   * @return                <var>NULL</var> if the ID is invalid.
   */
  NDB_EXPORT const char * ndb_mgm_get_node_type_alias_string(enum ndb_mgm_node_type type,
						  const char **str);

  /**
   * Converts a string to a <var>ndb_mgm_node_status</var> value
   *
   * @param   status        NDB node status string.
   * @return                NDB_MGM_NODE_STATUS_UNKNOWN if invalid string.
   */
  NDB_EXPORT enum ndb_mgm_node_status ndb_mgm_match_node_status(const char * status);

  /**
   * Converts an ID to a string
   *
   * @param   status        NDB node status.
   * @return                <var>NULL</var> if invalid ID.
   */
  NDB_EXPORT const char * ndb_mgm_get_node_status_string(enum ndb_mgm_node_status status);

  NDB_EXPORT const char * ndb_mgm_get_event_severity_string(enum ndb_mgm_event_severity);
  NDB_EXPORT enum ndb_mgm_event_category ndb_mgm_match_event_category(const char *);
  NDB_EXPORT const char * ndb_mgm_get_event_category_string(enum ndb_mgm_event_category);
#endif

  /** @} *********************************************************************/
  /**
   * @name Functions: Cluster status
   * @{
   */

  /**
   * Gets status of the nodes in an NDB Cluster
   *
   * @note The caller must free the pointer returned by this function.
   *
   * @param   handle        Management handle.
   *
   * @return                Cluster state (or <var>NULL</var> on error).
   */
  NDB_EXPORT struct ndb_mgm_cluster_state * ndb_mgm_get_status(NdbMgmHandle handle);


  /**
   * Gets status of the nodes *of specified types* in an NDB Cluster
   *
   * @note The caller must free the pointer returned by this function.
   * @note Passing a NULL pointer into types make this equivalent to
   *       ndb_mgm_get_status
   *
   * @param   handle        Management handle.
   * @param   types         Pointer to array of interesting node types.
   *                        Array should be terminated 
   *                        by *NDB_MGM_NODE_TYPE_UNKNOWN*.
   *
   * @return                Cluster state (or <var>NULL</var> on error).
   */
  NDB_EXPORT struct ndb_mgm_cluster_state *
  ndb_mgm_get_status2(NdbMgmHandle handle,
                      const enum ndb_mgm_node_type types[]);



  /**
   * Dump state
   *
   * @param handle the NDB management handle.
   * @param nodeId the node id.
   * @param args integer array
   * @param number of args in int array
   * @param reply the reply message.
   * @return 0 if successful or an error code.
   */
  NDB_EXPORT int ndb_mgm_dump_state(NdbMgmHandle handle,
			 int nodeId,
			 const int * args,
			 int num_args,
			 struct ndb_mgm_reply* reply);

  /**
   * Get the current configuration from a node.
   *
   * @param handle the NDB management handle.
   * @param nodeId of the node for which the configuration is requested.
   * @return the current configuration from the requested node.
   */
  NDB_EXPORT struct ndb_mgm_configuration *
  ndb_mgm_get_configuration_from_node(NdbMgmHandle handle,
                                      int nodeid);


  /** @} *********************************************************************/
  /**
   * @name Functions: Start/stop nodes
   * @{
   */

  /**
   * Stops database nodes
   *
   * @param   handle        Management handle.
   * @param   no_of_nodes   Number of database nodes to be stopped<br>
   *                          0: All database nodes in cluster<br>
   *                          n: Stop the <var>n</var> node(s) specified in the
   *                            array node_list
   * @param   node_list     List of node IDs for database nodes to be stopped
   *
   * @return                Number of nodes stopped (-1 on error)
   *
   * @note    This function is equivalent
   *          to calling ndb_mgm_stop2(handle, no_of_nodes, node_list, 0)
   */
  NDB_EXPORT int ndb_mgm_stop(NdbMgmHandle handle, int no_of_nodes,
		   const int * node_list);

  /**
   * Stops database nodes
   *
   * @param   handle        Management handle.
   * @param   no_of_nodes   Number of database nodes to stop<br>
   *                          0: All database nodes in cluster<br>
   *                          n: Stop the <var>n</var> node(s) specified in
   *                            the array node_list
   * @param   node_list     List of node IDs of database nodes to be stopped
   * @param   abort         Don't perform graceful stop,
   *                        but rather stop immediately
   *
   * @return                Number of nodes stopped (-1 on error).
   */
  NDB_EXPORT int ndb_mgm_stop2(NdbMgmHandle handle, int no_of_nodes,
		    const int * node_list, int abort);

  /**
   * Stops cluster nodes
   *
   * @param   handle        Management handle.
   * @param   no_of_nodes   Number of database nodes to stop<br>
   *                         -1: All database and management nodes<br>
   *                          0: All database nodes in cluster<br>
   *                          n: Stop the <var>n</var> node(s) specified in
   *                            the array node_list
   * @param   node_list     List of node IDs of database nodes to be stopped
   * @param   abort         Don't perform graceful stop,
   *                        but rather stop immediately
   * @param   disconnect    Returns true if you need to disconnect to apply
   *                        the stop command (e.g. stopping the mgm server
   *                        that handle is connected to)
   *
   * @return                Number of nodes stopped (-1 on error).
   */
  NDB_EXPORT int ndb_mgm_stop3(NdbMgmHandle handle, int no_of_nodes,
		    const int * node_list, int abort, int *disconnect);

  /**
   * Stops cluster nodes
   *
    * @param   handle        Management handle.
   * @param   no_of_nodes   Number of database nodes to stop<br>
   *                         -1: All database and management nodes<br>
   *                          0: All database nodes in cluster<br>
   *                          n: Stop the <var>n</var> node(s) specified in
   *                            the array node_list
   * @param   node_list     List of node IDs of database nodes to be stopped
   * @param   abort         Don't perform graceful stop,
   *                        but rather stop immediately
   * @param   force         Force stop of nodes even if it means the
   *                        whole cluster will be shutdown
   * @param   disconnect    Returns true if you need to disconnect to apply
   *                        the stop command (e.g. stopping the mgm server
   *                        that handle is connected to)
   *
   * @return                Number of nodes stopped (-1 on error).
   */
  NDB_EXPORT int ndb_mgm_stop4(NdbMgmHandle handle, int no_of_nodes,
		    const int * node_list, int abort, int force,
                    int *disconnect);

  /**
   * Restart database nodes
   *
   * @param   handle        Management handle.
   * @param   no_of_nodes   Number of database nodes to restart<br>
   *                          0: All database nodes in cluster<br>
   *                          n: Restart the <var>n</var> node(s) specified in the
   *                            array node_list
   * @param   node_list     List of node IDs of database nodes to be restarted
   *
   * @return                Number of nodes restarted (-1 on error).
   *
   * @note    This function is equivalent to calling
   *          ndb_mgm_restart2(handle, no_of_nodes, node_list, 0, 0, 0);
   */
  NDB_EXPORT int ndb_mgm_restart(NdbMgmHandle handle, int no_of_nodes,
		      const int * node_list);

  /**
   * Restart database nodes
   *
   * @param   handle        Management handle.
   * @param   no_of_nodes   Number of database nodes to be restarted:<br>
   *                          0: Restart all database nodes in the cluster<br>
   *                          n: Restart the <var>n</var> node(s) specified in the
   *                            array node_list
   * @param   node_list     List of node IDs of database nodes to be restarted
   * @param   initial       Remove filesystem from restarting node(s)
   * @param   nostart       Don't actually start node(s) but leave them
   *                        waiting for start command
   * @param   abort         Don't perform graceful restart,
   *                        but rather restart immediately
   *
   * @return                Number of nodes stopped (-1 on error).
   */
  NDB_EXPORT int ndb_mgm_restart2(NdbMgmHandle handle, int no_of_nodes,
		       const int * node_list, int initial,
		       int nostart, int abort);

  /**
   * Restart nodes
   *
   * @param   handle        Management handle.
   * @param   no_of_nodes   Number of database nodes to be restarted:<br>
   *                          0: Restart all database nodes in the cluster<br>
   *                          n: Restart the <var>n</var> node(s) specified in the
   *                            array node_list
   * @param   node_list     List of node IDs of database nodes to be restarted
   * @param   initial       Remove filesystem from restarting node(s)
   * @param   nostart       Don't actually start node(s) but leave them
   *                        waiting for start command
   * @param   abort         Don't perform graceful restart,
   *                        but rather restart immediately
   * @param   disconnect    Returns true if mgmapi client must disconnect from
   *                        server to apply the requested operation. (e.g.
   *                        restart the management server)
   *
   *
   * @return                Number of nodes stopped (-1 on error).
   */
  NDB_EXPORT int ndb_mgm_restart3(NdbMgmHandle handle, int no_of_nodes,
		       const int * node_list, int initial,
		       int nostart, int abort, int *disconnect);

  /**
   * Restart nodes
   *
   * @param   handle        Management handle.
   * @param   no_of_nodes   Number of database nodes to be restarted:<br>
   *                          0: Restart all database nodes in the cluster<br>
   *                          n: Restart the <var>n</var> node(s) specified
   *                             in the array node_list
   * @param   node_list     List of node IDs of database nodes to be restarted
   * @param   initial       Remove filesystem from restarting node(s)
   * @param   nostart       Don't actually start node(s) but leave them
   *                        waiting for start command
   * @param   abort         Don't perform graceful restart,
   *                        but rather restart immediately
   * @param   force         Force restart of nodes even if it means the
   *                        whole cluster will be restarted
   * @param   disconnect    Returns true if mgmapi client must disconnect from
   *                        server to apply the requested operation. (e.g.
   *                        restart the management server)
   *
   *
   * @return                Number of nodes stopped (-1 on error).
   */
  NDB_EXPORT int ndb_mgm_restart4(NdbMgmHandle handle, int no_of_nodes,
		       const int * node_list, int initial,
		       int nostart, int abort, int force, int *disconnect);

  /**
   * Start database nodes
   *
   * @param   handle        Management handle.
   * @param   no_of_nodes   Number of database nodes to be started<br>
   *                        0: Start all database nodes in the cluster<br>
   *                        n: Start the <var>n</var> node(s) specified in
   *                            the array node_list
   * @param   node_list     List of node IDs of database nodes to be started
   *
   * @return                Number of nodes actually started (-1 on error).
   *
   * @note    The nodes to be started must have been started with nostart(-n)
   *          argument.
   *          This means that the database node binary is started and
   *          waiting for a START management command which will
   *          actually enable the database node
   */
  NDB_EXPORT int ndb_mgm_start(NdbMgmHandle handle,
		    int no_of_nodes,
		    const int * node_list);

  /** @} *********************************************************************/
  /**
   * @name Functions: Controlling Clusterlog output
   * @{
   */

  /**
   * Filter cluster log severities
   *
   * @param   handle        NDB management handle.
   * @param   severity      A cluster log severity to filter.
   * @param   enable        set 1=enable o 0=disable
   * @param   reply         Reply message.
   *
   * @return                -1 on error.
   */
  NDB_EXPORT int ndb_mgm_set_clusterlog_severity_filter(NdbMgmHandle handle,
					     enum ndb_mgm_event_severity severity,
					     int enable,
					     struct ndb_mgm_reply* reply);
  /**
   * Get clusterlog severity filter
   *
   * @param   handle        NDB management handle
   *
   * @param loglevel        A vector of seven (NDB_MGM_EVENT_SEVERITY_ALL)
   *                        elements of struct ndb_mgm_severity,
   *                        where each element contains
   *                        1 if a severity indicator is enabled and 0 if not.
   *                        A severity level is stored at position
   *                        ndb_mgm_clusterlog_level;
   *                        for example the "error" level is stored in position
   *                        [NDB_MGM_EVENT_SEVERITY_ERROR].
   *                        The first element [NDB_MGM_EVENT_SEVERITY_ON] in 
   *                        the vector signals whether the cluster log
   *                        is disabled or enabled.
   * @param severity_size   The size of the vector (NDB_MGM_EVENT_SEVERITY_ALL)
   * @return                Number of returned severities or -1 on error
   */
  NDB_EXPORT int ndb_mgm_get_clusterlog_severity_filter(NdbMgmHandle handle,
					     struct ndb_mgm_severity* severity,
					     unsigned int severity_size);

#ifndef DOXYGEN_SHOULD_SKIP_DEPRECATED
  /**
   * Get clusterlog severity filter
   *
   * @param   handle        NDB management handle
   *
   * @return                A vector of seven elements,
   *                        where each element contains
   *                        1 if a severity indicator is enabled and 0 if not.
   *                        A severity level is stored at position
   *                        ndb_mgm_clusterlog_level;
   *                        for example the "error" level is stored in position
   *                        [NDB_MGM_EVENT_SEVERITY_ERROR].
   *                        The first element [NDB_MGM_EVENT_SEVERITY_ON] in 
   *                        the vector signals
   *                        whether the cluster log
   *                        is disabled or enabled.
   */
  NDB_EXPORT const unsigned int *ndb_mgm_get_clusterlog_severity_filter_old(NdbMgmHandle handle);
#endif

  /**
   * Set log category and levels for the cluster log
   *
   * @param   handle        NDB management handle.
   * @param   nodeId        Node ID.
   * @param   category      Event category.
   * @param   level         Log level (0-15).
   * @param   reply         Reply message.
   * @return                -1 on error.
   */
  NDB_EXPORT int ndb_mgm_set_clusterlog_loglevel(NdbMgmHandle handle,
				      int nodeId,
				      enum ndb_mgm_event_category category,
				      int level,
				      struct ndb_mgm_reply* reply);
  
  /**
   * get log category and levels 
   *
   * @param   handle        NDB management handle.
   * @param loglevel        A vector of twelve (MGM_LOGLEVELS) elements
   *                        of struct ndb_mgm_loglevel, 
   *                        where each element contains
   *                        loglevel of corresponding category
   * @param loglevel_size   The size of the vector (MGM_LOGLEVELS)
   * @return                Number of returned loglevels or -1 on error
   */
  NDB_EXPORT int ndb_mgm_get_clusterlog_loglevel(NdbMgmHandle handle,
				      struct ndb_mgm_loglevel* loglevel,
				      unsigned int loglevel_size);

#ifndef DOXYGEN_SHOULD_SKIP_DEPRECATED
  /**
   * get log category and levels 
   *
   * @param   handle        NDB management handle.
   * @return                A vector of twelve elements,
   *                        where each element contains
   *                        loglevel of corresponding category
   */
  NDB_EXPORT const unsigned int *ndb_mgm_get_clusterlog_loglevel_old(NdbMgmHandle handle);
#endif


  /** @} *********************************************************************/
  /**
   * @name Functions: Listening to log events
   * @{
   */

  /**
   * Listen to log events. They are read from the return file descriptor
   * and the format is textual, and the same as in the cluster log.
   *
   * @param handle NDB management handle.
   * @param filter pairs of { level, ndb_mgm_event_category } that will be
   *               pushed to fd, level=0 ends list.
   *
   * @return fd    filedescriptor to read events from
   */
#ifdef NDB_WIN
  NDB_EXPORT SOCKET ndb_mgm_listen_event(NdbMgmHandle handle, const int filter[]);
#else
  NDB_EXPORT int ndb_mgm_listen_event(NdbMgmHandle handle, const int filter[]);
#endif

#ifndef DOXYGEN_SHOULD_SKIP_INTERNAL
  /**
   * Set log category and levels for the Node
   *
   * @param   handle        NDB management handle.
   * @param   nodeId        Node ID.
   * @param   category      Event category.
   * @param   level         Log level (0-15).
   * @param   reply         Reply message.
   * @return                -1 on error.
   */
  NDB_EXPORT int ndb_mgm_set_loglevel_node(NdbMgmHandle handle,
				int nodeId,
				enum ndb_mgm_event_category category,
				int level,
				struct ndb_mgm_reply* reply);
#endif

  /**
   * The NdbLogEventHandle
   */
  typedef struct ndb_logevent_handle * NdbLogEventHandle;

  /**
   * Listen to log events.
   *
   * @param handle NDB management handle.
   * @param filter pairs of { level, ndb_mgm_event_category } that will be
   *               pushed to fd, level=0 ends list.
   *
   * @return       NdbLogEventHandle
   */
  NDB_EXPORT NdbLogEventHandle ndb_mgm_create_logevent_handle(NdbMgmHandle,
						   const int filter[]);
  NDB_EXPORT void ndb_mgm_destroy_logevent_handle(NdbLogEventHandle*);

  /**
   * Retrieve filedescriptor from NdbLogEventHandle.  May be used in
   * e.g. an application select() statement.
   *
   * @note Do not attemt to read from it, it will corrupt the parsing.
   *
   * @return       filedescriptor, -1 on failure.
   */
#ifdef NDB_WIN
  NDB_EXPORT SOCKET ndb_logevent_get_fd(const NdbLogEventHandle);
#else
  NDB_EXPORT int ndb_logevent_get_fd(const NdbLogEventHandle);
#endif

  /**
   * Attempt to retrieve next log event and will fill in the supplied
   * struct dst
   *
   * @param dst Pointer to struct to fill in event information
   * @param timeout_in_milliseconds Timeout for waiting for event
   *
   * @return     >0 if event exists, 0 no event (timed out), or -1 on error.
   *
   * @note Return value <=0 will leave dst untouched
   */
  NDB_EXPORT int ndb_logevent_get_next(const NdbLogEventHandle,
			    struct ndb_logevent *dst,
			    unsigned timeout_in_milliseconds);

  /**
   * Retrieve laterst error code
   *
   * @return     error code
   */
  NDB_EXPORT int ndb_logevent_get_latest_error(const NdbLogEventHandle);

  /**
   * Retrieve laterst error message
   *
   * @return     error message
   */
  NDB_EXPORT const char *ndb_logevent_get_latest_error_msg(const NdbLogEventHandle);


  /** @} *********************************************************************/
  /**
   * @name Functions: Backup
   * @{
   */

  /**
   * Start backup
   *
   * @param   handle          NDB management handle.
   * @param   wait_completed  0:  Don't wait for confirmation<br>
   *                          1:  Wait for backup to be started<br>
   *                          2:  Wait for backup to be completed
   * @param   backup_id       Backup ID is returned from function.
   * @param   reply           Reply message.
   * @return                  -1 on error.
   * @note                    backup_id will not be returned if
   *                          wait_completed == 0
   */
  NDB_EXPORT int ndb_mgm_start_backup(NdbMgmHandle handle, int wait_completed,
			   unsigned int* backup_id,
			   struct ndb_mgm_reply* reply);

  /**
   * Start backup
   *
   * @param   handle          NDB management handle.
   * @param   wait_completed  0:  Don't wait for confirmation<br>
   *                          1:  Wait for backup to be started<br>
   *                          2:  Wait for backup to be completed
   * @param   backup_id       Backup ID is returned from function.
   * @param   reply           Reply message.
   * @param   input_backupId  run as backupId and set next backup id to input_backupId+1.
   * @return                  -1 on error.
   * @note                    backup_id will not be returned if
   *                          wait_completed == 0
   */
  NDB_EXPORT int ndb_mgm_start_backup2(NdbMgmHandle handle, int wait_completed,
			   unsigned int* backup_id,
			   struct ndb_mgm_reply* reply,
			   unsigned int input_backupId);

  /**
   * Start backup
   *
   * @param   handle          NDB management handle.
   * @param   wait_completed  0:  Don't wait for confirmation<br>
   *                          1:  Wait for backup to be started<br>
   *                          2:  Wait for backup to be completed
   * @param   backup_id       Backup ID is returned from function.
   * @param   reply           Reply message.
   * @param   input_backupId  run as backupId and set next backup id to input_backupId+1.
   * @param   backuppoint     Backup happen at start time(1) or complete time(0).
   * @return                  -1 on error.
   * @note                    backup_id will not be returned if
   *                          wait_completed == 0
   */
  NDB_EXPORT int ndb_mgm_start_backup3(NdbMgmHandle handle, int wait_completed,
			   unsigned int* backup_id,
			   struct ndb_mgm_reply* reply,
			   unsigned int input_backupId,
			   unsigned int backuppoint);

  /**
   * Abort backup
   *
   * @param   handle        NDB management handle.
   * @param   backup_id     Backup ID.
   * @param   reply         Reply message.
   * @return                -1 on error.
   */
  NDB_EXPORT int ndb_mgm_abort_backup(NdbMgmHandle handle, unsigned int backup_id,
			   struct ndb_mgm_reply* reply);


  /** @} *********************************************************************/
  /**
   * @name Functions: Single User Mode
   * @{
   */

  /**
   * Enter Single user mode
   *
   * @param   handle        NDB management handle.
   * @param   nodeId        Node ID of the single user node
   * @param   reply         Reply message.
   * @return                -1 on error.
   */
  NDB_EXPORT int ndb_mgm_enter_single_user(NdbMgmHandle handle, unsigned int nodeId,
				struct ndb_mgm_reply* reply);

  /**
   * Exit Single user mode
   *
   * @param   handle        NDB management handle.
   * @param   reply         Reply message.
   *
   * @return                -1 on error.
   */
  NDB_EXPORT int ndb_mgm_exit_single_user(NdbMgmHandle handle,
			       struct ndb_mgm_reply* reply);

#ifndef DOXYGEN_SHOULD_SKIP_INTERNAL
  /** @} *********************************************************************/
  /**
   * @name Configuration handling
   * @{
   */

  /**
   * Get configuration
   * @param   handle     NDB management handle.
   * @param   version    Version of configuration, 0 means latest
   *                     (Currently this is the only supported value for this parameter)
   *
   * @return configuration
   *
   * @note The caller is responsible for calling ndb_mgm_destroy_configuration()
   */
  NDB_EXPORT struct ndb_mgm_configuration * ndb_mgm_get_configuration(NdbMgmHandle handle,
							   unsigned version);
  NDB_EXPORT void ndb_mgm_destroy_configuration(struct ndb_mgm_configuration *);

  NDB_EXPORT int ndb_mgm_alloc_nodeid(NdbMgmHandle handle,
			   unsigned version, int nodetype, int log_event);

  /**
   * End Session
   *
   * This function tells the mgm server to free all resources associated with
   * this connection. It will also close it.
   *
   * This differs from just disconnecting as we now synchronously clean up,
   * so that a quickly restarting server that needs the same node id can
   * get it when it restarts.
   *
   * @param  handle NDB management handle
   * @return 0 on success
   *
   * @note you still have to destroy the NdbMgmHandle.
   */
  NDB_EXPORT int ndb_mgm_end_session(NdbMgmHandle handle);

  /**
   * ndb_mgm_get_fd
   *
   * get the file descriptor of the handle.
   * On Win32, returns SOCKET.
   * INTERNAL ONLY.
   * USE FOR TESTING. OTHER USES ARE NOT A GOOD IDEA.
   *
   * @param  handle NDB management handle
   * @return handle->socket
   *
   */
#ifdef NDB_WIN
  NDB_EXPORT SOCKET ndb_mgm_get_fd(NdbMgmHandle handle);
#else
  NDB_EXPORT int ndb_mgm_get_fd(NdbMgmHandle handle);
#endif

  /**
   * Get the node id of the mgm server we're connected to
   */
  NDB_EXPORT Uint32 ndb_mgm_get_mgmd_nodeid(NdbMgmHandle handle);

  /**
   * Config iterator
   */
  typedef struct ndb_mgm_configuration_iterator ndb_mgm_configuration_iterator;

  NDB_EXPORT ndb_mgm_configuration_iterator* ndb_mgm_create_configuration_iterator
  (struct ndb_mgm_configuration *, unsigned type_of_section);
  NDB_EXPORT void ndb_mgm_destroy_iterator(ndb_mgm_configuration_iterator*);

  NDB_EXPORT int ndb_mgm_first(ndb_mgm_configuration_iterator*);
  NDB_EXPORT int ndb_mgm_next(ndb_mgm_configuration_iterator*);
  NDB_EXPORT int ndb_mgm_valid(const ndb_mgm_configuration_iterator*);
  NDB_EXPORT int ndb_mgm_find(ndb_mgm_configuration_iterator*,
		   int param, unsigned value);

  NDB_EXPORT int ndb_mgm_get_int_parameter(const ndb_mgm_configuration_iterator*,
				int param, unsigned * value);
  NDB_EXPORT int ndb_mgm_get_int64_parameter(const ndb_mgm_configuration_iterator*,
				  int param, Uint64 * value);
  NDB_EXPORT int ndb_mgm_get_string_parameter(const ndb_mgm_configuration_iterator*,
				   int param, const char  ** value);
  NDB_EXPORT int ndb_mgm_purge_stale_sessions(NdbMgmHandle handle, char **);
  NDB_EXPORT int ndb_mgm_check_connection(NdbMgmHandle handle);

  NDB_EXPORT int ndb_mgm_report_event(NdbMgmHandle handle, Uint32 *data, Uint32 length);

  struct ndb_mgm_param_info
  {
    Uint32 m_id;
    const char * m_name;
  };
  int ndb_mgm_get_db_parameter_info(Uint32 paramId, struct ndb_mgm_param_info * info, 
             size_t * size);
#endif

  NDB_EXPORT int ndb_mgm_create_nodegroup(NdbMgmHandle handle,
                               int * nodes,
                               int * ng,
                               struct ndb_mgm_reply* mgmreply);

  NDB_EXPORT int ndb_mgm_drop_nodegroup(NdbMgmHandle handle,
                             int ng,
                             struct ndb_mgm_reply* mgmreply);

#ifndef DOXYGEN_SHOULD_SKIP_DEPRECATED
  enum ndb_mgm_clusterlog_level {
     NDB_MGM_ILLEGAL_CLUSTERLOG_LEVEL = -1,
     NDB_MGM_CLUSTERLOG_ON    = 0,
     NDB_MGM_CLUSTERLOG_DEBUG = 1,
     NDB_MGM_CLUSTERLOG_INFO = 2,
     NDB_MGM_CLUSTERLOG_WARNING = 3,
     NDB_MGM_CLUSTERLOG_ERROR = 4,
     NDB_MGM_CLUSTERLOG_CRITICAL = 5,
     NDB_MGM_CLUSTERLOG_ALERT = 6,
     NDB_MGM_CLUSTERLOG_ALL = 7
  };
  static inline
  int ndb_mgm_filter_clusterlog(NdbMgmHandle h,
				enum ndb_mgm_clusterlog_level s,
				int e, struct ndb_mgm_reply* r)
  { return ndb_mgm_set_clusterlog_severity_filter(h,(enum ndb_mgm_event_severity)s,
						  e,r); }
  static inline
  const unsigned int * ndb_mgm_get_logfilter(NdbMgmHandle h)
  { return ndb_mgm_get_clusterlog_severity_filter_old(h); }

  static inline
  int ndb_mgm_set_loglevel_clusterlog(NdbMgmHandle h, int n,
				      enum ndb_mgm_event_category c,
				      int l, struct ndb_mgm_reply* r)
  { return ndb_mgm_set_clusterlog_loglevel(h,n,c,l,r); }

  static inline
  const unsigned int * ndb_mgm_get_loglevel_clusterlog(NdbMgmHandle h)
  { return ndb_mgm_get_clusterlog_loglevel_old(h); }

#endif

  /**
   *   Struct containing array of ndb_logevents
   *   of the requested type, describing for example
   *   memoryusage or baclupstatus for the whole cluster,
   *   returned from ndb_mgm_dump_events()
   */
  struct NDB_EXPORT ndb_mgm_events {
    /** Number of entries in the logevents array */
    int no_of_events;
    /** Array of ndb_logevents  */
    struct ndb_logevent events [
#ifndef DOXYGEN_SHOULD_SKIP_INTERNAL
                    1
#endif
    ];
  };

  /**
   * Get an array of ndb_logevent of a specified type, describing
   * for example memoryusage or backupstatus for the whole cluster
   *
   * @note The caller must free the pointer returned by this function.
   *
   * @param   handle        Management handle.
   * @param   type          Which ndb_logevent to request
   * @param   no_of_nodes   Number of database nodes to request info from<br>
   *                          0: All database nodes in cluster<br>
   *                          n: Only the <var>n</var> node(s) specified in<br>
   *                             the array node_list
   * @param   node_list     List of node IDs of database nodes to request<br>
   *                        info from
   *
   * @return                struct with array of ndb_logevent's
   *                        (or <var>NULL</var> on error).
   */
  NDB_EXPORT struct ndb_mgm_events*
  ndb_mgm_dump_events(NdbMgmHandle handle, enum Ndb_logevent_type type,
                      int no_of_nodes, const int * node_list);

#ifdef __cplusplus
}
#endif

/** @} */

#endif

