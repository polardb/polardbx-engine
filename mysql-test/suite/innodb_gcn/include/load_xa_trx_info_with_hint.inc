# ==== Purpose ====
# Load xa trx info by dbms_xa.find_by_xid_with_hint(...)

# ==== Usage ====
# --let $my_xid, $slot_hint='1','',1

--disable_ps_protocol
--let $actual_status= query_get_value("call dbms_xa.find_by_xid_with_hint($my_xid, $slot_hint)", STATUS, 1)

--let $actual_gcn= query_get_value("call dbms_xa.find_by_xid_with_hint($my_xid, $slot_hint)", GCN, 1)

--let $actual_csr= query_get_value("call dbms_xa.find_by_xid_with_hint($my_xid, $slot_hint)", CSR, 1)

--let $actual_trx_id= query_get_value("call dbms_xa.find_by_xid_with_hint($my_xid, $slot_hint)", TRX_ID, 1)

--let $actual_uba= query_get_value("call dbms_xa.find_by_xid_with_hint($my_xid, $slot_hint)", UBA, 1)

--let $actual_m_trx_id= query_get_value("call dbms_xa.find_by_xid_with_hint($my_xid, $slot_hint)", MASTER_TRX_ID, 1)

--let $actual_m_uba= query_get_value("call dbms_xa.find_by_xid_with_hint($my_xid, $slot_hint)", MASTER_UBA, 1)

--let $actual_n_branch= query_get_value("call dbms_xa.find_by_xid_with_hint($my_xid, $slot_hint)", N_BRANCH, 1)

--let $actual_n_local_branch= query_get_value("call dbms_xa.find_by_xid_with_hint($my_xid, $slot_hint)", N_LOCAL_BRANCH, 1)
--enable_ps_protocol
