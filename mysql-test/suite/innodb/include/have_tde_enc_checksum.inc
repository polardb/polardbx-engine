disable_query_log;

if (`SELECT @@have_dynamic_loading != 'YES'`) {
  --skip tde_enc_checksum requires dynamic loading
}

if (!$KEYRING_PLUGIN) {
  --skip tde_enc_checksum requires keyring plugin support
}

if (`SELECT COUNT(*) = 0 FROM performance_schema.global_status WHERE VARIABLE_NAME = 'enc_checksum_active' AND VARIABLE_VALUE = 'ON'`) {
  --skip enc_checksum plugin not active
}

enable_query_log;
