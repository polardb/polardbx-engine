
if (`SELECT count(plugin_status)=0 FROM information_schema.plugins WHERE plugin_name LIKE 'polarx_rpc'`)
{
  skip polarx_rpc plugin not available.;
}

if (`SELECT plugin_status!='ACTIVE' FROM information_schema.plugins WHERE plugin_name LIKE 'polarx_rpc'`)
{
  skip polarx_rpc plugin is disabled.;
}

if (`select VARIABLE_VALUE!='ON' from performance_schema.global_variables where VARIABLE_NAME like 'enable_polarx_rpc';`)
{
  skip new rpc option is disabled.;
}
