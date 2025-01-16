
if (`SELECT count(plugin_status)=0 FROM information_schema.plugins WHERE plugin_name LIKE 'POLARX_LICENSE'`)
{
  --skip POLARX_LICENSE plugin not available;
}

