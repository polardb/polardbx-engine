#
# Check if server has support for loading plugins
#
if (`SELECT @@have_dynamic_loading != 'YES'`) {
  --skip GALAXY requires dynamic loading
}

#
# Check if the variable GALAXY_PLUGIN is set
#
if (!$GALAXY_PLUGIN) {
  --skip GALAXY requires the environment variable \$GALAXY_PLUGIN to be set (normally done by mtr)
}

#
# Check if --plugin-dir was setup for udf
#
if (`SELECT CONCAT('--plugin-dir=', REPLACE(@@plugin_dir, '\\\\', '/')) != '$GALAXY_PLUGIN_OPT/'`) {
  --skip UDF requires that --plugin-dir is set to the udf plugin dir (either the .opt file does not contain \$UDF_EXAMPLE_LIB_OPT or another plugin is in use)
}
