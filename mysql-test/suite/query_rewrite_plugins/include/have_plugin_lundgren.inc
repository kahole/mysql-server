#
# Check if server has support for loading plugins
#
if (`SELECT @@have_dynamic_loading != 'YES'`) {
  --skip Rewrite example plugin requires dynamic loading
}

# Test requires: sp-protocol/ps-protocol/view-protocol/cursor-protocol disabled
--source include/no_protocol.inc
--source include/no_opt_trace_protocol.inc
--source include/no_explain_protocol.inc
--source include/no_json_explain_protocol.inc
#
# Check if the variable LUNDGREN is set
#
if (!$LUNDGREN) {
  --skip Rewrite example plugin requires the environment variable \$LUNDGREN to be set (normally done by mtr)
}

#
# Check if --plugin-dir was setup for exampledb
#
if (`SELECT CONCAT('--plugin-dir=', REPLACE(@@plugin_dir, '\\\\', '/')) != '$LUNDGREN_OPT/'`) {
  --skip Rewrite Example plugin requires that --plugin-dir is set to the example plugin dir (either the .opt file does not contain \$LUNDGREN_OPT or another plugin is in use)
}
enable_query_log;