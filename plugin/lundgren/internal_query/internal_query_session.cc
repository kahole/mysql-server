#include "plugin/lundgren/internal_query/internal_query_session.h"

#include <fcntl.h>
#include <mysql/plugin.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>

#include <mysql/components/my_service.h>
//#include <mysql/components/services/log_builtins.h>
#include <mysqld_error.h>

#include "m_string.h"
#include "my_dbug.h"
#include "my_inttypes.h"
#include "my_io.h"
#include "my_sys.h"  // my_write, my_malloc
#include "mysql_com.h"
#include "sql_string.h" /* STRING_PSI_MEMORY_KEY */

#include "plugin/lundgren/internal_query/sql_resultset.h"
#include "plugin/lundgren/internal_query/sql_service_context_base.h"
#include "plugin/lundgren/internal_query/sql_service_context.h"

static void sql_handle_ok(void *ctx MY_ATTRIBUTE((unused)),
                          uint server_status MY_ATTRIBUTE((unused)),
                          uint statement_warn_count MY_ATTRIBUTE((unused)),
                          ulonglong affected_rows MY_ATTRIBUTE((unused)),
                          ulonglong last_insert_id MY_ATTRIBUTE((unused)),
                          const char *const message MY_ATTRIBUTE((unused))) {

  DBUG_ENTER("sql_handle_ok");
  DBUG_VOID_RETURN;
}

static void sql_handle_error(
    void *ctx MY_ATTRIBUTE((unused)), uint sql_errno MY_ATTRIBUTE((unused)),
    const char *const err_msg MY_ATTRIBUTE((unused)),
    const char *const sqlstate MY_ATTRIBUTE((unused))) {

  DBUG_ENTER("sql_handle_error");
  DBUG_VOID_RETURN;
}

static void sql_shutdown(void *, int) {
  DBUG_ENTER("sql_shutdown");
  DBUG_VOID_RETURN;
}

const struct st_command_service_cbs sql_cbs = {
    NULL,         NULL, NULL, NULL, NULL,          NULL,
    NULL,         NULL, NULL, NULL, NULL,          NULL,
    NULL,         NULL, NULL, NULL, sql_handle_ok, sql_handle_error,
    sql_shutdown,
};

struct st_send_field_n {
  char db_name[256];
  char table_name[256];
  char org_table_name[256];
  char col_name[256];
  char org_col_name[256];
  unsigned long length;
  unsigned int charsetnr;
  unsigned int flags;
  unsigned int decimals;
  enum_field_types type;
};

struct st_decimal_n {
  int intg, frac, len;
  bool sign;
  decimal_digit_t buf[256];
};

struct st_plugin_ctx {
  const CHARSET_INFO *resultcs;
  uint meta_server_status;
  uint meta_warn_count;
  uint current_col;
  uint num_cols;
  uint num_rows;
  st_send_field_n sql_field[64];
  char sql_str_value[64][64][256];
  size_t sql_str_len[64][64];
  longlong sql_int_value[64][64];
  longlong sql_longlong_value[64][64];
  uint sql_is_unsigned[64][64];
  st_decimal_n sql_decimal_value[64][64];
  double sql_double_value[64][64];
  uint32 sql_double_decimals[64][64];
  MYSQL_TIME sql_date_value[64][64];
  MYSQL_TIME sql_time_value[64][64];
  uint sql_time_decimals[64][64];
  MYSQL_TIME sql_datetime_value[64][64];
  uint sql_datetime_decimals[64][64];

  uint server_status;
  uint warn_count;
  uint affected_rows;
  uint last_insert_id;
  char message[1024];

  uint sql_errno;
  char err_msg[1024];
  char sqlstate[6];
  st_plugin_ctx() {}
};

// static const char *user_localhost = "localhost";
// static const char *user_local = "127.0.0.1";
// static const char *user_db = "test";
// static const char *user_privileged = "root";
// // static const char *user_ordinary= "ordinary";

// static void switch_user(MYSQL_SESSION session, const char *user) {
//   MYSQL_SECURITY_CONTEXT sc;
//   thd_get_security_context(srv_session_info_get_thd(session), &sc);
//   security_context_lookup(sc, user, user_localhost, user_local, user_db);
// }

Internal_query_session::Internal_query_session() {

  plugin_ctx = new st_plugin_ctx();
  session = srv_session_open(NULL, plugin_ctx);
  // if (!session) {
  //} else {
  //  switch_user(session, user_privileged);
  //}
}

int Internal_query_session::execute_resultless_query(const char *query) {
  
  COM_DATA cmd;
  cmd.com_query.query = (char*)query;
  cmd.com_query.length = strlen(cmd.com_query.query);

  int fail = command_service_run_command(session, COM_QUERY, &cmd,
                                         &my_charset_utf8_general_ci, &sql_cbs,
                                         CS_TEXT_REPRESENTATION, plugin_ctx);
  return fail;
}

Sql_resultset * Internal_query_session::execute_query(const char *query) {

  COM_DATA cmd;
  cmd.com_query.query = (char*)query;
  cmd.com_query.length = strlen(cmd.com_query.query);

  Sql_resultset *rset = new Sql_resultset();
  Sql_service_context_base *ctx = new Sql_service_context(rset);


  /*int fail = */command_service_run_command(session, COM_QUERY, &cmd,
                                         &my_charset_utf8_general_ci, &Sql_service_context_base::sql_service_callbacks,
                                         CS_TEXT_REPRESENTATION, ctx);
  return rset;
}

Internal_query_session::~Internal_query_session() {
  delete plugin_ctx;
  // session close deletes session also
  srv_session_close(session);
}