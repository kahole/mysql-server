#include "plugin/lundgren/internal_query_session.h"

#include <fcntl.h>
#include <mysql/plugin.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>

#include <mysql/components/my_service.h>
#include <mysql/components/services/log_builtins.h>
#include <mysqld_error.h>

#include "m_string.h"
#include "my_dbug.h"
#include "my_inttypes.h"
#include "my_io.h"
#include "my_sys.h"  // my_write, my_malloc
#include "mysql_com.h"
#include "sql_string.h" /* STRING_PSI_MEMORY_KEY */

static void sql_handle_ok(void *ctx MY_ATTRIBUTE((unused)),
                          uint server_status MY_ATTRIBUTE((unused)),
                          uint statement_warn_count MY_ATTRIBUTE((unused)),
                          ulonglong affected_rows MY_ATTRIBUTE((unused)),
                          ulonglong last_insert_id MY_ATTRIBUTE((unused)),
                          const char *const message MY_ATTRIBUTE((unused))) {
  //   struct st_plugin_ctx *pctx = (struct st_plugin_ctx *)ctx;
  DBUG_ENTER("sql_handle_ok");
  /* This could be an EOF */
  //   if (!pctx->num_cols) pctx->num_rows = 0;
  //   pctx->server_status = server_status;
  //   pctx->warn_count = statement_warn_count;
  //   pctx->affected_rows = affected_rows;
  //   pctx->last_insert_id = last_insert_id;
  //   if (message) strncpy(pctx->message, message, sizeof(pctx->message) - 1);
  //   pctx->message[sizeof(pctx->message) - 1] = '\0';

  DBUG_VOID_RETURN;
}

static void sql_handle_error(
    void *ctx MY_ATTRIBUTE((unused)), uint sql_errno MY_ATTRIBUTE((unused)),
    const char *const err_msg MY_ATTRIBUTE((unused)),
    const char *const sqlstate MY_ATTRIBUTE((unused))) {
  //   struct st_plugin_ctx *pctx = (struct st_plugin_ctx *)ctx;
  DBUG_ENTER("sql_handle_error");
  //   pctx->sql_errno = sql_errno;
  //   if (pctx->sql_errno) {
  //     strcpy(pctx->err_msg, (char *)err_msg);
  //     strcpy(pctx->sqlstate, (char *)sqlstate);
  //   }
  //   pctx->num_rows = 0;
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

static void exec_test_cmd(MYSQL_SESSION session, const char *test_cmd,
                          void *p MY_ATTRIBUTE((unused)), void *ctx) {
  // WRITE_VAL("%s\n", test_cmd);
  // struct st_plugin_ctx *pctx = (struct st_plugin_ctx *)ctx;
  COM_DATA cmd;
  //   pctx->reset();
  cmd.com_query.query = (char *)test_cmd;
  cmd.com_query.length = strlen(cmd.com_query.query);

  int fail = command_service_run_command(session, COM_QUERY, &cmd,
                                         &my_charset_utf8_general_ci, &sql_cbs,
                                         CS_TEXT_REPRESENTATION, ctx);
  if (fail) {
    // if (!srv_session_close(session))
    //   LogPluginErrMsg(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
    //                   "test_sql_2_sessions - ret code : %d", fail);
  } else {
    // if (pctx->num_cols) get_data_str(pctx);
    // handle_error(pctx);
  }
}

static const char *user_localhost = "localhost";
static const char *user_local = "127.0.0.1";
static const char *user_db = "test";
static const char *user_privileged = "root";
// static const char *user_ordinary= "ordinary";

static void switch_user(MYSQL_SESSION session, const char *user) {
  MYSQL_SECURITY_CONTEXT sc;
  thd_get_security_context(srv_session_info_get_thd(session), &sc);
  security_context_lookup(sc, user, user_localhost, user_local, user_db);
}

Internal_query_session::Internal_query_session() {

  plugin_ctx = new st_plugin_ctx();

  MYSQL_SESSION session_1 = srv_session_open(NULL, plugin_ctx);

  if (!session_1) {
  } else {
    switch_user(session_1, user_privileged);
  }

  exec_test_cmd(session_1, "USE test;", NULL, plugin_ctx);
  exec_test_cmd(session_1,
                "CREATE TABLE lundgren_banan ( id INT, name VARCHAR(25))", NULL,
                plugin_ctx);
}

void Internal_query_session::execute_resultless_queries() {


}

Internal_query_session::~Internal_query_session() {
    delete plugin_ctx;
    //TODO: close session?
    // delete session??
}