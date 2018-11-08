/*  Copyright (c) 2015, 2017, Oracle and/or its affiliates. All rights reserved.

    This program is free software; you can redistribute it and/or modify
    it under the terms of the GNU General Public License, version 2.0,
    as published by the Free Software Foundation.

    This program is also distributed with certain software (including
    but not limited to OpenSSL) that is licensed under separate terms,
    as designated in a particular file or component or in included license
    documentation.  The authors of MySQL hereby grant you an additional
    permission to link the program and your derivative works with the
    separately licensed software that they have included with MySQL.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License, version 2.0, for more details.

    You should have received a copy of the GNU General Public License
    along with this program; if not, write to the Free Software
    Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include <ctype.h>
#include <mysql/components/services/log_builtins.h>
#include <mysql/plugin.h>
#include <mysql/plugin_audit.h>
#include <mysql/psi/mysql_memory.h>
#include <mysql/service_mysql_alloc.h>
#include <string.h>

#include <mysql/service_command.h>
#include <mysql/service_parser.h>
#include <mysql/service_srv_session.h>

#include "my_inttypes.h"
#include "my_psi_config.h"
#include "my_thread.h"  // my_thread_handle needed by mysql_memory.h

// include everything
#define LOG_COMPONENT_TAG "test_sql_2_sessions"

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

//

/* instrument the memory allocation */
#ifdef HAVE_PSI_INTERFACE
static PSI_memory_key key_memory_lundgren;

static PSI_memory_info all_rewrite_memory[] = {
    {&key_memory_lundgren, "lundgren", 0, 0, PSI_DOCUMENT_ME}};

static int plugin_init(MYSQL_PLUGIN) {
  const char *category = "sql";
  int count;

  count = static_cast<int>(array_elements(all_rewrite_memory));
  mysql_memory_register(category, all_rewrite_memory, count);
  return 0; /* success */
}
#else
#define plugin_init NULL
#define key_memory_lundgren PSI_NOT_INSTRUMENTED
#endif /* HAVE_PSI_INTERFACE */

//
// Callbacks cbs
//

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

// const struct st_command_service_cbs sql_cbs = {
//     sql_start_result_metadata,
//     sql_field_metadata,
//     sql_end_result_metadata,
//     sql_start_row,
//     sql_end_row,
//     sql_abort_row,
//     sql_get_client_capabilities,
//     sql_get_null,
//     sql_get_integer,
//     sql_get_longlong,
//     sql_get_decimal,
//     sql_get_double,
//     sql_get_date,
//     sql_get_time,
//     sql_get_datetime,
//     sql_get_string,
//     sql_handle_ok,
//     sql_handle_error,
//     sql_shutdown,
// };
//
//
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

// static void handle_error(struct st_plugin_ctx *pctx) {
//   /* handle_ok/error */
// //   if (pctx->sql_errno) {
// //     //WRITE_VAL("error     : %d\n", pctx->sql_errno);
// //     //WRITE_VAL("error msg : %s\n", pctx->err_msg);
// //   } else {
// //     //WRITE_VAL("affected rows : %d\n", pctx->affected_rows);
// //     //WRITE_VAL("server status : %d\n", pctx->server_status);
// //     //WRITE_VAL("warn count    : %d\n", pctx->warn_count);
// //   }
// }

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

static void open_session() {
  DBUG_ENTER("test_sql");
  struct st_plugin_ctx *plugin_ctx = new st_plugin_ctx();

  /* Session declarations */
  MYSQL_SESSION session_1;

  /* Opening session 1 */
  // WRITE_STR("Opening Session 1\n");
  session_1 = srv_session_open(NULL, plugin_ctx);

  if (!session_1) {
  } else {
    switch_user(session_1, user_privileged);
  }

  /* Statement 1 */
  // WRITE_SEP();
  // WRITE_STR("Session 1 : \n");
  // WRITE_SEP();
  exec_test_cmd(session_1,
                "CREATE TABLE lundgren_banan ( id INT, name VARCHAR(25))", NULL,
                plugin_ctx);
  DBUG_VOID_RETURN;
}

int i = 0;

static int rewrite_lower(MYSQL_THD thd, mysql_event_class_t event_class,
                         const void *event) {
  if (event_class == MYSQL_AUDIT_PARSE_CLASS) {
    const struct mysql_event_parse *event_parse =
        static_cast<const struct mysql_event_parse *>(event);
    if (event_parse->event_subclass == MYSQL_AUDIT_PARSE_PREPARSE) {
      // MYSQL_LEX_STRING db_string = {"test", 4};
      // mysql_parser_set_current_database(thd, db_string);

      if (i++ == 0) {
        open_session();
      }

      auto banan = thd;
      thd = banan;

      //

      size_t query_length = event_parse->query.length;
      char *rewritten_query = static_cast<char *>(
          my_malloc(key_memory_lundgren, query_length + 1, MYF(0)));

      for (unsigned i = 0; i < query_length + 1; i++)
        rewritten_query[i] = tolower(event_parse->query.str[i]);

      event_parse->rewritten_query->str = rewritten_query;
      event_parse->rewritten_query->length = query_length;
      *((int *)event_parse->flags) |=
          (int)MYSQL_AUDIT_PARSE_REWRITE_PLUGIN_QUERY_REWRITTEN;
    }
  }

  return 0;
}

/* Audit plugin descriptor */
static struct st_mysql_audit lundgren_descriptor = {
    MYSQL_AUDIT_INTERFACE_VERSION, /* interface version */
    NULL,                          /* release_thd()     */
    rewrite_lower,                 /* event_notify()    */
    {
        0,
        0,
        (unsigned long)MYSQL_AUDIT_PARSE_ALL,
    } /* class mask        */
};

/* Plugin descriptor */
mysql_declare_plugin(audit_log){
    MYSQL_AUDIT_PLUGIN,   /* plugin type                   */
    &lundgren_descriptor, /* type specific descriptor      */
    "lundgren",           /* plugin name                   */
    "Kristian Andersen Hole & Haavard Ola Eggen", /* author */
    "Distributed query plugin", /* description                   */
    PLUGIN_LICENSE_GPL,         /* license                       */
    plugin_init,                /* plugin initializer            */
    NULL,                       /* plugin check uninstall        */
    NULL,                       /* plugin deinitializer          */
    0x0002,                     /* version                       */
    NULL,                       /* status variables              */
    NULL,                       /* system variables              */
    NULL,                       /* reserverd                     */
    0                           /* flags                         */
} mysql_declare_plugin_end;
