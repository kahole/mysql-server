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

//#include <mysql/service_command.h>
//#include <mysql/service_parser.h>
//#include <mysql/service_srv_session.h>

#include "my_inttypes.h"
#include "my_psi_config.h"
#include "my_thread.h"  // my_thread_handle needed by mysql_memory.h

#include <iostream>

#include "plugin/lundgren/distributed_query_manager.h"
#include "plugin/lundgren/distributed_query_rewriter.h"
#include "plugin/lundgren/distributed_query.h"
#include "plugin/lundgren/query_acceptance.h"
#include "plugin/lundgren/join_strategies/data_to_query.h"

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

static int lundgren_start(MYSQL_THD thd, mysql_event_class_t event_class,
                          const void *event) {
  if (event_class == MYSQL_AUDIT_PARSE_CLASS) {
    const struct mysql_event_parse *event_parse =
        static_cast<const struct mysql_event_parse *>(event);
    if (event_parse->event_subclass == MYSQL_AUDIT_PARSE_POSTPARSE) {


      if (!accept_query(thd, event_parse->query.str)) {
        return 0;
      }

      bool is_join = detect_join(event_parse->query.str);

      L_Parser_info *parser_info = get_tables_from_parse_tree(thd);

      Distributed_query* distributed_query;
      
      if (is_join) {
        parser_info->tables.pop_back(); //hack

        // TODO: strategy switch
        switch(1) {
        case 2:
          break;
        default:
          distributed_query = make_data_to_query_distributed_query(parser_info, true);
          break;
        }
        
        
      } else {
        distributed_query = make_data_to_query_distributed_query(parser_info, false);
      }


      if (distributed_query == NULL) {
          return 0;
      }

      std::cout << distributed_query->rewritten_query << std::endl;

      execute_distributed_query(distributed_query);

      size_t query_length = distributed_query->rewritten_query.length();

      char *rewritten_query = static_cast<char *>(
          my_malloc(key_memory_lundgren, query_length, MYF(0)));
      sprintf(rewritten_query, "%s", distributed_query->rewritten_query.c_str());
      MYSQL_LEX_STRING new_query = {rewritten_query, query_length};

      mysql_parser_parse(thd, new_query, false, NULL, NULL);

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
    lundgren_start,                /* event_notify()    */
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
