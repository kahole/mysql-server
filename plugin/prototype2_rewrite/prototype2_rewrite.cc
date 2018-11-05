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
#include <mysql/plugin.h>
#include <mysql/plugin_audit.h>
#include <mysql/psi/mysql_memory.h>
#include <mysql/service_mysql_alloc.h>
#include <mysql/service_parser.h>
#include <string.h>

#include "sql/table.h"

#include "my_inttypes.h"
#include "my_psi_config.h"
#include "my_thread.h"  // my_thread_handle needed by mysql_memory.h

/* instrument the memory allocation */
#ifdef HAVE_PSI_INTERFACE
static PSI_memory_key key_memory_post_parse_example;

static PSI_memory_info all_rewrite_memory[] = {{&key_memory_post_parse_example,
                                                "prototype2_rewrite", 0, 0,
                                                PSI_DOCUMENT_ME}};

static int plugin_init(MYSQL_PLUGIN) {
  const char *category = "sql";
  int count;

  count = static_cast<int>(array_elements(all_rewrite_memory));
  mysql_memory_register(category, all_rewrite_memory, count);
  return 0; /* success */
}
#else
#define plugin_init NULL
#define key_memory_post_parse_example PSI_NOT_INSTRUMENTED
#endif /* HAVE_PSI_INTERFACE */

int catch_literal(MYSQL_ITEM item, unsigned char *arg) {
  MYSQL_LEX_STRING *result_string_ptr = (MYSQL_LEX_STRING *)arg;
  if (result_string_ptr->str == NULL) {
    *result_string_ptr = mysql_parser_item_string(item);
    return 0;
  }
  return 1;
}

int catch_table(TABLE_LIST *tl, unsigned char *arg) {
  
  const char *result_string_ptr = tl->table_name;
  MYSQL_LEX_STRING *result_string_ptr2 = (MYSQL_LEX_STRING *)arg;
  if (result_string_ptr == NULL || result_string_ptr2->str == NULL) {
    //*result_string_ptr = mysql_parser_item_string(item);
    return 0;
  }
  return 1;
}


// // Hack
// int catch_table(MYSQL_ITEM item, unsigned char *arg) {

//   TABLE_LIST *tl = (TABLE_LIST*)arg;
  
//   MYSQL_LEX_STRING *result_string_ptr = (MYSQL_LEX_STRING *)tl;

//   if (item == NULL)
//     return 1;

//   if (result_string_ptr->str == NULL) {
//     //*result_string_ptr = mysql_parser_item_string(item);
//     return 0;
//   }
//   return 1;
// }

static int swap_table(MYSQL_THD thd, mysql_event_class_t event_class,
                      const void *event) {
  if (event_class == MYSQL_AUDIT_PARSE_CLASS) {
    const struct mysql_event_parse *event_parse =
        static_cast<const struct mysql_event_parse *>(event);

    if (event_parse->event_subclass == MYSQL_AUDIT_PARSE_POSTPARSE) {
      // Swap table reference from "Person" to "Planet"
      MYSQL_LEX_STRING first_literal = {NULL, 0};

      mysql_parser_visit_tables(thd, catch_table, (unsigned char *)&first_literal);
      //mysql_parser_visit_tree(thd, catch_literal, (unsigned char *)&first_literal);

      //if (first_literal.str != NULL) {
        size_t query_length = event_parse->query.length;
          //first_literal.length + event_parse->query.length + 23;

       // first_literal.str[first_literal.length] = '\0';

        char *rewritten_query = static_cast<char *>(
            my_malloc(key_memory_post_parse_example, query_length, MYF(0)));
        sprintf(rewritten_query, "%s",
                /*first_literal.str, */event_parse->query.str);
        MYSQL_LEX_STRING new_query = {rewritten_query, query_length};
        //mysql_parser_free_string(first_literal);

        mysql_parser_parse(thd, new_query, false, NULL, NULL);

        *((int *)event_parse->flags) |=
            (int)MYSQL_AUDIT_PARSE_REWRITE_PLUGIN_QUERY_REWRITTEN;
     //}
    }
  }

  return 0;
}

/* Audit plugin descriptor */
static struct st_mysql_audit rewrite_example_descriptor = {
    MYSQL_AUDIT_INTERFACE_VERSION, /* interface version */
    NULL,                          /* release_thd()     */
    swap_table,                    /* event_notify()    */
    {
        0,
        0,
        (unsigned long)MYSQL_AUDIT_PARSE_ALL,
    } /* class mask        */
};

/* Plugin descriptor */
mysql_declare_plugin(audit_log){
    MYSQL_AUDIT_PLUGIN,          /* plugin type                   */
    &rewrite_example_descriptor, /* type specific descriptor      */
    "prototype2_rewrite",        /* plugin name                   */
    "Kristian and Haavard",      /* author                        */
    "An example of a query rewrite"
    " plugin that rewrites all queries", /* description                   */
    PLUGIN_LICENSE_GPL, /* license                       */
    plugin_init,        /* plugin initializer            */
    NULL,               /* plugin check uninstall        */
    NULL,               /* plugin deinitializer          */
    0x0002,             /* version                       */
    NULL,               /* status variables              */
    NULL,               /* system variables              */
    NULL,               /* reserverd                     */
    0                   /* flags                         */
} mysql_declare_plugin_end;
