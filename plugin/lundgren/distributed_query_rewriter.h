#include <mysql/service_parser.h>
#include <string.h>
#include <algorithm>
#include <iostream>
#include "plugin/lundgren/constants.h"
#include "plugin/lundgren/distributed_query.h"
#include "plugin/lundgren/partitions/node.h"
#include "plugin/lundgren/partitions/partition.h"
#include "sql/item.h"
#include "sql/table.h"

#ifndef LUNDGREN_DQR
#define LUNDGREN_DQR

struct L_Item {
  std::string sql;
  Item::Type type;
};


int catch_item(MYSQL_ITEM item, unsigned char *arg) {
  std::vector<L_Item> *fields = (std::vector<L_Item> *)arg;

  if (item != NULL) {
    String s;
    item->print(&s, QT_ORDINARY);

    std::string item_sql = std::string(s.ptr());
    // hack
    item_sql.erase(std::remove(item_sql.begin(), item_sql.end(), '`'),
                   item_sql.end());
    item_sql.erase(std::remove(item_sql.begin(), item_sql.end(), '('),
                   item_sql.end());
    item_sql.erase(std::remove(item_sql.begin(), item_sql.end(), ')'),
                   item_sql.end());

    L_Item fi = {item_sql, item->type()};
    fields->push_back(fi);
  }

  return 0;
}

int catch_table(TABLE_LIST *tl, unsigned char *arg) {
  // const char **result_string_ptr = (const char **)arg;

  std::vector<L_Table> *tables = (std::vector<L_Table> *)arg;

  if (tl != NULL) {
    L_Table t = {std::string(tl->table_name)};
    tables->push_back(t);
    return 0;
  }
  return 1;
}

static void place_projection_in_table(std::string projection,
                                      std::vector<L_Table> *tables,
                                      bool where_transitive_projection) {
  std::string field =
      projection.substr(projection.find(".") + 1, projection.length());
  for (auto &table : *tables) {
    if (table.name == projection.substr(0, projection.find("."))) {
      if (where_transitive_projection) {
        table.where_transitive_projections.push_back(field);
      } else {
        table.projections.push_back(field);
      }
      break;
    }
  }
}



static L_Parser_info *get_tables_from_parse_tree(MYSQL_THD thd) {

  /*
   * Walk parse tree
   */

  std::vector<L_Item> fields = std::vector<L_Item>();
  mysql_parser_visit_tree(thd, catch_item, (unsigned char *)&fields);

  std::vector<L_Table> tables = std::vector<L_Table>();

  mysql_parser_visit_tables(thd, catch_table, (unsigned char *)&tables);

  if (tables.size() == 0) {
    return NULL;
  }


  std::string where_clause = "";
  bool passed_where_clause = false;

  std::vector<L_Item>::iterator f = fields.begin();

  switch (f->type) {
    case Item::FIELD_ITEM:

      while (f != fields.end()) {
        if (f->sql.find("=") != std::string::npos) {
          where_clause += f->sql;
          passed_where_clause = true;

          f++;
          continue;
        }
        if (f->type != Item::FIELD_ITEM) {
          f++;
          continue;
        }

        place_projection_in_table(f->sql, &tables, passed_where_clause);
        f++;
      }
      break;
    case Item::SUM_FUNC_ITEM:

      // THIS IS NOW BROKEN
      // ++f;

      // partition_query_string += "SUM(" + f->sql + ") as " + f->sql +
      //                           "_sum, count(*) as " + f->sql + "_count ";
      // final_query_string +=
      //     "(SUM(" + f->sql + "_sum) / SUM(" + f->sql + "_count)) as average
      //     ";

      break;
    default:
      break;
  }


  L_Parser_info *parser_info = new L_Parser_info();
  parser_info->tables = tables;
  parser_info->where_clause = where_clause;

  return parser_info;
}


#endif  // LUNDGREN_DQR
