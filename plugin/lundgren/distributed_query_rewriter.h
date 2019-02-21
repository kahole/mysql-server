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

struct L_Table {
  std::string name;
  std::string interim_name;
  std::vector<std::string> projections;
  std::vector<std::string> where_transitive_projections;
};

std::string random_string(size_t length);

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

static Distributed_query *make_distributed_query(MYSQL_THD thd) {
  /*
   * Walk parse tree
   */

  std::vector<L_Item> fields = std::vector<L_Item>();
  mysql_parser_visit_tree(thd, catch_item, (unsigned char *)&fields);

  std::vector<L_Table> tables = std::vector<L_Table>();

  mysql_parser_visit_tables(thd, catch_table, (unsigned char *)&tables);

  bool is_join = tables.size() >= 2;

  if (tables.size() == 0) {
    return NULL;
  }

  std::vector<Partition_query> *partition_queries =
      new std::vector<Partition_query>;

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

  /*
   * Generate partition queries
   */

  // hack
  if (is_join) {
    tables.pop_back();
  }

  for (auto &table : tables) {

    std::vector<Partition> *partitions =
        get_partitions_by_table_name(table.name);

    if (partitions == NULL) {
      return NULL;
    }

    // std::string partition_query_string = std::string(partition_query_string);
    std::string partition_query_string = "SELECT ";

    std::vector<std::string>::iterator p = table.projections.begin();

    while (p != table.projections.end()) {
      partition_query_string += table.name + "." + *p;
      ++p;
      if (p != table.projections.end()) partition_query_string += ", ";
    }
    partition_query_string += ", ";
    p = table.where_transitive_projections.begin();
    while (p != table.where_transitive_projections.end()) {
      partition_query_string += table.name + "." + *p;
      ++p;
      if (p != table.where_transitive_projections.end())
        partition_query_string += ", ";
    }

    std::string from_table = " FROM " + std::string(table.name);
    table.interim_name = random_string(30);

    partition_query_string += from_table;

    if (!is_join && where_clause.length() > 0)
      partition_query_string += " WHERE " + where_clause;

    for (std::vector<Partition>::iterator p = partitions->begin();
         p != partitions->end(); ++p) {
      Partition_query pq = {partition_query_string, table.interim_name,
                            p->node};
      partition_queries->push_back(pq);
    }
  }

  /*
   * Generate final rewritten query
   */

  std::string final_query_string = "SELECT ";

  if (is_join) {
    std::string join_on = std::string(where_clause);

    // iterate in reverse, because we get the tables in reverse order from mysql
    for (auto it = tables.rbegin(); it != tables.rend();) {
      L_Table table = *it;

      // make final query join clause by replacing table names with interim
      // names in where_clause Warning! this only replaces the first occurence!
      join_on.replace(join_on.find(table.name), table.name.length(),
                      table.interim_name);

      std::vector<std::string>::iterator p = table.projections.begin();

      // ALIAS? for like kolonnenavn? trengs det?
      while (p != table.projections.end()) {
        // final_query_string  += ", " + table.interim_name + "." + *p + " as "
        // + table.name + "." + *p;
        final_query_string += table.interim_name + "." + *p;
        ++p;
        if (p != table.projections.end()) final_query_string += ", ";
      }

      if (++it != tables.rend()) final_query_string += ", ";
    }

    final_query_string += " FROM " + tables[1].interim_name + " JOIN " +
                          tables[0].interim_name + " ON " + join_on;

  } else {
    L_Table first_table = tables[0];
    std::vector<std::string>::iterator p = first_table.projections.begin();

    // ALIAS trengs ikke her. Tror Person.blabla blir brukt dersom flere
    // kolonner med samme navn, altså kun med flere tabeller final_query_string
    // += first_table.interim_name + "." + *p + " as " + first_table.name + "."
    // + *p;
    while (p != first_table.projections.end()) {
      // final_query_string  += ", " + first_table.interim_name + "." + *p + "
      // as " + first_table.name + "." + *p;
      final_query_string += first_table.interim_name + "." + *p;
      ++p;
      if (p != first_table.projections.end()) final_query_string += ", ";
    }
    final_query_string += " FROM " + first_table.interim_name;
  }

  // Construct distributed query object

  Distributed_query *dq = new Distributed_query();

  dq->partition_queries = partition_queries;
  dq->rewritten_query = final_query_string;

  return dq;
}

// TODO
// this is a quick fix. cite stackoverflow
std::string random_string(size_t length) {
  auto randchar = []() -> char {
    const char charset[] =
        //"0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";
    const size_t max_index = (sizeof(charset) - 1);
    return charset[rand() % max_index];
  };
  std::string str(length, 0);
  std::generate_n(str.begin(), length, randchar);
  return str;
}
//-------------------------------------------------------------

#endif  // LUNDGREN_DQR
