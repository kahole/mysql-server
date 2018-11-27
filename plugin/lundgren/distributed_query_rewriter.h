#include <mysql/service_parser.h>
#include <string.h>
#include <iostream>
#include "plugin/lundgren/constants.h"
#include "plugin/lundgren/distributed_query.h"
#include "plugin/lundgren/partitions/node.h"
#include "plugin/lundgren/partitions/partition.h"
#include "sql/item.h"
#include "sql/table.h"
#include <algorithm>

#ifndef LUNDGREN_DQR
#define LUNDGREN_DQR

// TODO
// this is a quick fix
std::string random_string( size_t length )
{
    auto randchar = []() -> char
    {
        const char charset[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";
        const size_t max_index = (sizeof(charset) - 1);
        return charset[ rand() % max_index ];
    };
    std::string str(length,0);
    std::generate_n( str.begin(), length, randchar );
    return str;
}

//

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
    //hack
    item_sql.erase(std::remove(item_sql.begin(), item_sql.end(), '`'), item_sql.end());

    L_Item fi = {item_sql, item->type()};
    fields->push_back(fi);
  }

  return 0;
}

int catch_table(TABLE_LIST *tl, unsigned char *arg) {
  const char **result_string_ptr = (const char **)arg;
  if (tl != NULL) {
    *(result_string_ptr) = tl->table_name;
    return 0;
  }
  return 1;
}

// std::string write_partition_queries() {}
// std::string write_final_query() {}

static Distributed_query *make_distributed_query(MYSQL_THD thd) {
  // Walk parse tree

  std::vector<L_Item> fields = std::vector<L_Item>();
  mysql_parser_visit_tree(thd, catch_item, (unsigned char *)&fields);

  // mysql_parser_free_string(first_literal);

  const char *first_table_name = NULL;
  mysql_parser_visit_tables(thd, catch_table,
                            (unsigned char *)&first_table_name);

  if (first_table_name == NULL) {
    return NULL;
  }

  // Resolve partitions

  std::vector<Partition> *partitions =
      get_partitions_by_table_name(first_table_name);

  if (partitions == NULL) {
    return NULL;
  }

  std::cout << (*partitions)[0].node.host << "\n";

  // Write queries

  std::string partition_query_string = "SELECT ";
  std::string final_query_string = "SELECT ";

  std::vector<L_Item>::iterator f = fields.begin();

  switch (f->type) {
    case Item::FIELD_ITEM:
        partition_query_string += f->sql;
        final_query_string += f->sql;
        if (f == fields.end()) {
            break;
        }
        f++;
      while (f != fields.end()) {
        partition_query_string += ", " + f->sql;
        final_query_string += ", " + f->sql;
        f++;
      }
        partition_query_string += " ";
        final_query_string += " ";
      break;
    case Item::SUM_FUNC_ITEM:

      ++f;

      partition_query_string += "SUM(" + f->sql + ") as " + f->sql +
                                "_sum, count(*) as " + f->sql + "_count ";
      final_query_string +=
          "(SUM(" + f->sql + "_sum) / SUM(" + f->sql + "_count)) as average ";

      break;
    default:
      //partition_query_string += "* ";
      //final_query_string += "* ";
      break;
  }

  std::string from_table = "FROM " + std::string(first_table_name);
  std::string interim_table_name = random_string(30);

  partition_query_string += from_table;
  final_query_string += "FROM " + interim_table_name;

  std::vector<Partition_query> *partition_queries =
      new std::vector<Partition_query>;

  for (std::vector<Partition>::iterator p = partitions->begin();
       p != partitions->end(); ++p) {
    Partition_query pq = {partition_query_string, interim_table_name, p->node};
    partition_queries->push_back(pq);
  }

  Distributed_query *dq = new Distributed_query();

  dq->partition_queries = partition_queries;
  dq->rewritten_query = final_query_string;

  return dq;
}

#endif  // LUNDGREN_DQR