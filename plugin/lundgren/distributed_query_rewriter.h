#include <string.h>
#include <mysql/service_parser.h>
#include "plugin/lundgren/constants.h"
#include "plugin/lundgren/partitions/partition.h"
#include "plugin/lundgren/partitions/node.h"
#include "plugin/lundgren/distributed_query.h"
#include "sql/table.h"
#include <iostream>

#ifndef LUNDGREN_DQR
#define LUNDGREN_DQR



int catch_table(TABLE_LIST *tl, unsigned char *arg) {

  const char **result_string_ptr = (const char **)arg;
  if (tl != NULL) {
    *(result_string_ptr) = tl->table_name;
    return 0;
  }
  return 1;
}

/*
SELECT height FROM Person;

Regular select:
SELECT {colums} FROM {table};


Utvid sql_lex.cc

SELECT_LEX::accept(Select_lex_visitor *visitor)

Item er det vi trenger

  for (Item *item = it++; item != end; item = it++)
    if (walk_item(item, visitor)) return true;

Average select:
SELECT AVG(height) FROM Person;


SELECT SUM(mass) FROM Planet;
SELECT height FROM Person INNER JOIN Planet on Person.homeworld = Planet.id;
*/

static Distributed_query* make_distributed_query(MYSQL_THD thd) {

    const char *first_table_name = NULL;
    mysql_parser_visit_tables(thd, catch_table, (unsigned char *)&first_table_name);

    if (first_table_name == NULL) {
        return NULL;
    }

    std::vector<Partition> *partitions = get_partitions_by_table_name(first_table_name);

    if (partitions == NULL) {
        return NULL;
    }

    std::cout << (*partitions)[0].node.host << "\n";

    std::string interim_table_name = "static_interim_table";


    std::vector<Partition_query> *partition_queries = new std::vector<Partition_query>;

    for (std::vector<Partition>::iterator p = partitions->begin(); p != partitions->end(); ++p) {

        Partition_query pq = {"SELECT * FROM " + std::string(first_table_name), interim_table_name, p->node};
        partition_queries->push_back(pq);
    }

    Distributed_query *dq = new Distributed_query();

    dq->partition_queries = partition_queries;
    // TODO: is the plugin flag needed here?
    dq->rewritten_query = PLUGIN_FLAG "SELECT * FROM " + interim_table_name;

    return dq;
}

#endif  // LUNDGREN_DQR