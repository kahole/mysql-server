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


static Distributed_query* make_distributed_query(MYSQL_THD thd MY_ATTRIBUTE((unused))) {

    std::vector<Partition> *partitions = get_partitions_by_table_name("Planet");
    std::cout << (*partitions)[0].node.host << "\n";


    const char *first_table_name;
    mysql_parser_visit_tables(thd, catch_table, (unsigned char *)&first_table_name);




    std::vector<Partition_query> *partition_queries = new std::vector<Partition_query>;

    Distributed_query *dq = new Distributed_query();

    dq->partition_queries = partition_queries;

    

    return dq;
}

#endif  // LUNDGREN_DQR