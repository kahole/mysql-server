#include <string.h>
#include "plugin/lundgren/partitions/partition.h"
#include "plugin/lundgren/partitions/node.h"

#ifndef LUNDGREN_DISTRIBUTED_QUERY
#define LUNDGREN_DISTRIBUTED_QUERY

struct Partition_query {
    std::string sql_statement;
    std::string interim_table_name;
    Node node;
};

struct Distributed_query {
    std::string rewritten_query;
    std::vector<Partition_query> *partition_queries;
};

#endif  // LUNDGREN_DISTRIBUTED_QUERY