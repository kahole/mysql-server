#include <string.h>
#include "plugin/lundgren/partitions/partition.h"
#include "plugin/lundgren/partitions/node.h"

#ifndef LUNDGREN_DISTRIBUTED_QUERY
#define LUNDGREN_DISTRIBUTED_QUERY

struct L_Table {
  std::string name;
  std::string interim_name;
  std::vector<std::string> projections;
  std::vector<std::string> where_transitive_projections;
  std::vector<std::string> join_columns;
  std::vector<std::string> aliases;
};


struct L_Parser_info {
  std::vector<L_Table> tables;
  std::string where_clause;
};

//------------------------

struct Interim_target {
    std::string interim_table_name;
    Node node;
    std::string index_name;
    //bool is_temp;
};

struct Partition_query {
  std::string sql_statement;
  Node node;
  Interim_target interim_target;
};

struct Stage {
    std::vector<Partition_query> partition_queries;
};

struct Distributed_query {
    std::string rewritten_query;
    std::vector<Stage> stages;
};

#endif  // LUNDGREN_DISTRIBUTED_QUERY
