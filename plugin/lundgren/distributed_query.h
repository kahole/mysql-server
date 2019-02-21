#include <string.h>
#include "plugin/lundgren/partitions/partition.h"
#include "plugin/lundgren/partitions/node.h"

#ifndef LUNDGREN_DISTRIBUTED_QUERY
#define LUNDGREN_DISTRIBUTED_QUERY

struct Partition_query {
    std::string sql_statement;
    std::string interim_table_name;
    Node node;
    // Er dette godt nok for å dekke alle Plugin-to-MySQL strategiene? Dekker ikke semi-join med to partisjonerte tabeller!!
    // TODO: Bruk prep-statements for å opprette tabeller osv?
    // hva med "insert join column". Da må DQM vite at prep_statementen må fylles med intern-spørring data
    std::vector<std::string> prep_statements;
};

// TODO: bruk stages for å fikse multi-step strategier, slik som semi-join med 2 partisjonerte tabeller
struct Stage {
    std::vector<Partition_query> *partition_queries;
};

struct Distributed_query {
    std::string rewritten_query;
    std::vector<Partition_query> *partition_queries;
    std::vector<Stage> *stages;
};


#endif  // LUNDGREN_DISTRIBUTED_QUERY
