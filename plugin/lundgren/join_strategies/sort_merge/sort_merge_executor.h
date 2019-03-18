#include <mysqlx/xdevapi.h>
#include "plugin/lundgren/partitions/node.h"
#include "plugin/lundgren/distributed_query_manager.h"


#ifndef LUNDGREN_SORT_MERGE_EXECUTOR
#define LUNDGREN_SORT_MERGE_EXECUTOR


mysqlx::SqlResult execute_sm_query(std::string query, Node node) {

    std::string con_string = generate_connection_string(node);

    mysqlx::Session s(con_string);
    mysqlx::SqlResult res = s.sql(query).execute();

    s.close();

    return res;
}



#endif  // LUNDGREN_SORT_MERGE_EXECUTOR