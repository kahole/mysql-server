#include <mysqlx/xdevapi.h>
#include "plugin/lundgren/distributed_query.h"
#include "plugin/lundgren/join_strategies/common.h"
#include "plugin/lundgren/helpers.h"
#include "plugin/lundgren/join_strategies/sort_merge/k_way_merge_joiner.h"
// #include "plugin/lundgren/join_strategies/sort_merge/sort_merge_executor.h"

#ifndef LUNDGREN_SORT_MERGE
#define LUNDGREN_SORT_MERGE

std::string generate_order_by_query(L_Table* table, std::string join_column);

Distributed_query *execute_sort_merge_distributed_query(L_Parser_info *parser_info) {

    std::string merge_joined_interim_name = generate_interim_name();

    std::vector<L_Table> *tables = &(parser_info->tables);

    L_Table lhs_table = tables->at(0);
    L_Table rhs_table = tables->at(1);

    std::vector<Partition>* lhs_partitions = get_partitions_by_table_name(lhs_table.name);
    std::vector<Partition>* rhs_partitions = get_partitions_by_table_name(rhs_table.name);

    std::string lhs_join_column = lhs_table.where_transitive_projections[0];
    std::string rhs_join_column = rhs_table.where_transitive_projections[0];

    std::string lhs_order_query = generate_order_by_query(&lhs_table, lhs_join_column);
    std::string rhs_order_query = generate_order_by_query(&rhs_table, rhs_join_column);

    // TODO: flytt all eksekvering til executor saken

    std::vector<mysqlx::Session*> sessions;

    std::vector<mysqlx::SqlResult*> lhs_streams;
    mysqlx::SqlResult* lhs_res = new mysqlx::SqlResult[rhs_partitions->size()];
    int z = 0;

    for (auto &p : *lhs_partitions) {

        std::string con_string = generate_connection_string(p.node);
        mysqlx::Session* s = new mysqlx::Session(con_string);

        lhs_res[z] = s->sql(lhs_order_query).execute();

        lhs_streams.push_back(&lhs_res[z]);
        sessions.push_back(s);
        z++;
    }

    std::vector<mysqlx::SqlResult*> rhs_streams;
    mysqlx::SqlResult* res = new mysqlx::SqlResult[rhs_partitions->size()];
    int i = 0;

    for (auto &p : *rhs_partitions) {

        std::string con_string = generate_connection_string(p.node);
        mysqlx::Session* s = new mysqlx::Session(con_string);

        res[i] = s->sql(rhs_order_query).execute();

        rhs_streams.push_back(&res[i]);
        sessions.push_back(s);
        i++;
    }

    //-----------------------------------------------------

    // FIND LHS JOIN COLUMN INDEX
    uint lhs_join_column_index = 0;
    const mysqlx::Columns *lhs_columns = &lhs_streams.at(0)->getColumns();
    uint lhs_num_columns = lhs_streams.at(0)->getColumnCount();

    for (uint i = 0; i < lhs_num_columns; i++) {
        if (std::string((*lhs_columns)[i].getColumnLabel()) == lhs_join_column) {
            lhs_join_column_index = i;
            break;
        }
    }

    // FIND RHS JOIN COLUMN INDEX
    uint rhs_join_column_index = 0;
    const mysqlx::Columns *rhs_columns = &rhs_streams.at(0)->getColumns();
    uint rhs_num_columns = rhs_streams.at(0)->getColumnCount();

    for (uint i = 0; i < rhs_num_columns; i++) {
        if (std::string((*rhs_columns)[i].getColumnLabel()) == rhs_join_column) {
            rhs_join_column_index = i;
            break;
        }
    }

    //-----------------------------------------------------


    K_way_merge_joiner merge_joiner = K_way_merge_joiner(lhs_streams, rhs_streams, lhs_join_column_index, rhs_join_column_index);

    std::vector<mysqlx::Row> lhs_candidates;
    std::vector<mysqlx::Row> rhs_candidates;

    std::tie(lhs_candidates, rhs_candidates) = merge_joiner.fetchNextMatches();


    //--------------------------

    std::string final_query_string = "SELECT * FROM " + merge_joined_interim_name;

    // TEST
    final_query_string = "SELECT * FROM Person";

    Distributed_query *dq = new Distributed_query();
    dq->rewritten_query = final_query_string;
    return dq;
}

std::string generate_order_by_query(L_Table* table, std::string join_column) {

    std::string order_query = "SELECT ";
    order_query += generate_projections_string_for_partition_query(table);
    order_query += " FROM " + table->name;
    order_query += " ORDER BY " + table->name + "." + join_column + " ASC";
    return order_query;
}

#endif  // LUNDGREN_SORT_MERGE