#include <string.h>
#include "plugin/lundgren/distributed_query.h"
#include "plugin/lundgren/distributed_query_manager.h"
#include "plugin/lundgren/constants.h"
#include "plugin/lundgren/helpers.h"
#include "plugin/lundgren/join_strategies/bloom_join/bloom_filter.h"
#include "plugin/lundgren/join_strategies/bloom_join/bloom_filter_parameters.h"
#include "plugin/lundgren/join_strategies/bloom_join/filter_coding.h"

#ifndef LUNDGREN_BLOOM_SLAVE
#define LUNDGREN_BLOOM_SLAVE


std::string generate_filtered_insert_statement(mysqlx::SqlResult *res, bloom_filter filter, std::string filter_column);


bloom_filter parse_bloom_filter(L_parsed_comment_args parsed_args) {

    std::string bloom_filter_base64 = parsed_args.comment_args_lookup_table[BLOOM_FILTER_FLAG];

    std::vector<unsigned char> bit_table = decode_bit_table(bloom_filter_base64);

    uint64 bf_inserted_count = std::stoi(parsed_args.comment_args_lookup_table[BLOOM_FILTER_PARAMETER_COUNT_FLAG]);

    bloom_filter bf(get_bloom_parameters(bf_inserted_count));

    bf.bit_table_ = bit_table;

    return bf;
}


Distributed_query *bloom_slave_execute_strategy(L_Parser_info *parser_info MY_ATTRIBUTE((unused)), L_parsed_comment_args parsed_args) {

    bloom_filter filter = parse_bloom_filter(parsed_args);

    //BLOOM_FILTERED_INTERIM_NAME_FLAG

    std::string filtered_interim_name = parsed_args.comment_args_lookup_table[BLOOM_FILTERED_INTERIM_NAME_FLAG];
    std::string join_column = parsed_args.comment_args_lookup_table[BLOOM_FILTER_REMOTE_JOIN_COLUMN_FLAG];

    std::string remote_table_name = parsed_args.comment_args_lookup_table[BLOOM_FILTER_REMOTE_TABLE_FLAG];

    std::string master_node_id = parsed_args.comment_args_lookup_table[BLOOM_FILTER_MASTER_ID_FLAG];

    std::string query_for_filtering = "SELECT ";
    
    L_Table remote_table = parser_info->tables[0];
    std::vector<std::string>::iterator p = remote_table.projections.begin();

    while (p != remote_table.projections.end()) {
      query_for_filtering += remote_table_name + "." + *p;
      ++p;
      if (p != remote_table.projections.end()) query_for_filtering += ", ";
    }
    
    query_for_filtering += " FROM " + remote_table_name;

    Node master_node = getNodeById(master_node_id);

    std::string con_string = generate_connection_string(master_node);

    mysqlx::Session s(con_string);
    mysqlx::SqlResult res = s.sql(query_for_filtering).execute();

    std::string create_table_statement = "CREATE TABLE IF NOT EXISTS " + filtered_interim_name + " ";

    std::string schema = generate_table_schema(&res);

    schema.pop_back();
    schema += ", INDEX (" + join_column + "))"; // index

    create_table_statement += schema + " " + INTERIM_TABLE_ENGINE ";";

    s.sql(create_table_statement).execute();

    mysqlx::Schema sch = s.getSchema(SelfNode::getNode().database);
    mysqlx::Table tbl = sch.getTable(filtered_interim_name);

    mysqlx::Row row;
    const mysqlx::Columns *columns = &res.getColumns();
    uint num_columns = res.getColumnCount();

    uint filter_column_index = 0;

    for (uint i = 0; i < num_columns; i++) {
      if (std::string((*columns)[i].getColumnLabel()) == join_column) {
          filter_column_index = i;
          break;
      }
    }

    int n = BLOOM_SLAVE_BATCH_SIZE;
    auto insert = tbl.insert();

    while ((row = res.fetchOne())) {
      switch ((*columns)[filter_column_index].getType()) {
        case mysqlx::Type::INT:
          if (!filter.contains(int(row[filter_column_index]))) continue;
          break;
        case mysqlx::Type::DECIMAL:
          if (!filter.contains(double(row[filter_column_index]))) continue;
          break;
        case mysqlx::Type::DOUBLE:
          if (!filter.contains(double(row[filter_column_index]))) continue;
          break;
        case mysqlx::Type::STRING:
          if (!filter.contains(std::string(row[filter_column_index]))) continue;
          break;
        default:
        break;
      }

      insert.values(row);

      if (n == 0) {
        insert.execute();
        insert = tbl.insert();
        n = BLOOM_SLAVE_BATCH_SIZE;
      } else {
        n--;
      }
    }
    if (n != BLOOM_SLAVE_BATCH_SIZE) {
      // final batch
      insert.execute();
    }

    s.close();

    // rwrite to NO-OP
    Distributed_query *dq = new Distributed_query{"DO 0;"};

    return dq;
}


#endif  // LUNDGREN_BLOOM_SLAVE