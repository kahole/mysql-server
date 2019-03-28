#include <tuple>
#include <mysqlx/xdevapi.h>
#include "plugin/lundgren/partitions/node.h"
#include "plugin/lundgren/distributed_query_manager.h"
#include "plugin/lundgren/join_strategies/bloom_join/bloom_filter.h"
#include "plugin/lundgren/join_strategies/bloom_join/bloom_filter_parameters.h"
#include "plugin/lundgren/join_strategies/bloom_join/filter_coding.h"

#ifndef LUNDGREN_BLOOM_EXECUTOR
#define LUNDGREN_BLOOM_EXECUTOR

std::tuple<std::string, uint64> generate_bloom_filter_from_query(std::string query);

std::tuple<std::string, uint64> generate_bloom_filter_from_query(std::string query) {


    std::string con_string = generate_connection_string(SelfNode::getNode());

    mysqlx::Session s(con_string);
    mysqlx::SqlResult res = s.sql(query).execute();

    const mysqlx::Columns *columns = &res.getColumns();
    uint64 row_count = res.count();

    //Instantiate Bloom Filter
    bloom_filter filter(get_bloom_parameters(row_count));

    // Insert into Bloom Filter
    {
        mysqlx::Row row;
        while ((row = res.fetchOne())) {

            switch ((*columns)[0].getType()) {
            case mysqlx::Type::INT : 
                filter.insert(int(row[0]));
                break;
            case mysqlx::Type::DECIMAL :
                filter.insert(double(row[0]));
                break;
            case mysqlx::Type::DOUBLE : 
                filter.insert(double(row[0]));
                break;
            case mysqlx::Type::STRING :
                filter.insert(std::string(row[0]));
                break;
            default:
            break;
            }
        }
    }
    s.close();

    std::vector<unsigned char> bit_table_ = filter.bit_table_;

    std::string ress = encode_bit_table(bit_table_);

    return {ress, row_count};
}

#endif  // LUNDGREN_BLOOM_EXECUTOR
