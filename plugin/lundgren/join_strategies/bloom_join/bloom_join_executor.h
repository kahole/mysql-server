#include <tuple>
#include "plugin/lundgren/distributed_query_manager.h"
#include "plugin/lundgren/join_strategies/bloom_join/bloom_filter.h"
#include "plugin/lundgren/join_strategies/bloom_join/bloom_filter_parameters.h"
#include "plugin/lundgren/join_strategies/bloom_join/filter_coding.h"

#ifndef LUNDGREN_BLOOM_EXECUTOR
#define LUNDGREN_BLOOM_EXECUTOR

std::tuple<std::string, uint64> generate_bloom_filter_from_query(std::string query);

std::tuple<std::string, uint64> generate_bloom_filter_from_query(std::string query) {


    std::string con_string = generate_connection_string(Node(true));

    mysqlx::Session s(con_string);
    mysqlx::SqlResult res = s.sql(query).execute();

    uint64 row_count = res.count();

    //Instantiate Bloom Filter
    bloom_filter filter(get_bloom_parameters(row_count));

    // Insert into Bloom Filter
    {
        mysqlx::Row row;
        while ((row = res.fetchOne())) {
            // TODO: trenger en spesifikk type? eller går det fint?
            filter.insert(int(row[0]));
        }
    }
    s.close();

    std::vector<unsigned char> bit_table_ = filter.bit_table_;

    std::string ress = encode_bit_table(bit_table_);

    return {ress, row_count};
}

#endif  // LUNDGREN_BLOOM_EXECUTOR
