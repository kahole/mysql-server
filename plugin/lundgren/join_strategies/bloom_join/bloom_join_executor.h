
#include "plugin/lundgren/distributed_query_manager.h"
#include "plugin/lundgren/join_strategies/bloom_join/bloom_filter.h"
#include "include/base64.h"

#ifndef LUNDGREN_BLOOM_EXECUTOR
#define LUNDGREN_BLOOM_EXECUTOR

std::string encode_bit_table(std::vector<unsigned char> bit_table);
std::vector<unsigned char> decode_bit_table(std::string base64);


std::string generate_bloom_filter_from_query(std::string query) {


    std::string con_string = generate_connection_string(Node(true));

    con_string.length();

    mysqlx::Session s(con_string);
    mysqlx::SqlResult res = s.sql(query).execute();

    uint64 row_count = res.count();


    bloom_parameters parameters;
    // How many elements roughly do we expect to insert?
    parameters.projected_element_count = row_count;
    // Maximum tolerable false positive probability? (0,1)
    parameters.false_positive_probability = 0.0001; // 1 in 10000
    // Simple randomizer (optional)
    parameters.random_seed = 0xA5A5A5A5;

    if (!parameters) {
        std::cout << "Error - Invalid set of bloom filter parameters!" << std::endl;
        return "";
    }

    parameters.compute_optimal_parameters();

    //Instantiate Bloom Filter
    bloom_filter filter(parameters);

    // Insert into Bloom Filter
    {
        mysqlx::Row row;
        while ((row = res.fetchOne())) {
            // TODO: trenger en spesifikk type? eller går det fint?
            filter.insert(row[0]);
        }
    }
    s.close();

    std::vector<unsigned char> bit_table_ = filter.bit_table_;

    std::string ress = encode_bit_table(bit_table_);

    // Reverse

    bit_table_ = decode_bit_table(ress);

    return ress;
}


std::string encode_bit_table(std::vector<unsigned char> bit_table) {
    unsigned char* bit_array = bit_table.data();

    uint64 size_of_bit_array = sizeof(unsigned char) * bit_table.size();
    uint64 needed_length = base64_needed_encoded_length(size_of_bit_array);

    char *base64_dst = new char[needed_length];

    base64_encode(bit_array, size_of_bit_array, base64_dst);

    std::string res(base64_dst, needed_length);

    delete base64_dst;
    return res;
}

std::vector<unsigned char> decode_bit_table(std::string base64) {

    uint64 needed_length = base64_needed_decoded_length(base64.length());

    unsigned char* bit_array = new unsigned char[needed_length];

    base64_decode(base64.c_str(), base64.length(), bit_array, NULL, MY_BASE64_DECODE_ALLOW_MULTIPLE_CHUNKS);

    std::vector<unsigned char> bit_table(bit_array, bit_array + needed_length);

    delete bit_array;
    return bit_table;
}


#endif  // LUNDGREN_BLOOM_EXECUTOR