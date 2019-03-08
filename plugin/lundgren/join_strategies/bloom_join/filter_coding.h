
#include "include/base64.h"

#ifndef LUNDGREN_FILTER_CODING
#define LUNDGREN_FILTER_CODING

std::string encode_bit_table(std::vector<unsigned char> bit_table);
std::vector<unsigned char> decode_bit_table(std::string base64);

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


#endif  // LUNDGREN_FILTER_CODING