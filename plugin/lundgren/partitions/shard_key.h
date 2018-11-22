#include <string.h>

#ifndef LUNDGREN_SHARD_KEY
#define LUNDGREN_SHARD_KEY

struct Shard_key
{
    std::string column_name;
    uint range_start;
    uint range_end;

    Shard_key(char * column_name_in, uint range_start_in, uint range_end_in) :range_start(range_start_in), range_end(range_end_in) {
        column_name = std::string(column_name_in);
    }
};

#endif  // LUNDGREN_SHARD_KEY