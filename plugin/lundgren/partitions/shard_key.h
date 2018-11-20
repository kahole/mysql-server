#include <string.h>

#ifndef LUNDGREN_SHARD_KEY
#define LUNDGREN_SHARD_KEY

struct Shard_key
{
    std::string column_name;
    int range_start;
    int range_end;
};

#endif  // LUNDGREN_SHARD_KEY