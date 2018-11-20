#include <string.h>

#ifndef LUNDGREN_NODE
#define LUNDGREN_NODE

struct Node
{
    std::string host;
    int port;
    std::string database;
};

#endif  // LUNDGREN_NODE