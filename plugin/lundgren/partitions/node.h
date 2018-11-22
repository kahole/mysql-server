#include <string.h>

#ifndef LUNDGREN_NODE
#define LUNDGREN_NODE

struct Node
{
    std::string host;
    uint port;
    std::string database;

    Node(char * host_in, uint port_in, char * database_in) : port(port_in) {
        host = std::string(host_in);
        database = std::string(database_in);
    }
};

#endif  // LUNDGREN_NODE