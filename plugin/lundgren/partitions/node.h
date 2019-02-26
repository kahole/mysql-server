#include <string.h>

#ifndef LUNDGREN_NODE
#define LUNDGREN_NODE

struct Node
{
    std::string host;
    uint port;
    std::string database;
    std::string user;
    bool is_self = false;

    Node(bool is_self_in) : is_self(is_self_in) {}

    Node(char * host_in, uint port_in, char * database_in, char * user_in) : port(port_in) {
        host = std::string(host_in);
        database = std::string(database_in);
        user = std::string(user_in);
    }
};

#endif  // LUNDGREN_NODE