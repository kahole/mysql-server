#include <string.h>

struct node_query
{
    std::string host;
    int port;
    std::string database;
    std::string query_str;
    std::string table_name;
};

/*

META DATA kladd

CREATE TABLE lundgren_node (
  id INT UNSIGNED PRIMARY KEY,
  host VARCHAR(80) NOT NULL,
  port INT UNSIGNED,
  user VARCHAR(50),
  password VARCHAR(50)
);

// database i node eller partition??

CREATE TABLE lundgren_partition (
  id INT UNSIGNED PRIMARY KEY,
  database VARCHAR(80) NOT NULL,
  name VARCHAR(80) NOT NULL
);

*/