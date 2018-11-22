#include <string.h>
#include "plugin/lundgren/partitions/node.h"
#include "plugin/lundgren/partitions/shard_key.h"
#include "plugin/lundgren/internal_query/internal_query_session.h"
#include "plugin/lundgren/internal_query/sql_resultset.h"
#include "plugin/lundgren/constants.h"

#ifndef LUNDGREN_PARTITION
#define LUNDGREN_PARTITION

struct Partition
{
    std::string table_name;
    Node node;
    Shard_key shard_key;
    Partition(char *table_name_in, Node node_in, Shard_key shard_key_in) : node(node_in), shard_key(shard_key_in) {
        table_name = std::string(table_name_in);
    }
};

static std::vector<Partition>* get_partitions_by_table_name(std::string table_name MY_ATTRIBUTE((unused))) {
    Internal_query_session *session = new Internal_query_session();

    session->execute_resultless_query(PLUGIN_FLAG "USE test");

    std::string partition_query = PLUGIN_FLAG
        "SELECT * FROM lundgren_partition p\n"
        "INNER JOIN lundgren_node n on p.nodeId = n.id\n"
        "INNER JOIN lundgren_shard_key s on p.shardKeyId = s.id\n"
        "WHERE p.table_name = \"" + table_name + "\";";

    Sql_resultset *result = session->execute_query(partition_query.c_str());

    std::vector<Partition> *partitions = new std::vector<Partition>;

    if (result->get_rows() == 0)
        return partitions;

    do {

        Node n = Node(result->getString(5), (uint)result->getLong(6), result->getString(7));
        Shard_key s = Shard_key(result->getString(9), (uint)result->getLong(10), (uint)result->getLong(11));

        Partition p = Partition(result->getString(3), n, s);

        partitions->push_back(p);
    } while (result->next());

    return partitions;
}

#endif  // LUNDGREN_PARTITION

/*
CREATE TABLE lundgren_node (
  id INT UNSIGNED PRIMARY KEY,
  host_l VARCHAR(80) NOT NULL,
  port_l INT UNSIGNED,
  database_l VARCHAR(80) NOT NULL,
  username_l VARCHAR(50),
  password_l VARCHAR(50)
);

CREATE TABLE lundgren_shard_key (
  id INT UNSIGNED PRIMARY KEY,
  column_name VARCHAR(80) NOT NULL,
  range_start INT UNSIGNED,
  range_end INT UNSIGNED
);

CREATE TABLE lundgren_partition (
  id INT UNSIGNED PRIMARY KEY,
  nodeId INT UNSIGNED,
  shardKeyId INT UNSIGNED,
  table_name VARCHAR(80) NOT NULL,
  FOREIGN KEY (nodeId) REFERENCES lundgren_node(id),
  FOREIGN KEY (shardKeyId) REFERENCES lundgren_shard_key(id)
);

INSERT INTO lundgren_node VALUES (1, "127.0.0.1", 13000, "test", "root", NULL);
INSERT INTO lundgren_shard_key VALUES (1, "height", 0, 165);
INSERT INTO lundgren_partition VALUES (1, 1, 1, "Person");

SELECT * FROM lundgren_partition p
INNER JOIN lundgren_node n on p.nodeId = n.id
INNER JOIN lundgren_shard_key s on p.shardKeyId = s.id;


*/