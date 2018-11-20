#include <string.h>
#include "plugin/lundgren/partitions/node.h"
#include "plugin/lundgren/partitions/shard_key.h"
#include "plugin/lundgren/internal_query/internal_query_session.h"
#include "plugin/lundgren/internal_query/sql_resultset.h"

#ifndef LUNDGREN_PARTITION
#define LUNDGREN_PARTITION

struct Partition
{
    Node node;
    std::string table_name;
    Shard_key shard_key;
};

static Partition* get_partitions() {
    Internal_query_session *session = new Internal_query_session();

    Sql_resultset *result = session->execute_query(
        "SELECT * FROM lundgren_partition p"
        "INNER JOIN lundgren_node n on p.nodeId = n.id"
        "INNER JOIN lundgren_shard_key s on p.shardKeyId = s.id;"
    );

    if (result->get_rows() == 0)
        return NULL;

    std::vector<Partition> *partitions = new std::vector<Partition>;
    do {

        Partition p = {.table_name = std::string(result->getString(3))};

        partitions->push_back(p);
        //result->getLong(1);
    } while (result->next());

    return partitions->data();
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