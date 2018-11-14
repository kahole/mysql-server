#include <mysqlx/xdevapi.h>
#include <string.h>
#include <thread>
#include "plugin/lundgren/constants.h"
#include "plugin/lundgren/internal_query/internal_query_session.h"

#ifndef LUNDGREN_DQM
#define LUNDGREN_DQM

int connect_node(std::string node, std::string query,
                 std::string **result MY_ATTRIBUTE((unused))) {
  mysqlx::Session s(node);
  mysqlx::SqlResult res = s.sql(query).execute();
  mysqlx::Row row;
  std::string result_string;
  while ((row = res.fetchOne())) {
    result_string += std::string(row[1]) + std::string("\n");
  }
  *result = new std::string(result_string);

    s.close();

  return 0;
}

static void execute_distributed_query_set() {
  const int num_thd = 2;
  std::string nodes_metadata[] = {"127.0.0.1:12110", "127.0.0.1:13010"};
  std::thread nodes_connection[num_thd];

  std::string *results[num_thd] = {NULL, NULL};

  for (int i = 0; i < num_thd; i++) {
    std::string node = std::string("mysqlx://root@") + nodes_metadata[i] +
                       std::string("/test");

    std::string query =
        std::string(PLUGIN_FLAG) + std::string("SELECT * FROM Person");
    nodes_connection[i] = std::thread(connect_node, node, query, &results[i]);
  }

  for (int i = 0; i < num_thd; i++) {
    nodes_connection[i].join();
  }

  Internal_query_session *session = new Internal_query_session();
  session->execute_resultless_query(PLUGIN_FLAG "USE test");
  session->execute_resultless_query(
      PLUGIN_FLAG
      "CREATE TABLE fake_temp_table_person"
      "(id INT UNSIGNED PRIMARY KEY,"
      "name VARCHAR(30) NOT NULL,"
      "height INT UNSIGNED,"
      "mass INT UNSIGNED,"
      "hair_color VARCHAR(20),"
      "gender VARCHAR(20),"
      "homeworld INT UNSIGNED,"
      "FOREIGN KEY (homeworld) REFERENCES Planet(id))");
  session->execute_resultless_query(
      PLUGIN_FLAG
      "INSERT INTO fake_temp_table_person VALUES (1, \"Luke Skywalker\", 172, "
      "77, \"blond\", \"male\", 1)");

  delete session;
}

#endif  // LUNDGREN_DQM