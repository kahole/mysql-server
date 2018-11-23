#include <mysqlx/xdevapi.h>
#include <string.h>
#include <thread>
#include "plugin/lundgren/constants.h"
#include "plugin/lundgren/internal_query/internal_query_session.h"
#include "plugin/lundgren/distributed_query.h"

#ifndef LUNDGREN_DQM
#define LUNDGREN_DQM

int connect_node(std::string node, std::string query,
                 std::string *result) {
  mysqlx::Session s(node);
  mysqlx::SqlResult res = s.sql(query).execute();
  mysqlx::Row row;
  std::string result_string;
  while ((row = res.fetchOne())) {
    // result_string += std::string(row[1]) + std::string("\n");
    result_string +=
        std::string("(") + std::to_string(int(row[0])) + std::string(",") +
        std::string("\"") + std::string(row[1]) + std::string("\",") +
        std::to_string(int(row[2])) + std::string(",") +
        std::to_string(int(row[3])) + std::string(",") + std::string("\"") +
        std::string(row[4]) + std::string("\",") + std::string("\"") +
        std::string(row[5]) + std::string("\",") + std::to_string(int(row[6])) +
        std::string("),");
  }
  result_string.pop_back();
  *result = result_string;
  s.close();

  return 0;
}

static void execute_distributed_query(Distributed_query* distributed_query) {

  std::vector<Partition_query>* partition_queries = distributed_query->partition_queries;

  const int num_thd = partition_queries->size();
  // //std::string nodes_metadata[] = {"127.0.0.1:12110", "127.0.0.1:13010"};

  std::thread *nodes_connection = new std::thread[num_thd];
  std::string *results = new std::string[num_thd];

  for (int i = 0; i < num_thd; i++) {

    Partition_query pq = (*partition_queries)[i];

    std::string node = std::string("mysqlx://root@")
    + pq.node.host
    + ":"
    + std::to_string(pq.node.port)
    + "/" + pq.node.database;

    std::cout << node << std::endl;

    std::string query = std::string(PLUGIN_FLAG) + (*partition_queries)[i].sql_statement;
    nodes_connection[i] = std::thread(connect_node, node, query, &results[i]);
  }

  for (int i = 0; i < num_thd; i++) {
    nodes_connection[i].join();
  }

  // TODO: each query should be pointed to it's correct interim table
  std::string insert_query =
      PLUGIN_FLAG "INSERT INTO " + (*partition_queries)[0].interim_table_name + " VALUES ";

  for (int i = 0; i < num_thd; i++) {
    insert_query += results[i];
    if (i != num_thd - 1) {
      insert_query += ",";
    }
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
  session->execute_resultless_query(insert_query.c_str());

  delete session;
  delete nodes_connection;
  delete results;
}

#endif  // LUNDGREN_DQM