#include <mysqlx/xdevapi.h>
#include <string.h>
#include <thread>
#include "plugin/lundgren/internal_query/internal_query_session.h"
#include "plugin/lundgren/distributed_query.h"
#include "plugin/lundgren/constants.h"

#ifndef LUNDGREN_DQM
#define LUNDGREN_DQM

std::string get_column_length(unsigned long length);
std::string generate_table_schema(mysqlx::SqlResult *res);
int connect_node(std::string node, Partition_query *pq);
std::string generate_connection_string(Node node);
void execute_distributed_query(Distributed_query* distributed_query);

std::string get_column_length(unsigned long length) {
  return std::to_string(length/4);
}

std::string generate_table_schema(mysqlx::SqlResult *res) {
  std::string return_string = "(";
  for (uint i = 0; i < res->getColumnCount(); i++) {
    return_string += res->getColumn(i).getColumnLabel();
    return_string += " ";
    switch (res->getColumn(i).getType()) {
      case mysqlx::Type::BIGINT : 
        return_string += (res->getColumn(i).isNumberSigned()) ? "BIGINT" : "BIGINT UNSIGNED"; break;
      case mysqlx::Type::INT : 
        return_string += (res->getColumn(i).isNumberSigned()) ? "INT" : "INT UNSIGNED"; break;
      case mysqlx::Type::DECIMAL : 
        return_string += (res->getColumn(i).isNumberSigned()) ? "DECIMAL" : "DECIMAL UNSIGNED"; break;
      case mysqlx::Type::DOUBLE : 
        return_string += (res->getColumn(i).isNumberSigned()) ? "DOUBLE" : "DOUBLE UNSIGNED"; break;
      case mysqlx::Type::STRING : 
        return_string += "VARCHAR(" + get_column_length(res->getColumn(i).getLength()) + ")"; break;
      default: break;
    }
    return_string += ",";
  }
  return_string.pop_back();
  return return_string + ")";
}

int connect_node(std::string node, Partition_query *pq) {
  mysqlx::Session s(node);
  mysqlx::SqlResult res = s.sql(pq->sql_statement).execute();

  if (res.hasData()) {
    std::string table_schema = generate_table_schema(&res);

    if (pq->interim_target.index_name.length() > 0) {
      table_schema.pop_back();
      table_schema += ", INDEX (" + pq->interim_target.index_name + "))";
    }

    std::vector<std::thread> insertion_workers;
    std::vector<mysqlx::Session> interim_sessions;
    std::vector<mysqlx::Schema> interim_schemas;
    std::vector<mysqlx::Table> interim_table_objects;
    std::vector<mysqlx::TableInsert> inserts;

    interim_sessions.emplace_back(mysqlx::Session(generate_connection_string(pq->interim_target.node)));

    std::string create_table_query = "CREATE TABLE IF NOT EXISTS " + pq->interim_target.interim_table_name + " " + table_schema + " " + INTERIM_TABLE_ENGINE ";";
    interim_sessions[0].sql(create_table_query).execute();

    mysqlx::Schema schema = interim_sessions[0].getSchema(pq->node.database);
    mysqlx::Table table = schema.getTable(pq->interim_target.interim_table_name);

    mysqlx::Row row;
    while((row = res.fetchOne())) {

      if (inserts.size() == 0) {
        inserts.emplace_back(table.insert());
      } else {
        //push new session and table insert
        interim_sessions.emplace_back(mysqlx::Session(generate_connection_string(pq->interim_target.node)));
        interim_schemas.emplace_back(interim_sessions[interim_sessions.size()-1].getSchema(pq->node.database));
        interim_table_objects.emplace_back(interim_schemas[interim_schemas.size()-1].getTable(pq->interim_target.interim_table_name));
        inserts.emplace_back(interim_table_objects[interim_table_objects.size()-1].insert());
      }

      inserts[inserts.size()-1].values(row);
      int n = BATCH_SIZE - 1;
      while(n-- && (row = res.fetchOne())){
        inserts[inserts.size()-1].values(row);
      }
      int insert_index = inserts.size()-1;
      insertion_workers.emplace_back(std::thread([&, insert_index]() {
            inserts[insert_index].execute();
          }));
    }

    std::for_each(insertion_workers.begin(), insertion_workers.end(), [](std::thread &t) {
        t.join();
      });

    for (auto &s : interim_sessions) {
      s.close();
    }

  }
  s.close();
  return 0;
}

std::string generate_connection_string(Node node) {
  return (std::string("mysqlx://")
    + node.user + "@"
    + node.host
    + ":"
    + std::to_string(node.port)
    + "/" + node.database); 
}

void execute_distributed_query(Distributed_query* distributed_query) {

  for (auto &stage : distributed_query->stages) {

    std::vector<Partition_query> partition_queries = stage.partition_queries;
    const int num_thd = partition_queries.size();

    std::thread *nodes_connection = new std::thread[num_thd];
    for (int i = 0; i < num_thd; i++) {     
      std::string node = generate_connection_string(partition_queries[i].node);
      nodes_connection[i] = std::thread(connect_node, node, &(partition_queries[i]));
    }

    for (int i = 0; i < num_thd; i++) {
      nodes_connection[i].join();
    }
    delete [] nodes_connection;
  }
}

#endif  // LUNDGREN_DQM
