#include <mysqlx/xdevapi.h>
#include <string.h>
#include <thread>
#include "plugin/lundgren/internal_query/internal_query_session.h"
#include "plugin/lundgren/distributed_query.h"

#ifndef LUNDGREN_DQM
#define LUNDGREN_DQM

std::string get_column_length(unsigned long length);
std::string generate_table_schema(mysqlx::SqlResult *res);
std::string generate_result_string(mysqlx::SqlResult *res);
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

std::string generate_result_string(mysqlx::SqlResult *res) {
  mysqlx::Row row;
  std::string result_string = "";
  const mysqlx::Columns *columns = &res->getColumns();
  uint num_columns = res->getColumnCount();
  while ((row = res->fetchOne())) {
    result_string += "(";
    for (uint i = 0; i < num_columns; i++) {
      switch ((*columns)[i].getType()) {
        case mysqlx::Type::INT : 
          result_string += std::to_string(int(row[i])); break;
        case mysqlx::Type::DECIMAL :
          result_string += std::to_string(double(row[i])); break;
        case mysqlx::Type::DOUBLE : 
          result_string += std::to_string(double(row[i])); break;
        case mysqlx::Type::STRING : 
          result_string += std::string("\"") + std::string(row[i]) + std::string("\"") ; break;
        default: break;
      }
      result_string += ",";
    }
    result_string.pop_back();
    result_string += "),";
  }
  result_string.pop_back();
  return result_string;
}

int connect_node(std::string node, Partition_query *pq) {
  mysqlx::Session s(node);
  mysqlx::SqlResult res = s.sql(pq->sql_statement).execute();

  std::string result = generate_result_string(&res);
  std::string table_schema = generate_table_schema(&res);

  s.close();

  for (auto n : pq->interim_target.nodes) {
    mysqlx::Session interim_session(generate_connection_string(n));

    std::string insert_query = "INSERT INTO " + pq->interim_target.interim_table_name + " VALUES " + result;
    std::string create_table_query = "CREATE TABLE IF NOT EXISTS " + pq->interim_target.interim_table_name + ' ' + table_schema;
    interim_session.sql(create_table_query).execute();
    if (result.length() > 0) {
      interim_session.sql(insert_query).execute();
    }
    interim_session.close();
  }
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