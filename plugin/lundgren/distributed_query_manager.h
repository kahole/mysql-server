#include <mysqlx/xdevapi.h>
#include <string.h>
#include <thread>
#include "plugin/lundgren/constants.h"
#include "plugin/lundgren/internal_query/internal_query_session.h"
#include "plugin/lundgren/distributed_query.h"

#ifndef LUNDGREN_DQM
#define LUNDGREN_DQM

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
          result_string += std::to_string(double(row[i]));; break;
        case mysqlx::Type::DOUBLE : 
          result_string += std::to_string(double(row[i]));; break;
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

int connect_node(std::string node, std::string query,
                 std::string *result, std::string *table_schema) {
  mysqlx::Session s(node);
  mysqlx::SqlResult res = s.sql(query).execute();

  *result = generate_result_string(&res);
  *table_schema = generate_table_schema(&res);

  s.close();

  return 0;
}

std::string generate_connection_string(Partition_query pq) {
  return (std::string("mysqlx://")
    + pq.node.user + "@"
    + pq.node.host
    + ":"
    + std::to_string(pq.node.port)
    + "/" + pq.node.database); 
}

static void execute_distributed_query(Distributed_query* distributed_query) {

  std::vector<Partition_query> partition_queries = (distributed_query->stages)[0].partition_queries;

  const int num_thd = partition_queries.size();
  // //std::string nodes_metadata[] = {"127.0.0.1:12110", "127.0.0.1:13010"};

  std::thread *nodes_connection = new std::thread[num_thd];
  std::string *results = new std::string[num_thd];
  std::string *table_schema = new std::string[num_thd];

  for (int i = 0; i < num_thd; i++) {
    Partition_query pq = partition_queries[i];

    std::string node = generate_connection_string(pq);
    std::string query = partition_queries[i].sql_statement;
    nodes_connection[i] = std::thread(connect_node, node, query, &results[i], &table_schema[i]);
  }

  for (int i = 0; i < num_thd; i++) {
    nodes_connection[i].join();
  }

  /* Generate INSERT strings for every interim table */
  std::map<std::string, std::string> insert_queries;
  for (int i = 0; i < num_thd; i++) {
    insert_queries[partition_queries[i].interim_target.interim_table_name] += results[i] + ',';
  }

  /* Add INSERT INTO interim VALUES in front of the string and removing trailing comma */
  for (auto& query : insert_queries) {
    query.second.pop_back();
    insert_queries[query.first] = "INSERT INTO " + query.first + " VALUES " + query.second;
  }

  /* Generate CREATE TABLE for table schemas for each interim */
  std::map<std::string, std::string> create_table_schemas;
  for (int i = 0; i < num_thd; i++) {
    std::string interim_table = partition_queries[i].interim_target.interim_table_name;
    create_table_schemas[interim_table] = "CREATE TABLE " + interim_table + table_schema[i];
  }

  // TODO:
  // if "node.is_self" use internal query stuff, because we dont have the credentials to do CPP-con for it

  /* Perform internal queries to create and populate interim tables */
  Internal_query_session *session = new Internal_query_session();
  session->execute_resultless_query("USE test");
  for (const auto& create_table : create_table_schemas) {
    session->execute_resultless_query(create_table.second.c_str());;
    session->execute_resultless_query(insert_queries[create_table.first].c_str());
  }

  delete session;
  //delete nodes_connection;
  //delete results;
}

#endif  // LUNDGREN_DQM