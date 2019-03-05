#include <mysqlx/xdevapi.h>
#include <string.h>
#include <thread>
#include "plugin/lundgren/constants.h"
#include "plugin/lundgren/internal_query/internal_query_session.h"
#include "plugin/lundgren/distributed_query.h"
#include <semaphore.h>

#ifndef LUNDGREN_DQM
#define LUNDGREN_DQM

std::string generate_connection_string(Node node);

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

// int connect_node(std::string node, std::string query,
//                  std::string *result, std::string *table_schema) {
//   mysqlx::Session s(node);
//   mysqlx::SqlResult res = s.sql(query).execute();

//   *result = generate_result_string(&res);
//   *table_schema = generate_table_schema(&res);

//   s.close();

//   return 0;
// }

int connect_node(std::string node, Partition_query *pq,
                 std::string *result, std::string *table_schema, 
                 sem_t *sem, bool *is_table_created) {
  mysqlx::Session s(node);
  mysqlx::SqlResult res = s.sql(pq->sql_statement).execute();

  *result = generate_result_string(&res);
  *table_schema = generate_table_schema(&res);

  s.close();

  for (auto n : pq->interim_target.nodes) {
    mysqlx::Session interim_session(generate_connection_string(n));

    std::string insert_query = "INSERT INTO " + pq->interim_target.interim_table_name + " VALUES " + (*result);
    sem_wait(sem);
    if (*is_table_created == false) {
      std::string create_table_query = "CREATE TABLE " + pq->interim_target.interim_table_name + (*table_schema);
      interim_session.sql(create_table_query).execute();
      *is_table_created = true;
    }
    sem_post(sem);
    interim_session.sql(insert_query).execute();
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

static void execute_distributed_query(Distributed_query* distributed_query) {

  Internal_query_session *session = new Internal_query_session();

  for (auto &stage : distributed_query->stages) {

    std::vector<Partition_query> partition_queries = stage.partition_queries;

    const int num_thd = partition_queries.size();
    // //std::string nodes_metadata[] = {"127.0.0.1:12110", "127.0.0.1:13010"};
    
    std::map<std::string, sem_t> create_interim_table_sem;
    std::map<std::string, bool> interim_table_is_created;
    for (int i = 0; i < num_thd; i++) {
      std::string interim_table = partition_queries[i].interim_target.interim_table_name;
      create_interim_table_sem[interim_table];
      sem_init(&create_interim_table_sem[interim_table], 0, 1);
      interim_table_is_created[interim_table] = false;
    }


    std::thread *nodes_connection = new std::thread[num_thd];
    std::string *results = new std::string[num_thd];
    std::string *table_schema = new std::string[num_thd];

    for (int i = 0; i < num_thd; i++) {     
      std::string node = generate_connection_string(partition_queries[i].node);
      sem_t *sem = &create_interim_table_sem[partition_queries[i].interim_target.interim_table_name];
      bool *is_created = &interim_table_is_created[partition_queries[i].interim_target.interim_table_name];
      nodes_connection[i] = std::thread(connect_node, node, &(partition_queries[i]), &results[i], &table_schema[i], sem, is_created);
    }

    for (int i = 0; i < num_thd; i++) {
      nodes_connection[i].join();
    }
  }
  
  delete session;
  //delete nodes_connection;
  //delete results;
}

#endif  // LUNDGREN_DQM