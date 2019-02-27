#include <string.h>
#include "plugin/lundgren/distributed_query.h"
#include "plugin/lundgren/helpers.h"
#include "plugin/lundgren/partitions/partition.h"
#include "plugin/lundgren/join_strategies/common.h"
#include "plugin/lundgren/constants.h"

#ifndef LUNDGREN_SEMI_JOIN
#define LUNDGREN_SEMI_JOIN

// Semi join

// n = 1
static Distributed_query *make_one_sided_semi_join_distributed_query(L_Parser_info *parser_info, L_Table* stationary_table, L_Table* remote_table, std::vector<Partition>* remote_partitions) {

  std::vector<L_Table> tables = parser_info->tables;
  std::string where_clause = parser_info->where_clause;


  std::vector<Stage> stages;

  stationary_table->interim_name = generate_interim_name();
  remote_table->interim_name = generate_interim_name();

  // STAGE 1
  Stage stage1;

  std::string stationary_join_column = stationary_table->where_transitive_projections[0];
  std::string remote_join_column = remote_table->where_transitive_projections[0];

  std::string join_column_projection_query_string = "SELECT " + stationary_join_column + " FROM " + stationary_table->name;

  std::vector<Node> target_nodes;

  for (auto &p : *remote_partitions) {
    target_nodes.push_back(p.node);
  }
  
  Interim_target interim_target = {stationary_table->interim_name, target_nodes};
  Partition_query pq = {join_column_projection_query_string, Node(true), interim_target};

  stage1.partition_queries.push_back(pq);
  stages.push_back(stage1);

  // STAGE 2
  Stage stage2;

  std::string semi_join_query_string = "SELECT ";

  std::vector<std::string>::iterator p = remote_table->projections.begin();

  while (p != remote_table->projections.end()) {
    semi_join_query_string += remote_table->name + "." + *p;
    ++p;
    if (p != remote_table->projections.end()) semi_join_query_string += ", ";
  }
  if (remote_table->where_transitive_projections.size() > 0)
    semi_join_query_string += ", ";
  p = remote_table->where_transitive_projections.begin();
  while (p != remote_table->where_transitive_projections.end()) {
    semi_join_query_string += remote_table->name + "." + *p;
    ++p;
    if (p != remote_table->where_transitive_projections.end())
      semi_join_query_string += ", ";
  }

  semi_join_query_string += "FROM " + remote_table->name +
                            " JOIN " + stationary_table->interim_name +
                            " ON " + stationary_table->interim_name + "." + stationary_join_column +
                            " = " + remote_table->name + "." + remote_join_column;

  std::vector<Node> stage2_target_nodes{Node(true)}; //vector of self-node
  Interim_target stage2_interim_target = {remote_table->interim_name, stage2_target_nodes};

  for (auto &p : *remote_partitions) {
    Partition_query pq = {semi_join_query_string, p.node, stage2_interim_target};
    stage2.partition_queries.push_back(pq);
  }

  stages.push_back(stage2);

  // Construct distributed query object
  Distributed_query *dq = new Distributed_query{generate_final_join_query_string(tables, where_clause), stages};

  delete remote_partitions;

  return dq;

  // TODO: remember delete remote_partitions
}

// n > 1
static Distributed_query *make_recursive_semi_join_distributed_query(L_Parser_info *parser_info MY_ATTRIBUTE((unused)), L_Table* remote_table, std::vector<Partition> *remote_partitions) {

  std::vector<Stage> stages;

  Stage stage1;

  std::string join_union_interim_table_name = generate_interim_name();

  //Distributed Partition queries

  //  - sets ignore flag for the "remote_table"
  //  - send query to every node in remote_partitions
  //  - this avoids the "multiple" query problem
  // TODO: brukt constants.h !!
  std::string recursive_distributed_join_query_string = "/*distributed<join_strategy=semi,ignore_table_partitions=" + remote_table->name + ">*/";

  recursive_distributed_join_query_string += generate_join_query_string(parser_info->tables, parser_info->where_clause, false);

  std::vector<Node> target_nodes;
  target_nodes.push_back(Node(true));
  Interim_target interim_target = {join_union_interim_table_name , target_nodes};

  for (auto &p : *remote_partitions) {
    Partition_query pq = {recursive_distributed_join_query_string, p.node, interim_target};
    stage1.partition_queries.push_back(pq);
  }
  
  stages.push_back(stage1);

  // Final query

  std::string final_query_string = "SELECT * FROM " + join_union_interim_table_name;

  // Construct distributed query object

  Distributed_query *dq = new Distributed_query{final_query_string, stages};

  delete remote_partitions;

  return dq;
  // remember delete remote_partitions
}


static Distributed_query *make_semi_join_distributed_query(L_Parser_info *parser_info) {

  // -----------------------------------------------------
  std::vector<L_Table> tables = parser_info->tables;
  std::string where_clause = parser_info->where_clause;
  // ------------------------------------------------------

  std::vector<Stage> stages;

  L_Table* stationary_table;
  L_Table* remote_table;
  std::vector<Partition>* remote_partitions;
  bool has_stationary_table = false;
  unsigned int biggest_partition_count = 0;

  for (auto &table : tables) {

    std::vector<Partition> *partitions =
        get_partitions_by_table_name(table.name);

    if (partitions == NULL) {
      return NULL;
    }

    // Choose table with one partition, or ignore flag
    if (!has_stationary_table && (partitions->size() == 1 /* || ignore_table_partitions(table)*/)) {
      has_stationary_table = true;
      stationary_table = &table;
      delete partitions;
    }
    else {
      if (partitions->size() > biggest_partition_count || biggest_partition_count == 0) {
        // Choose the most partitioned table, and its partitions
        remote_partitions = partitions;
        remote_table = &table;
        biggest_partition_count = partitions->size();
      }
    }
  }

  /* static Distributed_query *make_one_sided_semi_join_distributed_query(L_Parser_info *parser_info, L_Table* stationary_table, L_Table* remote_table, std::vector<Partition>* remote_partitions) { */
  if (has_stationary_table) {
    // n=1
    return make_one_sided_semi_join_distributed_query(parser_info, stationary_table, remote_table, remote_partitions);
    
  }
  else {
    // n=2
    return make_recursive_semi_join_distributed_query(parser_info, remote_table, remote_partitions);
  }
}

#endif  // LUNDGREN_SEMI_JOIN
