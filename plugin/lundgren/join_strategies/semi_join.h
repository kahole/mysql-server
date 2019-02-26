#include <string.h>
#include "plugin/lundgren/distributed_query.h"
#include "plugin/lundgren/helpers.h"
#include "plugin/lundgren/partitions/partition.h"

#ifndef LUNDGREN_SEMI_JOIN
#define LUNDGREN_SEMI_JOIN

// Semi join

static Distributed_query *make_semi_join_distributed_query(
    L_Parser_info *parser_info) {

  // -----------------------------------------------------
  std::vector<L_Table> tables = parser_info->tables;
  std::string where_clause = parser_info->where_clause;
  // ------------------------------------------------------

  std::vector<Stage> *stages =
      new std::vector<Stage>;

  L_Table* stationary_table;
  L_Table* remote_table;
  std::vector<Partition>* remote_partitions;

  for (auto &table : tables) {

    std::vector<Partition> *partitions =
        get_partitions_by_table_name(table.name);

    if (partitions == NULL) {
      return NULL;
    }

    // TODO: fix! only pick one table with size == 1!!
    // Choose table with one partition, or ignore flag
    if (partitions->size() == 1 /* || ignore_table_partitions(table)*/) {
      stationary_table = &table;
    }
    else {
      remote_partitions = partitions;
      remote_table = &table;
    }
  }

  // -------------------------------------------------

  stationary_table->interim_name = generate_interim_name();
  remote_table->interim_name = generate_interim_name();

  // STAGE 1

  // std::string join_column_interim_name = generate_interim_name();
  std::string stationary_join_column = stationary_table->where_transitive_projections[0];
  std::string remote_join_column = remote_table->where_transitive_projections[0];

  std::string join_column_projection_query_string = "SELECT " + stationary_join_column + " FROM " + stationary_table->name;

  std::vector<Node> target_nodes;

  for (auto &p : *remote_partitions) {
    target_nodes.push_back(p.node);
  }
  
  Interim_target interim_target = {stationary_table->interim_name, target_nodes};

  Partition_query pq = {join_column_projection_query_string, Node(true), interim_target};

  std::vector<Partition_query> *stage1_queries =
      new std::vector<Partition_query>;

  stage1_queries->push_back(pq);

  Stage stage1 = { stage1_queries };  
  stages->push_back(stage1);

  // STAGE 2

  // std::string collect_remote_table_interim_name = generate_interim_name();

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

  std::vector<Partition_query> *stage2_queries =
      new std::vector<Partition_query>;

  for (auto &p : *remote_partitions) {
    Partition_query pq = {semi_join_query_string, p.node, stage2_interim_target};
    stage2_queries->push_back(pq);
  }

  Stage stage2 = { stage2_queries };  
  stages->push_back(stage2);



  /*
   * Generate final rewritten query
   */

  std::string final_query_string = "SELECT ";

  std::string join_on = std::string(where_clause);

  // iterate in reverse, because we get the tables in reverse order from mysql
  for (auto it = tables.rbegin(); it != tables.rend();) {
    L_Table table = *it;

    // make final query join clause by replacing table names with interim
    // names in where_clause Warning! this only replaces the first occurence!
    join_on.replace(join_on.find(table.name), table.name.length(),
                    table.interim_name);

    std::vector<std::string>::iterator p = table.projections.begin();

    // ALIAS? for like kolonnenavn? trengs det?
    while (p != table.projections.end()) {
      // final_query_string  += ", " + table.interim_name + "." + *p + " as "
      // + table.name + "." + *p;
      final_query_string += table.interim_name + "." + *p;
      ++p;
      if (p != table.projections.end()) final_query_string += ", ";
    }

    if (++it != tables.rend()) final_query_string += ", ";
  }

  final_query_string += " FROM " + tables[1].interim_name + " JOIN " +
                        tables[0].interim_name + " ON " + join_on;

  // Construct distributed query object

  Distributed_query *dq = new Distributed_query();

  dq->stages = stages;
  dq->rewritten_query = final_query_string;

  return dq;
}

#endif  // LUNDGREN_SEMI_JOIN
