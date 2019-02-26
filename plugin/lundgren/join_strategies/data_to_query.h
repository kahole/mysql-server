#include <string.h>
#include "plugin/lundgren/distributed_query.h"
#include "plugin/lundgren/partitions/partition.h"
#include "plugin/lundgren/helpers.h"

#ifndef LUNDGREN_DATA_TO_QUERY
#define LUNDGREN_DATA_TO_QUERY


// Pure Distributed_query strategy, can be fed into DQM


static Distributed_query *make_data_to_query_distributed_query(L_Parser_info *parser_info, bool is_join) {

  std::vector<L_Table> tables = parser_info->tables;
  std::string where_clause = parser_info->where_clause;
  
  
  std::vector<Partition_query> partition_queries;

  for (auto &table : tables) {

    std::vector<Partition> *partitions =
        get_partitions_by_table_name(table.name);

    if (partitions == NULL) {
      return NULL;
    }

    std::string partition_query_string = "SELECT ";

    std::vector<std::string>::iterator p = table.projections.begin();

    while (p != table.projections.end()) {
      partition_query_string += table.name + "." + *p;
      ++p;
      if (p != table.projections.end()) partition_query_string += ", ";
    }
    if (table.where_transitive_projections.size() > 0)
      partition_query_string += ", ";
    p = table.where_transitive_projections.begin();
    while (p != table.where_transitive_projections.end()) {
      partition_query_string += table.name + "." + *p;
      ++p;
      if (p != table.where_transitive_projections.end())
        partition_query_string += ", ";
    }

    std::string from_table = " FROM " + std::string(table.name);
    table.interim_name = random_string(30);

    partition_query_string += from_table;

    if (!is_join && where_clause.length() > 0)
      partition_query_string += " WHERE " + where_clause;

    for (std::vector<Partition>::iterator p = partitions->begin();
         p != partitions->end(); ++p) {

      Node self(true);
      std::vector<Node> target_nodes{self};

      Interim_target interim_target = {table.interim_name, target_nodes};

      Partition_query pq = {partition_query_string, p->node, interim_target};
      partition_queries.push_back(pq);
    }

    delete partitions;
  }

  /*
   * Generate final rewritten query
   */

  std::string final_query_string = "SELECT ";

  if (is_join) {
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

  } else {
    L_Table first_table = tables[0];
    std::vector<std::string>::iterator p = first_table.projections.begin();

    // ALIAS trengs ikke her. Tror Person.blabla blir brukt dersom flere
    // kolonner med samme navn, altså kun med flere tabeller final_query_string
    // += first_table.interim_name + "." + *p + " as " + first_table.name + "."
    // + *p;
    while (p != first_table.projections.end()) {
      // final_query_string  += ", " + first_table.interim_name + "." + *p + "
      // as " + first_table.name + "." + *p;
      final_query_string += first_table.interim_name + "." + *p;
      ++p;
      if (p != first_table.projections.end()) final_query_string += ", ";
    }
    final_query_string += " FROM " + first_table.interim_name;
  }

  // Construct distributed query object

  Distributed_query *dq = new Distributed_query();

  Stage stage = {partition_queries};
  dq->stages = std::vector<Stage>();
  dq->stages.push_back(stage);
  dq->rewritten_query = final_query_string;

  return dq;
}

#endif  // LUNDGREN_DATA_TO_QUERY
