#include <string.h>
#include "plugin/lundgren/distributed_query.h"

#ifndef LUNDGREN_COMMON
#define LUNDGREN_COMMON

static std::string generate_join_query_string(std::vector<L_Table> tables, std::string join_on, bool interim);
static std::string generate_final_join_query_string(std::vector<L_Table> tables, std::string join_on);
std::string generate_projections_string_for_partition_query(L_Table* table);

static std::string generate_join_query_string(std::vector<L_Table> tables, std::string join_on, bool interim) {

  std::string final_query_string = "SELECT ";

  // iterate in reverse, because we get the tables in reverse order from mysql
  for (auto it = tables.rbegin(); it != tables.rend();) {
    L_Table table = *it;

    // make final query join clause by replacing table names with interim
    // names in where_clause Warning! this only replaces the first occurence!
    join_on.replace(join_on.find(table.name), table.name.length(),
                    (interim ? table.interim_name : table.name));

    std::vector<std::string>::iterator p = table.projections.begin();
      std::vector<std::string>::iterator a = table.aliases.begin();

    // ALIAS? for like kolonnenavn? trengs det?
    while (p != table.projections.end()) {
      // final_query_string  += ", " + (interim ? table.interim_name : table.name) + "." + *p + " as "
      // + table.name + "." + *p;
      final_query_string += (interim ? table.interim_name : table.name) + "." + *p;
      final_query_string += a->length() > 0 ? " as " + *a: "";
      ++p;
      ++a;
      if (p != table.projections.end()) final_query_string += ", ";
    }

    if (++it != tables.rend()) final_query_string += ", ";
  }

  final_query_string += " FROM " + (interim ? tables[1].interim_name : tables[1].name) + " JOIN " +
                        (interim ? tables[0].interim_name : tables[0].name) + " ON " + join_on;


  return final_query_string;
}

static std::string generate_final_join_query_string(std::vector<L_Table> tables, std::string join_on) {
  return generate_join_query_string(tables, join_on, true);
}

std::string generate_projections_string_for_partition_query(L_Table* table) {

  std::string proj_string = "";

  std::vector<std::string>::iterator p = table->projections.begin();

  while (p != table->projections.end()) {
    proj_string += table->name + "." + *p;
    ++p;
    if (p != table->projections.end()) proj_string += ", ";
  }
  if (table->where_transitive_projections.size() > 0){
    proj_string += ", ";
  }
  p = table->where_transitive_projections.begin();
  while (p != table->where_transitive_projections.end()) {
    proj_string += table->name + "." + *p;
    ++p;
    if (p != table->where_transitive_projections.end())
      proj_string += ", ";
  }

  return proj_string;
}

#endif  // LUNDGREN_COMMON
