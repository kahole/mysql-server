#include <string.h>
#include "plugin/lundgren/distributed_query.h"

#ifndef LUNDGREN_COMMON
#define LUNDGREN_COMMON

static std::string generate_final_join_query_string(std::vector<L_Table> tables, std::string join_on) {

  /*
   * Generate final rewritten query
   */

  std::string final_query_string = "SELECT ";

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


  return final_query_string;
}

#endif  // LUNDGREN_COMMON
