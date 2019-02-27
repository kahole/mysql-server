#include <string.h>
#include <mysql/service_parser.h>
#include "plugin/lundgren/constants.h"

#ifndef LUNDGREN_QUERY_ACCEPTANCE
#define LUNDGREN_QUERY_ACCEPTANCE

static bool should_query_be_distributed(const char *query) {

    const std::string plugin_flag(PLUGIN_FLAG);
    const std::string query_string(query);
    return (query_string.find(plugin_flag) != std::string::npos);
}

static bool accept_query(MYSQL_THD thd, const char *query) {

    if (!should_query_be_distributed(query)) {
        return false;
    }

    int type = mysql_parser_get_statement_type(thd);

    return (type == STATEMENT_TYPE_SELECT);
}

static bool detect_join(const char *query) {
  std::string join_keyword = "JOIN";
  std::string join_keyword_lower = "join";

  std::string query_str(query);

  return (query_str.find(join_keyword) != std::string::npos || query_str.find(join_keyword_lower) != std::string::npos);
}
#endif  // LUNDGREN_QUERY_ACCEPTANCE
