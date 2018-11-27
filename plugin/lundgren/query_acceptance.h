#include <string.h>
#include <mysql/service_parser.h>
#include "plugin/lundgren/constants.h"

#ifndef LUNDGREN_QUERY_ACCEPTANCE
#define LUNDGREN_QUERY_ACCEPTANCE

static bool should_query_be_distributed(const char *query) {

    const std::string plugin_flag(PLUGIN_FLAG);

    return (strncmp(query, plugin_flag.c_str(), plugin_flag.length()) == 0);
}

static bool accept_query(MYSQL_THD thd, const char *query) {

    if (!should_query_be_distributed(query)) {
        return false;
    }

    int type = mysql_parser_get_statement_type(thd);

    return (type == STATEMENT_TYPE_SELECT);
}

#endif  // LUNDGREN_QUERY_ACCEPTANCE