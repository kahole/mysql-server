#include <string.h>
#include "plugin/lundgren/constants.h"

#ifndef LUNDGREN_QUERY_ACCEPTANCE
#define LUNDGREN_QUERY_ACCEPTANCE

static bool is_query_plugin_generated(const char *query) {

    const std::string plugin_flag(PLUGIN_FLAG);

    return (strncmp(query, plugin_flag.c_str(), plugin_flag.length()) == 0);
}

static bool is_query_select_statement(const char *query) {
    return (strncmp(query, "SELECT", 6) == 0);
}

static bool accept_query(const char *query) {

    if (is_query_plugin_generated(query)) {
        return false;
    }

    if (!is_query_select_statement(query)) {
        return false;
    }

    return true;
}

#endif  // LUNDGREN_QUERY_ACCEPTANCE