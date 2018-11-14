#include <string.h>
#include "plugin/lundgren/constants.h"


static bool is_query_plugin_generated(const char *query) {

    const std::string plugin_flag(PLUGIN_FLAG);

    return (strncmp(query, plugin_flag.c_str(), plugin_flag.length()) == 0);
}

static bool accept_query(const char *query) {

    if (is_query_plugin_generated(query)) {
        return false;
    }

    // TODO:
    // Sjekke etter SELECT, etc
    // for å kun slippe gjennom de vi har støtte for

    return true;
}