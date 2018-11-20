
#ifndef LUNDGREN_INTERNAL_QUERY_SESSION
#define LUNDGREN_INTERNAL_QUERY_SESSION

struct st_plugin_ctx;
typedef class Srv_session *MYSQL_SESSION;
class Sql_resultset;

class Internal_query_session
{
private:
    struct st_plugin_ctx *plugin_ctx;
    MYSQL_SESSION session;
    
public:
    Internal_query_session(/* args */);
    ~Internal_query_session();
    int execute_resultless_query(const char *query);
    Sql_resultset * execute_query(const char *query);
};

#endif  // LUNDGREN_INTERNAL_QUERY_SESSION