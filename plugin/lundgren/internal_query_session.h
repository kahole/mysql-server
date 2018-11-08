
struct st_plugin_ctx;

class Internal_query_session
{
private:
    struct st_plugin_ctx *plugin_ctx;
public:
    Internal_query_session(/* args */);
    ~Internal_query_session();
    static void execute_resultless_queries();
};



