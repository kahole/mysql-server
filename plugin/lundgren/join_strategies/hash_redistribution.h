#include "plugin/lundgren/distributed_query.h"
#include "plugin/lundgren/helpers.h"
#include "plugin/lundgren/partitions/partition.h"
#include "plugin/lundgren/distributed_query_manager.h"
#include "plugin/lundgren/partitions/node.h"
#include "plugin/lundgren/constants.h"

#ifndef LUNDGREN_HASH_REDIST_JOIN
#define LUNDGREN_HASH_REDIST_JOIN

static Distributed_query *make_hash_redist_join_distributed_query(L_Parser_info *parser_info, L_parsed_comment_args parsed_args);
static bool is_hash_redist_slave(L_parsed_comment_args parsed_args);
Distributed_query *execute_hash_redist_slave(L_Parser_info *parser_info, L_parsed_comment_args parsed_args);

static bool is_hash_redist_slave(L_parsed_comment_args parsed_args) {
  return parsed_args.comment_args_lookup_table[HASH_REDIST_SLAVE_FLAG] == "true";
}

static Distributed_query *make_hash_redist_join_distributed_query(L_Parser_info *parser_info, L_parsed_comment_args parsed_args) {

    if (!is_hash_redist_slave(parsed_args)) {
    
        // STAGE 1
        // - Hente tabellnavn
        // - Finne ut hvor tabellene eksisterer (noder å involvere)
        // - Generere interim table names for tabellene
        // - Sende plan
        
        std::vector<Stage> stages;

        std::vector<L_Table> *tables = &(parser_info->tables);
        std::map<std::string, std::string> interim_table_names; // Maps table names to interim tabel names
        std::map<std::string, std::vector<std::string>> nodes_and_partitions; // Maps node id to partitions the node contains
        std::map<std::string, Node> node_id_to_node_obj; // Maps node id to actual node (a bit dumdum, should be refactored)
        
        for(auto &table : *tables) {
            interim_table_names[table.name] = generate_interim_name();

            /* Maps nodes with tables they hold */
            for (auto &partition : *get_partitions_by_table_name(table.name)) { 
                nodes_and_partitions[std::to_string(partition.node.id)].push_back(table.name);
                node_id_to_node_obj[std::to_string(partition.node.id)] = partition.node;
            }
        }

        Stage stage1;

        for (auto node : nodes_and_partitions) {
            std::string pq_sql_statement = "/*distributed";
            pq_sql_statement += "<join_strategy=hash_redist,";
            pq_sql_statement += HASH_REDIST_SLAVE_FLAG "=true";
            for (auto table : node.second) { // Iterates partitions 
                pq_sql_statement += ',' + table + '=' + interim_table_names[table];        
            }
            pq_sql_statement += ">*/";
            pq_sql_statement += "DO 0"; // Query no-op
            stage1.partition_queries.push_back(Partition_query{pq_sql_statement,node_id_to_node_obj[node.first]});
        }


        // STAGE 2
        // - Sparke i gang andre noder med flagg, litt som slave
        // - Opprette tråder
        // - Generere hash-funksjon for å hashe data
        // - Tråder hasher tabellene og flytter data som et steg (utfører spørringene sekvensielt)
        // "INSERT INTO interim_tabel (SELECT * FROM tabell WHERE MD5(join-kol) = node.id)"

        // STAGE 3
        // - Utføre join lokalt på hver node
        // - Returnere resultat


        return NULL;
    }
    else
    {
        return execute_hash_redist_slave(parser_info, parsed_args);
    }
    
}

Distributed_query *execute_hash_redist_slave(L_Parser_info *parser_info MY_ATTRIBUTE((unused)), L_parsed_comment_args parsed_args MY_ATTRIBUTE((unused))) {
    return NULL;
}

#endif  // LUNDGREN_HASH_REDIST_JOIN






// std::vector<std::string> interim_table_names;
// for (auto &table : *tables) {
//     std::vector<Partition> *partitions = get_partitions_by_table_name(table.name);
    
//     std::string interim_name = generate_interim_name();
//     interim_table_names.push_back(interim_name);

//     /* Hack alert - Get table schema */
//     std::string con_string = generate_connection_string(partitions->front().node);
//     mysqlx::Session s(con_string);
//     mysqlx::SqlResult res = s.sql("SELECT * FROM " + table.name + " LIMIT 1").execute();
//     std::string table_schema = generate_table_schema(&res);
//     s.close();

//     /* Create interim table on each node containing a part of the table */
//     for (auto partition : *partitions) {
//         std::string con_string = generate_connection_string(partition.node);
//         mysqlx::Session s(con_string);

//         std::string query_string = "CREATE TABLE IF NOT EXISTS " + interim_name + ' ' + table_schema;
//         mysqlx::SqlResult res = s.sql(query_string).execute();
//         s.close();
//     }
// }
