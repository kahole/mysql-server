#include "plugin/lundgren/distributed_query.h"
#include "plugin/lundgren/helpers.h"
#include "plugin/lundgren/partitions/partition.h"
#include "plugin/lundgren/distributed_query_manager.h"
#include "plugin/lundgren/partitions/node.h"
#include "plugin/lundgren/constants.h"
#include "plugin/lundgren/partitions/node.h"
#include "plugin/lundgren/join_strategies/common.h"

#ifndef LUNDGREN_HASH_REDIST_JOIN
#define LUNDGREN_HASH_REDIST_JOIN

static Distributed_query *make_hash_redist_join_distributed_query(L_Parser_info *parser_info, L_parsed_comment_args parsed_args, const char *original_query);
static bool is_hash_redist_slave(L_parsed_comment_args parsed_args);
Distributed_query *execute_hash_redist_slave(L_Parser_info *parser_info, L_parsed_comment_args parsed_args);

static bool is_hash_redist_slave(L_parsed_comment_args parsed_args) {
  return parsed_args.comment_args_lookup_table[HASH_REDIST_SLAVE_FLAG] == "true";
}

static Distributed_query *make_hash_redist_join_distributed_query(L_Parser_info *parser_info, L_parsed_comment_args parsed_args, const char *original_query) {

    if (!is_hash_redist_slave(parsed_args)) {
    
        // STAGE 1
        // - Hente tabellnavn
        // - Finne ut hvor tabellene eksisterer (noder å involvere)
        // - Generere interim table names for tabellene
        // - Sparke i gang andre noder med flagg, litt som slave
        // - Sende plan
        // - "INSERT INTO interim_tabel (SELECT * FROM tabell WHERE MD5(join-kol) = node.id)"
        
        std::vector<Stage> stages;

        std::vector<L_Table> *tables = &(parser_info->tables);
        std::map<std::string, std::string> interim_table_names; // Maps table names to interim tabel names
        std::map<std::string, std::string> table_join_column; // Maps table names to its join columns
        std::map<std::string, std::vector<std::string>> nodes_and_partitions; // Maps node id to partitions the node contains
        std::map<std::string, Node> node_id_to_node_obj; // Maps node id to actual node (a bit dumdum, should be refactored)
        
        for(auto &table : *tables) {
            interim_table_names[table.name] = generate_interim_name();
            table.interim_name = interim_table_names[table.name];
            table_join_column[table.name] = table.where_transitive_projections[0];

            /* Maps nodes with tables they hold and linking id to node objects */
            for (auto &partition : *get_partitions_by_table_name(table.name)) { 
                nodes_and_partitions[std::to_string(partition.node.id)].push_back(table.name);
                node_id_to_node_obj[std::to_string(partition.node.id)] = partition.node;
            }
        }

        Stage stage1;

        for (auto node : nodes_and_partitions) {
            std::string pq_sql_statement = "/*" PLUGIN_FLAG;
            pq_sql_statement += "<join_strategy=hash_redist,";
            pq_sql_statement += HASH_REDIST_SLAVE_FLAG "=true";
            pq_sql_statement += ",tables=["; // Hack - Format: "[table1:a6fbd:join-col|table2:bcf34:join-col]"
            for (auto table : node.second) { // Iterates partitions 
                pq_sql_statement += table + ':' + interim_table_names[table] + ':' + table_join_column[table] +'|';        
            }
            pq_sql_statement.pop_back(); // Delete last pipe
            pq_sql_statement += "]>*/";
            // pq_sql_statement += std::string(original_query); // Query no-op
            std::string original_query_stripped = std::string(original_query);
            std::string comment_end = "*/";
            pq_sql_statement += original_query_stripped.substr(original_query_stripped.find(comment_end) + std::string(comment_end).length());
            stage1.partition_queries.push_back(Partition_query{pq_sql_statement,node_id_to_node_obj[node.first]});
        }

        // STAGE 2
        // - Opprette interim table på master for å motta joinede resultater
        // - Utføre join lokalt på hver node
        // - Returnere resultat
        // - Select * from interim table med resultat

        Stage stage2;

        std::string result_interim_table = generate_interim_name();
        Interim_target it = {result_interim_table, {SelfNode::getNode()}};
        for (auto node : nodes_and_partitions) {
            std::string pq_sql_statement = generate_join_query_string(parser_info->tables, parser_info->where_clause, true);

            stage2.partition_queries.push_back(Partition_query{pq_sql_statement, node_id_to_node_obj[node.first], it});
        }

        std::string final_query = "SELECT * FROM " + result_interim_table + ';';
        Distributed_query *dq = new Distributed_query{final_query, {stage1,stage2}};
        return dq;
    }
    else
    {
        return execute_hash_redist_slave(parser_info, parsed_args);
    }
    
}

Distributed_query *execute_hash_redist_slave(L_Parser_info *parser_info, L_parsed_comment_args parsed_args) {
    
    Stage stage;

    std::string local_tables = parsed_args.comment_args_lookup_table["tables"];
    local_tables = string_remove_ends(local_tables);
    std::vector<std::string> parsed_local_tables = split(local_tables, '|');
    std::map<std::string, std::string> table_to_projection;

    for (auto &table : parser_info->tables) {
        table_to_projection[table.name] = generate_projections_string_for_partition_query(&table);
    }

    
    for (auto table : parsed_local_tables) {
        std::string table_name = table.substr(0,table.find(':'));
        std::string interim_table_name = table.substr(table.find(':')+1, table.rfind(':')-table.find(':')-1);
        std::string table_join_column = table.substr(table.rfind(':')+1);

        std::vector<Partition> *partitions = get_partitions_by_table_name(table_name);

        std::string pq_sql_statement = "SELECT " + table_to_projection[table_name] + " FROM " + table_name + 
            " WHERE MD5(" + table_join_column + ")%" + std::to_string(partitions->size()) + '=';
        
        for (auto &partition : *partitions) {
            Interim_target it = {interim_table_name, {partition.node}};
            Partition_query pq = {pq_sql_statement + std::to_string(partition.node.id), SelfNode::getNode(), it};
            stage.partition_queries.push_back(pq);
        }
    }

    Distributed_query *dq = new Distributed_query{"DO 0;", {stage}};
    return dq;
}

#endif  // LUNDGREN_HASH_REDIST_JOIN