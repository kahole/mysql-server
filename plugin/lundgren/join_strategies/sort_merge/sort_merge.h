#include <mysqlx/xdevapi.h>
#include "plugin/lundgren/distributed_query.h"
#include "plugin/lundgren/join_strategies/common.h"
#include "plugin/lundgren/helpers.h"
#include "plugin/lundgren/constants.h"
#include "plugin/lundgren/partitions/node.h"
#include "plugin/lundgren/join_strategies/sort_merge/k_way_merge_joiner.h"

#ifndef LUNDGREN_SORT_MERGE
#define LUNDGREN_SORT_MERGE

std::string generate_order_by_query(L_Table* table, std::string join_column);

std::string generate_joint_table_schema(mysqlx::SqlResult *lhs_res, mysqlx::SqlResult *rhs_res);
std::string generate_joint_insert_rows_statement(std::vector<mysqlx::Row> lhs_rows, std::vector<mysqlx::Row> rhs_rows, mysqlx::SqlResult *lhs_res, mysqlx::SqlResult *rhs_res);

std::string generate_projections_string_for_final_query(L_Table* table, std::string interim_name);


Distributed_query *execute_sort_merge_distributed_query(L_Parser_info *parser_info) {

    std::string merge_joined_interim_name = generate_interim_name();

    std::vector<L_Table> *tables = &(parser_info->tables);

    L_Table lhs_table = tables->at(0);
    L_Table rhs_table = tables->at(1);

    std::vector<Partition>* lhs_partitions = get_partitions_by_table_name(lhs_table.name);
    std::vector<Partition>* rhs_partitions = get_partitions_by_table_name(rhs_table.name);

    std::string lhs_join_column = lhs_table.join_columns[0];
    std::string rhs_join_column = rhs_table.join_columns[0];

    std::string lhs_order_query = generate_order_by_query(&lhs_table, lhs_join_column);
    std::string rhs_order_query = generate_order_by_query(&rhs_table, rhs_join_column);

    // TODO: flytt all eksekvering til executor saken?

    std::vector<mysqlx::Session*> sessions;

    std::vector<mysqlx::SqlResult*> lhs_streams;
    mysqlx::SqlResult* lhs_res = new mysqlx::SqlResult[rhs_partitions->size()];
    int z = 0;

    for (auto &p : *lhs_partitions) {

        std::string con_string = generate_connection_string(p.node);
        mysqlx::Session* s = new mysqlx::Session(con_string);

        lhs_res[z] = s->sql(lhs_order_query).execute();

        lhs_streams.push_back(&lhs_res[z]);
        sessions.push_back(s);
        z++;
    }

    std::vector<mysqlx::SqlResult*> rhs_streams;
    mysqlx::SqlResult* rhs_res = new mysqlx::SqlResult[rhs_partitions->size()];
    z = 0;

    for (auto &p : *rhs_partitions) {

        std::string con_string = generate_connection_string(p.node);
        mysqlx::Session* s = new mysqlx::Session(con_string);

        rhs_res[z] = s->sql(rhs_order_query).execute();

        rhs_streams.push_back(&rhs_res[z]);
        sessions.push_back(s);
        z++;
    }

    //-----------------------------------------------------

    // FIND LHS JOIN COLUMN INDEX
    uint lhs_join_column_index = 0;
    const mysqlx::Columns *lhs_columns = &lhs_streams.at(0)->getColumns();
    uint lhs_num_columns = lhs_streams.at(0)->getColumnCount();

    for (uint i = 0; i < lhs_num_columns; i++) {
        if (std::string((*lhs_columns)[i].getColumnLabel()) == lhs_join_column) {
            lhs_join_column_index = i;
            break;
        }
    }

    // FIND RHS JOIN COLUMN INDEX
    uint rhs_join_column_index = 0;
    const mysqlx::Columns *rhs_columns = &rhs_streams.at(0)->getColumns();
    uint rhs_num_columns = rhs_streams.at(0)->getColumnCount();

    for (uint i = 0; i < rhs_num_columns; i++) {
        if (std::string((*rhs_columns)[i].getColumnLabel()) == rhs_join_column) {
            rhs_join_column_index = i;
            break;
        }
    }

    //-----------------------------------------------------


    // Insertion into interim
    mysqlx::Session interim_session(generate_connection_string(SelfNode::getNode()));

    std::string create_interim_table_sql = "CREATE TABLE IF NOT EXISTS " + merge_joined_interim_name + " ";
    create_interim_table_sql += generate_joint_table_schema(lhs_streams.at(0), rhs_streams.at(0)) + " " + INTERIM_TABLE_ENGINE ";";

    interim_session.sql(create_interim_table_sql).execute();

    mysqlx::Schema schema = interim_session.getSchema(SelfNode::getNode().database);
    mysqlx::Table table = schema.getTable(merge_joined_interim_name);
    
    K_way_merge_joiner merge_joiner = K_way_merge_joiner(lhs_streams, rhs_streams, lhs_join_column_index, rhs_join_column_index);

    std::vector<mysqlx::Row> lhs_matches;
    std::vector<mysqlx::Row> rhs_matches;

    std::string insert_into_interim_table_start = "INSERT INTO " + merge_joined_interim_name + " VALUES ";

    bool cont = true;
    while(cont) {

        std::tie(lhs_matches, rhs_matches) = merge_joiner.fetchNextMatches();

        if (lhs_matches.size() > 0 && rhs_matches.size() > 0) {

          auto insert = table.insert();

          for (uint i = 0; i < lhs_matches.size(); ++i) {
            for (uint z = 0; z < rhs_matches.size(); ++z) {

              mysqlx::Row merged_row;

              for (uint lhs_c = 0; lhs_c < lhs_num_columns; lhs_c++) {
                merged_row.set(lhs_c, lhs_matches[i][lhs_c]);
              }

              for (uint rhs_c = 0; rhs_c < rhs_num_columns; rhs_c++) {
                merged_row.set(rhs_num_columns + rhs_c, rhs_matches[i][rhs_c]);
              }

              insert.values(merged_row);
            }
          }

          insert.execute();

        } else {
          cont = false;
        }
    }

    interim_session.close();
    
    //--------------------------
    // Cleanup
    for (auto &s : sessions) {
      s->close();
      delete s;
    }

    delete[] lhs_res;
    delete[] rhs_res;
    
    //--------------------------

    std::string final_query_string = "SELECT "
        + generate_projections_string_for_final_query(&lhs_table, merge_joined_interim_name)
        + ((lhs_table.projections.size() > 0 && rhs_table.projections.size() > 0 ) ? ", " : "")
        + generate_projections_string_for_final_query(&rhs_table, merge_joined_interim_name)
        + " FROM " + merge_joined_interim_name;

    Distributed_query *dq = new Distributed_query();
    dq->rewritten_query = final_query_string;
    return dq;
}


std::string generate_order_by_query(L_Table* table, std::string join_column) {

    std::string order_query = "SELECT ";
    order_query += generate_projections_string_for_partition_query(table);
    order_query += " FROM " + table->name;
    order_query += " ORDER BY " + table->name + "." + join_column + " ASC";
    return order_query;
}

std::string write_data_type_column(const mysqlx::Column& col) {
  std::string return_string = "";
  return_string += col.getColumnLabel();
  return_string += " ";
    switch (col.getType()) {
      case mysqlx::Type::BIGINT : 
        return_string += (col.isNumberSigned()) ? "BIGINT" : "BIGINT UNSIGNED"; break;
      case mysqlx::Type::INT : 
        return_string += (col.isNumberSigned()) ? "INT" : "INT UNSIGNED"; break;
      case mysqlx::Type::DECIMAL : 
        return_string += (col.isNumberSigned()) ? "DECIMAL" : "DECIMAL UNSIGNED"; break;
      case mysqlx::Type::DOUBLE : 
        return_string += (col.isNumberSigned()) ? "DOUBLE" : "DOUBLE UNSIGNED"; break;
      case mysqlx::Type::STRING : 
        return_string += "VARCHAR(" + get_column_length(col.getLength()) + ")"; break;
      default: break;
    }

    return return_string;
}

// std::string write_typed_insert_value(const mysqlx::Columns *columns, mysqlx::Row row, uint i) {
  
//   std::string result_string = "";
//   switch ((*columns)[i].getType()) {
//   case mysqlx::Type::BIGINT :
//     result_string += std::to_string(int64_t(row[i])); break;
//   case mysqlx::Type::INT :
//     result_string += std::to_string(int(row[i])); break;
//   case mysqlx::Type::DECIMAL :
//     result_string += std::to_string(double(row[i])); break;
//   case mysqlx::Type::DOUBLE :
//     result_string += std::to_string(double(row[i])); break;
//   case mysqlx::Type::STRING :
//     result_string += std::string("\"") + std::string(row[i]) + std::string("\"") ; break;
//   default: break;
//   }

//   return result_string;
// }


std::string generate_joint_table_schema(mysqlx::SqlResult *lhs_res, mysqlx::SqlResult *rhs_res) {
  std::string return_string = "(";
  uint lhs_column_count = lhs_res->getColumnCount();
  uint rhs_column_count = rhs_res->getColumnCount();

  for (uint i = 0; i < lhs_column_count; i++) {
    const mysqlx::Column& col = lhs_res->getColumn(i);
    return_string += write_data_type_column(col);
    return_string += ",";
  }
  if (rhs_column_count == 0 && !lhs_column_count == 0) {
    return_string.pop_back();
  }

  for (uint i = 0; i < rhs_column_count; i++) {
    const mysqlx::Column& col = rhs_res->getColumn(i);
    return_string += write_data_type_column(col);
    return_string += ",";
  }

  if (!rhs_column_count == 0) {
    return_string.pop_back();
  }
  return return_string + ")";
}

// std::string generate_joint_insert_rows_statement(std::vector<mysqlx::Row> lhs_rows, std::vector<mysqlx::Row> rhs_rows, mysqlx::SqlResult *lhs_res, mysqlx::SqlResult *rhs_res) {
//   std::string result_string = "";

//   const mysqlx::Columns *lhs_columns = &lhs_res->getColumns();
//   const mysqlx::Columns *rhs_columns = &rhs_res->getColumns();
//   uint lhs_column_count = lhs_res->getColumnCount();
//   uint rhs_column_count = rhs_res->getColumnCount();

//   for (uint i = 0; i < lhs_rows.size(); ++i) {
//     for (uint z = 0; z < rhs_rows.size(); ++z) {
//         result_string += "(";

//         for (uint li = 0; li < lhs_column_count; ++li) {
//             result_string += write_typed_insert_value(lhs_columns, lhs_rows[i], li);
//             result_string += ",";
//         }
//         if (rhs_column_count == 0 && !lhs_column_count == 0) {
//             result_string.pop_back();
//         }

//         for (uint ri = 0; ri < rhs_column_count; ++ri) {
//             result_string += write_typed_insert_value(rhs_columns, rhs_rows[z], ri);
//             result_string += ",";
//         }
//         if (!rhs_column_count == 0) {
//             result_string.pop_back();
//         }
//         result_string += "),";
//     }
//   }

//   result_string.pop_back();
//   return result_string;
// }

std::string generate_projections_string_for_final_query(L_Table* table, std::string interim_name) {

  std::string proj_string = "";

  std::vector<std::string>::iterator p = table->projections.begin();

  while (p != table->projections.end()) {
    proj_string += interim_name + "." + *p;
    ++p;
    if (p != table->projections.end()) proj_string += ", ";
  }

  return proj_string;
}

#endif  // LUNDGREN_SORT_MERGE
