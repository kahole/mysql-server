#include <string.h>
#include <vector>
#include <sstream>
#include <iostream>
#include <boost/uuid/uuid.hpp>            // uuid class
#include <boost/uuid/uuid_generators.hpp> // generators
#include <boost/uuid/uuid_io.hpp>         // streaming operators etc.
#include <boost/algorithm/string.hpp>

#ifndef LUNDGREN_HELPERS
#define LUNDGREN_HELPERS

struct L_parsed_comment_args;
std::string generate_interim_name();
std::vector<std::string> split(std::string strToSplit, char delimeter);
L_parsed_comment_args parse_query_comments(const char *query);
std::string string_remove_ends(std::string input_string);

std::string generate_interim_name() {

  boost::uuids::random_generator generator;
  boost::uuids::uuid uuid1 = generator();
  std::string uuid_string = boost::uuids::to_string(uuid1);

  boost::erase_all(uuid_string, "-");

  return 'i' + uuid_string; // Must prefix with a letter as numbers are not allowed.
}

// Split string by delimiter: https://thispointer.com/how-to-split-a-string-in-c/
std::vector<std::string> split(std::string strToSplit, char delimeter)
{
    std::stringstream ss(strToSplit);
    std::string item;
    std::vector<std::string> splittedStrings;
    while (std::getline(ss, item, delimeter))
    {
       splittedStrings.push_back(item);
    }
    return splittedStrings;
}

enum JOIN_STRATEGY {DATA_TO_QUERY, SEMI, BLOOM, HASH_REDIST};
const std::map<std::string, JOIN_STRATEGY> join_strategy_string_to_enum = {
    {"data_to_query", DATA_TO_QUERY}, 
    {"semi", SEMI}, 
    {"bloom", BLOOM}, 
    {"hash_redist", HASH_REDIST}
  };

struct L_parsed_comment_args {
  JOIN_STRATEGY join_strategy;
  std::map<std::string, std::string> comment_args_lookup_table;
};

// Parses the parameters in the passed comment and creates a lookup table
L_parsed_comment_args parse_query_comments(const char *query) {
  char delimiter;
  std::string query_string = std::string(query);
  int pos_start = query_string.find("<")+1;
  int pos_end = query_string.find(">") - pos_start;
  query_string = query_string.substr(pos_start, pos_end);

  delimiter = ',';
  std::vector<std::string> comment_parameters = split(query_string, delimiter);

  delimiter = '=';
  L_parsed_comment_args parsed_args;
  int i;
  for (i = 0; i < int(comment_parameters.size()); i++){
    if (comment_parameters[i].find("join_strategy") != std::string::npos) {
      int pos_delimiter = comment_parameters[i].find(delimiter);
      std::string js = comment_parameters[i].substr(pos_delimiter+1);
      parsed_args.join_strategy = join_strategy_string_to_enum.at(js);
      break;
    }
  }
  comment_parameters.erase(comment_parameters.begin() + i);

  delimiter = '=';
  std::map<std::string, std::string> comment_parameter_lookup_table;
  for (auto const parameter : comment_parameters) {
    int pos_delimiter = parameter.find(delimiter);
    parsed_args.comment_args_lookup_table[parameter.substr(0,pos_delimiter)] = parameter.substr(pos_delimiter+1);

  } 

  return parsed_args;
}

std::string string_remove_ends(std::string input_string) {
    input_string.erase(input_string.begin());
    input_string.pop_back();
    return input_string;
}



#endif  // LUNDGREN_HELPERS
