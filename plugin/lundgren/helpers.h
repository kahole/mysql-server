#include <string.h>
#include <vector>
#include <sstream>
#include <iostream>

#ifndef LUNDGREN_HELPERS
#define LUNDGREN_HELPERS


std::string random_string(size_t length) {
  auto randchar = []() -> char {
    const char charset[] =
        //"0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";
    const size_t max_index = (sizeof(charset) - 1);
    return charset[rand() % max_index];
  };
  std::string str(length, 0);
  std::generate_n(str.begin(), length, randchar);
  return str;
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

// Parses the parameters in the passed comment and creates a lookup table
std::map<std::string, std::string> parse_query_comments(const char *query) {
  char delimiter;
  std::string query_string = std::string(query);
  int pos_start = query_string.find("<")+1;
  int pos_end = query_string.find(">") - pos_start;
  query_string = query_string.substr(pos_start, pos_end);

  delimiter = ',';
  std::vector<std::string> comment_parameters = split(query_string, delimiter);

  delimiter = '=';
  std::map<std::string, std::string> comment_parameter_lookup_table;
  for (auto const parameter : comment_parameters) {
    int pos_delimiter = parameter.find(delimiter);
    comment_parameter_lookup_table[parameter.substr(0,pos_delimiter)] = parameter.substr(pos_delimiter+1);
  } 

  return comment_parameter_lookup_table;
}



#endif  // LUNDGREN_HELPERS