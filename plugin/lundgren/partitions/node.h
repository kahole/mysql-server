#include <string.h>
#include "plugin/lundgren/internal_query/internal_query_session.h"
#include "plugin/lundgren/internal_query/sql_resultset.h"

#ifndef LUNDGREN_NODE
#define LUNDGREN_NODE

struct Node {
  std::string host;
  uint port;
  std::string database;
  std::string user;
  bool is_self = false;
  int id;

  Node(bool is_self_in) : is_self(is_self_in) {
    host = "127.0.0.1";
    port = 13010;
    database = "test";
    user = "root";
    id = 0;
  }

  Node(char *host_in, uint port_in, char *database_in, char *user_in, int id_in)
      : port(port_in), id(id_in) {
    host = std::string(host_in);
    database = std::string(database_in);
    user = std::string(user_in);
  }

  Node() {}
};

Node getNodeById(std::string node_id) {
  
  Internal_query_session *session = new Internal_query_session();

  session->execute_resultless_query("USE test");

  std::string node_query = "SELECT * FROM lundgren_node WHERE lundgren_node.id = " + node_id;

  Sql_resultset *result = session->execute_query(node_query.c_str());

  if (result->get_rows() == 0) {
      return NULL;
  }

  Node node;

  do {
      node = Node(result->getString(1), (uint)result->getLong(2), result->getString(3), result->getString(4), result->getLong(0));
  } while (result->next());

  return node;
  //fiks self node med:
  // SET @node_id = 0|1|2|3...; i oppstartsscriptene
}

class SelfNode {
  static SelfNode *instance;
  Node internal_node = Node(true);

  SelfNode(uint port) {
    internal_node = Node(true);
    internal_node.port = port;
  }

 public:
  static Node getNode() {
    if (!instance) {
      Internal_query_session *session = new Internal_query_session();

      session->execute_resultless_query("USE test");

      // sjekk portnummer
      std::string port_number_query = "SELECT @@port;";
      Sql_resultset *result = session->execute_query(port_number_query.c_str());
      if (result->get_rows() == 0) {
        return Node(true);
      }
      // sett portnummer
      uint port_number = (uint)result->getLong(0) + 10;

      instance = new SelfNode(port_number);
    }
    return instance->internal_node;
  }
};

SelfNode *SelfNode::instance = 0;

#endif  // LUNDGREN_NODE