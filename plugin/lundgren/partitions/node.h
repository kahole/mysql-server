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


  delete session;

  return node;
}

class SelfNode {
  static SelfNode *instance;
  Node internal_node = Node(true);

  SelfNode(Node n) {
    internal_node = n;
  }

  // SET @@global.node_id = 0|1|2|3...; in startup scripts

 public:
  static Node getNode() {
    if (!instance) {
      Internal_query_session *session = new Internal_query_session();

      session->execute_resultless_query("USE test");

      // sjekk id
      std::string id_number_query = "SELECT node_id FROM lundgren_self_node_id";
      Sql_resultset *result = session->execute_query(id_number_query.c_str());
      if (result->get_rows() == 0) {
        return Node(true);
      }
      // sett portnummer
      //uint port_number = (uint)result->getLong(0) + 10;
      std::string node_query = "SELECT * FROM lundgren_node WHERE lundgren_node.id = " + std::to_string(result->getLong(0));
      Sql_resultset *node_result = session->execute_query(node_query.c_str());
      if (node_result->get_rows() == 0) {
          return Node(true);
      }
      Node node;
      do {
          node = Node(node_result->getString(1), (uint)node_result->getLong(2), node_result->getString(3), node_result->getString(4), node_result->getLong(0));
      } while (node_result->next());

      instance = new SelfNode(node);

      delete session;
    }
    return instance->internal_node;
  }
};

SelfNode *SelfNode::instance = 0;

#endif  // LUNDGREN_NODE