#include <mysqlx/xdevapi.h>
//#include <queue>
#include <tuple>

#ifndef LUNDGREN_K_WAY
#define LUNDGREN_K_WAY

class K_way_node {
private:
    mysqlx::SqlResult* stream;
    mysqlx::Row current_row;

public:
    K_way_node(mysqlx::SqlResult* s) {
        stream = s;
        current_row = stream->fetchOne();
    }

    mysqlx::Row peek() {
        return current_row;
    }

    bool is_empty() {
      if (current_row) {
        return false;
      } else {
        return true;
      }
      // Yes, i know this ^ looks stupid, but the one below doesn't invoke the bool operator () ?
        // return (current_row);
    }

    mysqlx::Row next() {
        mysqlx::Row popped_row = current_row;
        current_row = stream->fetchOne();
        return popped_row;
    }
};

class K_way_heap {
private:
    K_way_node *root;
    std::vector<K_way_node*> children;
    int column_index;

public:
    K_way_heap() {}
    K_way_heap(std::vector<mysqlx::SqlResult*> streams, int c_index) {

        column_index = c_index;
        for (auto &s : streams) {
            children.push_back(new K_way_node(s));
        }
        root = children[0];
        correct_heap();
    }

    ~K_way_heap() {
        // TODO: fix
        // for (auto &n : children) {
        //     delete n;
        // }
    }

    bool has_next() {
        for (auto &n : children) {
            if (!n->is_empty()) {
            return true;
            }
        }
        return false;
    }

    mysqlx::Row pop() {
        mysqlx::Row tmp = root->next();
        correct_heap();
        return tmp;
    }

    mysqlx::Row peek() {
        return root->peek();
    }

    void correct_heap() {
        for (auto &n : children) {
            if (root->is_empty()) {
                root = n;
            }
            if (!n->is_empty() && ((int) n->peek()[column_index]) < ((int) root->peek()[column_index])) {
                root = n;
            }
        }
    }
};

class K_way_merge_joiner {

private:
    K_way_heap lhs_heap;
    K_way_heap rhs_heap;

    std::vector<mysqlx::Row> lhs_buffer;
    std::vector<mysqlx::Row> rhs_buffer;

    int lhs_column_index;
    int rhs_column_index;

public:
    K_way_merge_joiner(std::vector<mysqlx::SqlResult*> lhs, std::vector<mysqlx::SqlResult*> rhs, int lhs_column_index, int rhs_column_index) {

        lhs_heap = K_way_heap(lhs, lhs_column_index);
        rhs_heap = K_way_heap(rhs, rhs_column_index);

        this->lhs_column_index = lhs_column_index;
        this->rhs_column_index = rhs_column_index;
    }

    void buffer_next_value_candidates() {
        int current_value;
        // empty buffers
        lhs_buffer = std::vector<mysqlx::Row>();
        rhs_buffer = std::vector<mysqlx::Row>();

        while ((lhs_heap.has_next() && rhs_heap.has_next()) && (lhs_buffer.size() == 0 || rhs_buffer.size() == 0)) {
          
        // empty buffers
        lhs_buffer = std::vector<mysqlx::Row>();
        rhs_buffer = std::vector<mysqlx::Row>();

        // Move onto next value
        if (((int)lhs_heap.peek()[lhs_column_index]) >= ((int)rhs_heap.peek()[rhs_column_index])) {
            current_value = lhs_heap.peek()[lhs_column_index];

            // skip ahead until we find rows that match
            while(((int)rhs_heap.peek()[rhs_column_index]) < current_value) {
              rhs_heap.pop();
            }
            
        } else {
            current_value = rhs_heap.peek()[rhs_column_index];

            // skip ahead until we find rows that match
            while(((int)lhs_heap.peek()[lhs_column_index]) < current_value) {
              lhs_heap.pop();
            }
            
        }

        // Buffer all values that are equal to the new "current"
        while (((int)lhs_heap.peek()[lhs_column_index]) == current_value) {
            lhs_buffer.push_back(lhs_heap.pop());
        }

        while (((int)rhs_heap.peek()[rhs_column_index]) == current_value) {
            rhs_buffer.push_back(rhs_heap.pop());
        }
        }
    }

    std::tuple<std::vector<mysqlx::Row>, std::vector<mysqlx::Row>> fetchNextMatches() {

        // if (lhs_heap.has_next() && rhs_heap.has_next()) {

        //   do {
        //     buffer_next_value_candidates();
        //   } while (lhs_buffer.size() == 0 || rhs_buffer.size() == 0);

        //     return std::make_tuple(lhs_buffer, rhs_buffer);

        // } else {
        //     // return empty vectors in tuple
        //     return std::tuple<std::vector<mysqlx::Row>, std::vector<mysqlx::Row>>();
        // }

        // New attempt
        buffer_next_value_candidates();

        return std::make_tuple(lhs_buffer, rhs_buffer);
    }
};


#endif  // LUNDGREN_K_WAY
