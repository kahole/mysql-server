#include <mysqlx/xdevapi.h>
//#include <queue>
#include <tuple>
#include <algorithm>

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
        return !current_row.operator bool();
    }

    mysqlx::Row next() {
        mysqlx::Row popped_row = current_row;
        current_row = stream->fetchOne();
        return popped_row;
    }
};

//----------------

class Bin_heap {

private:
    int column_index;
    std::vector<K_way_node> nodes;

private:
    static int left(int i) {
        return 2*i;
    }

    static int right(int i) {
        return 2*i+1;
    }

    static int parent(int i) {
        return i/2;
    }

    int at(int i) {
        return (int) nodes[i - 1].peek()[column_index];
    }

    bool node_empty(int i) {
        return nodes[i - 1].is_empty();
    }

    void swap(int t, int f) {
        std::iter_swap(nodes.begin() + t-1, nodes.begin() + f-1);
        // K_way_node t_node = nodes[t-1];
        // nodes[t-1] = nodes[f-1];
        // nodes[f-1] = t_node;
    }

    void heapify() {
        int heap_size = nodes.size();

        for (int i = heap_size / 2; i > 0; --i) {
            sift_down(i);
        }
    }

    void sift_down(int i) {
        int heap_size = nodes.size();
        int l = left(i);
        int r = right(i);

        int smallest;

        if (l <= heap_size && (node_empty(i) || at(l) < at(i))) {
            smallest = l;
        } else {
            smallest = i;
        }

        if (r <= heap_size && (node_empty(smallest) || at(r) < at(smallest))) {
            smallest = r;
        } else {
            smallest = i;
        }

        if (smallest != i) {
            swap(i, smallest);
            sift_down(smallest);
        }
    }

public:
    Bin_heap() {}
    Bin_heap(std::vector<mysqlx::SqlResult*> streams, int c_index) {
        column_index = c_index;
        for (auto &s : streams) {
            nodes.push_back(K_way_node(s));
        }
        heapify();
    }

    bool has_next() {
        return !nodes[0].is_empty();
    }

    mysqlx::Row pop() {
        mysqlx::Row tmp = nodes[0].next();
        sift_down(1);
        return tmp;
    }

    mysqlx::Row peek() {
        return nodes[0].peek();
    }
};




//----------------------

class K_way_heap {
private:
    K_way_node *root;
    std::vector<K_way_node> children;
    int column_index;

public:
    K_way_heap() {}
    K_way_heap(std::vector<mysqlx::SqlResult*> streams, int c_index) {

        column_index = c_index;
        for (auto &s : streams) {
            children.push_back(K_way_node(s));
        }
        root = &children[0];
        correct_heap();
    }

    bool has_next() {
        for (auto &n : children) {
            if (!n.is_empty()) {
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
                root = &n;
            }
            if (!n.is_empty() && ((int) n.peek()[column_index]) < ((int) root->peek()[column_index])) {
                root = &n;
            }
        }
    }
};

class K_way_merge_joiner {

private:
    Bin_heap lhs_heap;
    Bin_heap rhs_heap;

    std::vector<mysqlx::Row> lhs_buffer;
    std::vector<mysqlx::Row> rhs_buffer;

    int lhs_column_index;
    int rhs_column_index;

public:
    K_way_merge_joiner(std::vector<mysqlx::SqlResult*> lhs, std::vector<mysqlx::SqlResult*> rhs, int lhs_column_index, int rhs_column_index) {

        lhs_heap = Bin_heap(lhs, lhs_column_index);
        rhs_heap = Bin_heap(rhs, rhs_column_index);

        this->lhs_column_index = lhs_column_index;
        this->rhs_column_index = rhs_column_index;
    }

    void buffer_next_value_candidates() {

        while (lhs_heap.has_next() && rhs_heap.has_next() && (lhs_buffer.size() == 0 || rhs_buffer.size() == 0)) {

            int current_value;

            // empty buffers
            lhs_buffer = std::vector<mysqlx::Row>();
            rhs_buffer = std::vector<mysqlx::Row>();

            // Select current value
            if (((int)lhs_heap.peek()[lhs_column_index]) >= ((int)rhs_heap.peek()[rhs_column_index])) {

                current_value = lhs_heap.peek()[lhs_column_index];

                // skip ahead until we find rows that are equal or higher
                while(rhs_heap.has_next() && ((int)rhs_heap.peek()[rhs_column_index]) < current_value) {
                    rhs_heap.pop();
                }

            } else {
                current_value = rhs_heap.peek()[rhs_column_index];

                // skip ahead until we find rows that are equal or higher
                while(lhs_heap.has_next() && ((int)lhs_heap.peek()[lhs_column_index]) < current_value) {
                    lhs_heap.pop();
                }
            }

            // Buffer all values that are equal to the current value
            while (lhs_heap.has_next() && ((int)lhs_heap.peek()[lhs_column_index]) == current_value) {
                lhs_buffer.push_back(lhs_heap.pop());
            }

            while (rhs_heap.has_next() && ((int)rhs_heap.peek()[rhs_column_index]) == current_value) {
                rhs_buffer.push_back(rhs_heap.pop());
            }
        }
    }

    std::tuple<std::vector<mysqlx::Row>, std::vector<mysqlx::Row>> fetchNextMatches() {

        // empty buffers
        lhs_buffer = std::vector<mysqlx::Row>();
        rhs_buffer = std::vector<mysqlx::Row>();

        buffer_next_value_candidates();
        return std::make_tuple(lhs_buffer, rhs_buffer);
    }
};

#endif  // LUNDGREN_K_WAY
