//--------------------------------------------------------------------------------------------------
// Ferrari Reachability Index
// (c) 2012 Stephan Seufert. Web site: http://www.mpi-inf.mpg.de/~sseufert
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
//--------------------------------------------------------------------------------------------------
#ifndef FERRARI_GRAPH_H_
#define FERRARI_GRAPH_H_
//--------------------------------------------------------------------------------------------------
#include <deque>
#include <map>
#include <string>
#include <vector>
#include <algorithm>
//--------------------------------------------------------------------------------------------------
class Graph {
private:
  unsigned n, m;
  std::vector<std::vector<unsigned> > nb; // successors
  std::vector<std::vector<unsigned> > pd; // predecessors
  std::vector<unsigned> deg;              // number of outgoing edges
  std::vector<unsigned> indeg;            // number of incoming edges
  std::deque<unsigned> leaves;            // leaf nodes
  std::vector<unsigned> roots;            // root nodes
public:
  Graph(const std::string& filename);
  Graph(const std::vector<std::pair<unsigned,unsigned> >& edge_list,unsigned nodes);
  ~Graph();

  const std::vector<unsigned>* get_neighbors(unsigned node) const;

  std::vector<unsigned>* get_roots();

  inline std::deque<unsigned>* get_leaves() {
    return &leaves;
  }

  inline std::vector<unsigned>* get_degrees() {
    return &deg;
  }

  inline std::vector<unsigned>* get_indegrees() {
    return &indeg;
  }

  const inline std::vector<unsigned>* get_predecessors(unsigned v) const {
    return &pd[v];
  }

  inline unsigned num_nodes() const {
    return n;
  }
};
//--------------------------------------------------------------------------------------------------
#endif /* GRAPH_H_ */
