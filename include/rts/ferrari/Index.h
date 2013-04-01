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
#ifndef FERRARI_INDEX_H_
#define FERRARI_INDEX_H_
//--------------------------------------------------------------------------------------------------
#include "rts/ferrari/Graph.h"
#include "rts/ferrari/IntervalList.h"
//--------------------------------------------------------------------------------------------------
#include <set>
#include <vector>
//--------------------------------------------------------------------------------------------------
#include <boost/dynamic_bitset.hpp>
typedef boost::dynamic_bitset<> bitset;
//--------------------------------------------------------------------------------------------------
struct degreecompare {
  const std::vector<unsigned>* degrees;
  degreecompare(const std::vector<unsigned>* degrees) :
      degrees(degrees) {
  }
  bool operator()(const unsigned& a, const unsigned& b) {
    return degrees->at(a) >= degrees->at(b);
  }
};
//--------------------------------------------------------------------------------------------------
struct combined_degreecompare {
  const std::vector<unsigned>* degrees;
  const std::vector<unsigned>* indegrees;
  combined_degreecompare(const std::vector<unsigned>* degrees, 
                         const std::vector<unsigned>* indegrees) :
      degrees(degrees), indegrees(indegrees) {
  }
  bool operator()(const unsigned& a, const unsigned& b) {
    return degrees->at(a)*indegrees->at(a) >= degrees->at(b)*indegrees->at(b);
  }
};
//--------------------------------------------------------------------------------------------------
struct topological_order_less {
  const std::vector<unsigned>& to_;
  topological_order_less(const std::vector<unsigned>& to) : to_(to) {} 
  bool operator()(const unsigned& x, const unsigned& y) {
    return to_[x] > to_[y];
  }
};
//--------------------------------------------------------------------------------------------------
class Index {
private:
  Graph *g;

  // settings
  unsigned n_;  // number of nodes
  unsigned s_;  // seeds
  unsigned k_;  // size constraint
  bool global_; // global or local size restriction

  // seeds
  std::vector<unsigned> seed_nodes;
  std::vector<bool> is_seed;
  std::vector<bitset> seed_in;
  std::vector<bitset> seed_out;

  // reachable ids
  std::vector<unsigned> id_;

  // intervals
  std::vector<IntervalList*> intervals;

  // filters
  std::vector<unsigned> tlevel_;
  std::vector<unsigned> torder_;

  // topological order
  std::vector<unsigned> ordered_nodes_;

  // parent of node in resulting tree
  std::vector<unsigned> parent_;

  // query processing
  std::vector<unsigned> visited;
  unsigned queryId;
  unsigned expanded;

public:
  /// constructor
  Index(Graph *g, unsigned s, unsigned k = ~  0u, bool global = true);

  /// destructor
  ~Index();

  // index construction
  void build();
  inline const IntervalList* get_intervals(const unsigned& x) const { 
    return intervals[x]; 
  }

  // query processing
  bool reachable(unsigned x, unsigned y);
  bool reachable_dfs(unsigned x, unsigned y);
  bool reachable_bfs(unsigned x, unsigned y);
  unsigned reset();

  // helpers
  bool path(unsigned x, unsigned y, std::vector<unsigned>* p);

  // accessors
  inline const unsigned& get_id(const unsigned& x) const {
    return id_[x];
  }
  inline unsigned used_seed_count() const {
    return s_;
  }
  inline Graph* get_graph() const {
    return g;
  }
private:
  // seeds
  void generate_seeds_random(unsigned s);
  void generate_seeds_degree(unsigned s);

  // index construction
  void visit_bw();
  void visit_fw();
  void assign_sketches();
  void assign_sketches_global();

  // query processing
  bool __reachable_dfs(unsigned x, unsigned y);
  bool __reachable(const unsigned& x, const unsigned& y);
};
//--------------------------------------------------------------------------------------------------
#endif /* INDEX_H_ */
