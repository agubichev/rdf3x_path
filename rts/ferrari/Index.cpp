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
#include "rts/ferrari/Index.h"
//--------------------------------------------------------------------------------------------------
#include <assert.h>
#include <iostream>
#include <math.h>
#include <set>
#include <string.h>
#include <queue>
//--------------------------------------------------------------------------------------------------
Index::Index(Graph* g, unsigned s, unsigned k, bool global) :
    g(g), n_(g->num_nodes()), s_(s), k_(k), global_(global), queryId(0), expanded(
        0) {
}
//--------------------------------------------------------------------------------------------------
Index::~Index() {
  for (unsigned i = 0; i < n_; ++i) {
    if (intervals[i]) {
      delete intervals[i];
      intervals[i] = 0;
    }
  }
}
//--------------------------------------------------------------------------------------------------
void Index::build() {
  visited = std::vector<unsigned>(n_, 0);
  expanded = 0;

  // determine seeds
  if (s_) {
    seed_nodes = std::vector<unsigned>();
    is_seed = std::vector<bool>(n_, false);
    generate_seeds_degree(s_);

    // initialize bitmaps for seed nodes
    seed_in = std::vector<bitset>(n_, bitset(s_));
    seed_out = std::vector<bitset>(n_, bitset(s_));
    for (unsigned i = 0; i < s_; ++i) {
      seed_in[seed_nodes[i]][i] = 1;
      seed_out[seed_nodes[i]][i] = 1;
    }
  }

  // filters
  tlevel_ = std::vector<unsigned>(n_, 0);
  torder_ = std::vector<unsigned>(n_, ~0u);

  // reachable ids
  id_ = std::vector<unsigned>(n_, ~0u);

  // topological ordering
  ordered_nodes_ = std::vector<unsigned>();

  // parent in tree
  parent_ = std::vector<unsigned>(n_, ~0u);

  // create index
  visit_fw();
  visit_bw();

  // intervals
  intervals = std::vector<IntervalList*>(n_, 0);

  if (global_)
    assign_sketches_global();
  else
    assign_sketches();
}
//--------------------------------------------------------------------------------------------------
bool Index::reachable_dfs(unsigned x, unsigned y) {
  ++queryId;
  return __reachable_dfs(x, y);
}
//--------------------------------------------------------------------------------------------------
bool Index::__reachable_dfs(unsigned x, unsigned y) {
  if (x == y)
    return true;
  if (visited[x] == queryId)
    return false;
  visited[x] = queryId;
  ++expanded;
  const std::vector<unsigned> *nb = g->get_neighbors(x);
  for (std::vector<unsigned>::const_iterator it = nb->begin(); it != nb->end();
      ++it) {
    if (__reachable_dfs(*it, y)) {
      return true;
    }
  }
  return false;
}
//--------------------------------------------------------------------------------------------------
bool Index::reachable_bfs(unsigned x, unsigned y) {
  if (x == y)
    return true;
  ++queryId;
  std::deque<unsigned> queue(1, x);
  unsigned v;
  const std::vector<unsigned> *nb;
  while (!queue.empty()) {
    v = queue.front();
    queue.pop_front();
    if (visited[v] == queryId)
      continue;
    visited[v] = queryId;
    ++expanded;
    nb = g->get_neighbors(v);
    for (std::vector<unsigned>::const_iterator it = nb->begin();
        it != nb->end(); ++it) {
      if (y == *it)
        return true;
      queue.push_back(*it);
    }
  }
  return false;
}
//--------------------------------------------------------------------------------------------------
bool Index::reachable(unsigned x, unsigned y) {
  ++queryId;

  x=g->getNodeId(x);
  y=g->getNodeId(y);
  if (!intervals[x]) {
    return x == y;
  }

  if (s_) {
    if (seed_out[x].intersects(seed_in[y])) {
      return true;
    }
  }
  if (x == y) {
    return true;
  }
  if (tlevel_[x] <= tlevel_[y] || torder_[x] > torder_[y]) {
    return false;
  }

  switch (intervals[x]->contains(id_[y])) {
  case IntervalList::NOT:
    return false;
  case IntervalList::YES:
    return true;
  default:
    const std::vector<unsigned> *nb = g->get_neighbors(x);
    for (std::vector<unsigned>::const_iterator it = nb->begin();
        it != nb->end(); ++it) {
      if (y == *it
          || (visited[*it] < queryId && __reachable(*it, y))) {
        return true;
      }
    }
    return false;
  }
}
//--------------------------------------------------------------------------------------------------
bool Index::__reachable(const unsigned& x, const unsigned& y) {
  visited[x] = queryId;
  ++expanded;

  if (tlevel_[x] <= tlevel_[y] || torder_[x] > torder_[y]) {
    return false;
  }

  if (x == y) {
    return true;
  }
  if (!g->get_neighbors(x)) {
    return false;
  }

  switch (intervals[x]->contains(id_[y])) {
  case IntervalList::NOT:
    return false;
  case IntervalList::YES:
    return true;
  default:
    const std::vector<unsigned> *nb = g->get_neighbors(x);
    for (std::vector<unsigned>::const_iterator it = nb->begin();
        it != nb->end(); ++it) {
      if (*it == y
          || (visited[*it] < queryId && __reachable(*it, y))) {
        return true;
      }
    }
    return false;
  }
}
//--------------------------------------------------------------------------------------------------
void Index::visit_bw() {
  std::deque<unsigned> leaves = *(g->get_leaves());
  std::vector<unsigned> deg = *(g->get_degrees());
  const std::vector<unsigned> *pd;
  unsigned v;
  while (!leaves.empty()) {
    v = leaves.front();
    leaves.pop_front();
    pd = g->get_predecessors(v);
    for (std::vector<unsigned>::const_iterator it = pd->begin();
        it != pd->end(); ++it) {
      tlevel_[*it] = std::max(tlevel_[*it], 1 + tlevel_[v]);
      if (parent_[v] == ~0u || torder_[parent_[v]] < torder_[*it]) {
        parent_[v] = *it;
      }
      if (s_)
        seed_out[*it] |= seed_out[v];
      --deg[*it];
      if (!deg[*it]) {
        leaves.push_back(*it);
      }
    }
  }
}
//--------------------------------------------------------------------------------------------------
void Index::visit_fw() {
  std::vector<unsigned> *roots = g->get_roots();
  std::queue<unsigned> queue;
  for (std::vector<unsigned>::const_iterator it = roots->begin();
      it != roots->end(); ++it)
    queue.push(*it);
  std::vector<unsigned> indeg = *(g->get_indegrees());
  const std::vector<unsigned> *nb;
  unsigned v, to = 0;
  while (!queue.empty()) {
    v = queue.front();
    queue.pop();
    torder_[v] = to++;
    ordered_nodes_.push_back(v);
    nb = g->get_neighbors(v);
    for (std::vector<unsigned>::const_iterator it = nb->begin();
        it != nb->end(); ++it) {
      if (s_)
        seed_in[*it] |= seed_in[v];
      --indeg[*it];
      if (!indeg[*it]) {
        queue.push(*it);
      }
    }
  }
}
//--------------------------------------------------------------------------------------------------
void Index::assign_sketches() {
  // assign intervals based on tree cover specified by parent_
  // traverse dfs
  std::vector<unsigned> stack(*(g->get_roots()));
  unsigned n = g->num_nodes();
  std::vector<unsigned> mid = std::vector<unsigned>(n, ~0u);
  std::vector<unsigned> next_child(n, 0);
  unsigned _id = 0, v, c;
  std::vector<unsigned> const *nb;
  while (!stack.empty()) {
    v = stack.back();
    nb = g->get_neighbors(v);
    if (mid[v] < ~0u && next_child[v] >= nb->size()) {
      stack.pop_back();
      id_[v] = _id++;
      intervals[v] = new IntervalList(mid[v], id_[v]);
    } else {
      mid[v] = _id;
      if (next_child[v] < nb->size()) {
        c = nb->at(next_child[v]++);
        if (parent_[c] == v) {
          stack.push_back(c);
        } else {
        }
      }
    }
  }
  for (std::vector<unsigned>::const_reverse_iterator it =
      ordered_nodes_.rbegin(); it != ordered_nodes_.rend(); ++it) {
    nb = g->get_neighbors(*it);
    for (std::vector<unsigned>::const_iterator nbit = nb->begin();
        nbit != nb->end(); ++nbit) {
      if (!(parent_[*nbit] == *it) && (mid[*it] <= id_[*nbit])
          && (id_[*nbit] < id_[*it])) {
        // forward edge
      } else {
        intervals[*it]->merge(*intervals[*nbit]);
      }
    }
    intervals[*it]->restrict(k_);
  }

  for (unsigned i = 0; i < g->num_nodes(); ++i) {
    nb = g->get_neighbors(i);
    if (!nb->size()) { // leaf node
      delete intervals[i];
      intervals[i] = 0;
    }
  }
}
//--------------------------------------------------------------------------------------------------
void Index::assign_sketches_global() {
  // assign intervals based on tree cover specified by parent_
  // traverse dfs
  std::vector<unsigned> stack(*(g->get_roots()));
  unsigned n = g->num_nodes();
  std::vector<unsigned> mid = std::vector<unsigned>(n, ~0u);
  std::vector<unsigned> next_child(n, 0);
  unsigned _id = 0, v, c;
  std::vector<unsigned> const *nb;
  while (!stack.empty()) {
    v = stack.back();
    nb = g->get_neighbors(v);
    if (mid[v] < ~0u && next_child[v] >= nb->size()) {
      stack.pop_back();
      id_[v] = _id++;
      intervals[v] = new IntervalList(mid[v], id_[v]);
    } else {
      mid[v] = _id;
      if (next_child[v] < nb->size()) {
        c = nb->at(next_child[v]++);
        if (parent_[c] == v) {
          stack.push_back(c);
        } else {
        }
      }
    }
  }

  unsigned multiplier = 4;
  unsigned leaf_count = g->get_leaves()->size();
  unsigned budget = k_ * (n + leaf_count) / n;
  unsigned max_space = n * k_ + leaf_count;
  unsigned current_space = 0;
  std::vector<unsigned> restriction_queue;
  restriction_queue.reserve(n);
  std::vector<unsigned>* deg = g->get_degrees();
  degreecompare comp(deg);

  for (std::vector<unsigned>::const_reverse_iterator it =
      ordered_nodes_.rbegin(); it != ordered_nodes_.rend(); ++it) {
    nb = g->get_neighbors(*it);
    for (std::vector<unsigned>::const_iterator nbit = nb->begin();
        nbit != nb->end(); ++nbit) {
      if (!(parent_[*nbit] == *it) && (mid[*it] <= id_[*nbit])
          && (id_[*nbit] < id_[*it])) {
        // forward edge
      } else {
        intervals[*it]->merge(*intervals[*nbit]);
      }
    }
    intervals[*it]->restrict(multiplier * budget);
    current_space += intervals[*it]->size();
    unsigned v;
    while (current_space > max_space) {
      if (!restriction_queue.empty()) {
        std::pop_heap(restriction_queue.begin(), restriction_queue.end(), comp);
        v = restriction_queue.back();
        restriction_queue.pop_back();
        current_space -= (intervals[v]->size() - budget);
        intervals[v]->restrict(budget);
      } else {
        if (intervals[*it]->size() > budget) {
          current_space -= intervals[*it]->size() - budget;
          intervals[*it]->restrict(budget);
        }
      }
    }
    if (intervals[*it]->size() > budget) {
      restriction_queue.push_back(*it);
      std::push_heap(restriction_queue.begin(), restriction_queue.end(), comp);
    }
  }

  for (unsigned i = 0; i < g->num_nodes(); ++i) {
    nb = g->get_neighbors(i);
    if (!nb->size()) { // leaf node
      delete intervals[i];
      intervals[i] = 0;
    }
  }
}
//--------------------------------------------------------------------------------------------------
void Index::generate_seeds_random(unsigned s) {
  std::srand(std::time(0));
  std::set<unsigned> seed_set;
  unsigned i, n = g->num_nodes();
  while (seed_set.size() < s) {
    i = std::rand() % n + 1;
    if (g->get_indegrees()->at(i) && g->get_degrees()->at(i)) {
      seed_set.insert(i);
      is_seed[i] = true;
    }
  }
  std::copy(seed_set.begin(), seed_set.end(), std::back_inserter(seed_nodes));
}
//--------------------------------------------------------------------------------------------------
void Index::generate_seeds_degree(unsigned s) {
  // select nodes with maximum degree as seeds
  std::vector<unsigned>* deg = g->get_degrees();
  //std::vector<unsigned>* indeg = g->get_indegrees();
  degreecompare comp(deg);
  //combined_degreecompare comp(deg, indeg);
  for (unsigned i = 0; i < g->num_nodes(); ++i) {
    if (seed_nodes.size() < s && deg->at(i) > 1) {
      seed_nodes.push_back(i);
      std::push_heap(seed_nodes.begin(), seed_nodes.end(), comp);
    } else {
      if (/*deg->at(seed_nodes.front()) * */deg->at(i) > 1
          && deg->at(seed_nodes.front()) < /*deg->at(i) * */deg->at(i)) {
        std::pop_heap(seed_nodes.begin(), seed_nodes.end(), comp);
        seed_nodes.pop_back();
        seed_nodes.push_back(i);
        std::push_heap(seed_nodes.begin(), seed_nodes.end(), comp);
      }
    }
  }
  for (std::vector<unsigned>::const_iterator it = seed_nodes.begin();
      it != seed_nodes.end(); ++it) {
    is_seed[*it] = true;
  }
  s_ = std::min((unsigned) seed_nodes.size(), s_);
}
//--------------------------------------------------------------------------------------------------
unsigned Index::reset() {
  unsigned _expanded = expanded;
  expanded = 0;
  queryId = 0;
  memset(&visited[0], 0, sizeof(visited[0]) * visited.size());
  return _expanded;
}
//--------------------------------------------------------------------------------------------------
bool Index::path(unsigned x, unsigned y, std::vector<unsigned>* p) {
  if (x == y) {
    p->push_back(x);
    return true;
  } else {
    if (!reachable_dfs(x, y)) {
      return false;
    } else {
      p->push_back(x);
      const std::vector<unsigned> *nb = g->get_neighbors(x);
      for (std::vector<unsigned>::const_iterator it = nb->begin();
          it != nb->end(); ++it) {
        if (reachable_dfs(*it, y)) {
          return path(*it, y, p);
        }
      }
    }
  }
  return true;
}
//--------------------------------------------------------------------------------------------------
