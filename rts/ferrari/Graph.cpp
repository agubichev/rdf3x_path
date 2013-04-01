/**
 * Ferrari Reachability Index<br/>
 * (c) 2012 Stephan Seufert. Web site: http://www.mpi-inf.mpg.de/~sseufert<br/>
 * This work is licensed under the Creative Commons  Attribution-Noncommercial-Share Alike 3.0
 * Unported License. To view a copy of this license, visit
 * http://creativecommons.org/licenses/by-nc-sa/3.0/ or send a letter to Creative Commons,
 * 171 Second Street, Suite 300, San Francisco, California, 94105, USA.
 *
 * @file Graph.cpp
 * @author Stephan Seufert
 * @date 2012/10/31
 */
//--------------------------------------------------------------------------------------------------
#include "rts/ferrari/Graph.h"
#include <assert.h>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stack>
#include <map>
#include <set>
#include <string.h>
#include "infra/osdep/Timestamp.hpp"
using namespace std;
//---------------------------------------------------------------------------
template <class T> void eliminateDuplicates(vector<T>& data)
   // Eliminate duplicates in a sorted list
{
   data.resize(unique(data.begin(),data.end())-data.begin());
}

//--------------------------------------------------------------------------------------------------
/**
 * Constructor
 * @param filename file containing adjacency lists
 */
//--------------------------------------------------------------------------------------------------
Graph::Graph(const std::string& filename) :
    n(0), m(0) {
  std::string line;
  std::ifstream stream(filename.c_str());
  if (!stream.eof()) {
    getline(stream, line);
    std::istringstream iss(line, std::istringstream::in);
    iss >> n >> m;
    nb = std::vector<std::vector<unsigned> >(n);
    pd = std::vector<std::vector<unsigned> >(n);
    deg = std::vector<unsigned>(n, 0);
    for (unsigned i = 0; i < n; ++i)
      deg[i] = 0;
    indeg = std::vector<unsigned>(n, 0);
    leaves = std::deque<unsigned>();
    unsigned s, t, d;
    std::vector<bool> is_root(n, true);
    getline(stream, line);
    while (stream.good()) {
      iss.clear();
      iss.str(line);
      iss >> s;
      d = 0;
      while (iss >> t) {
        nb[s].push_back(t);
        pd[t].push_back(s);
        ++d;
        ++indeg[t];
        is_root[t] = false;
      }
      assert(s < n);
      deg[s] = d;
      getline(stream, line);
    }
    for (unsigned i = 0; i < n; ++i) {
      if (!deg[i]) {
        leaves.push_back(i);
      }
      if (is_root[i]) {
        roots.push_back(i);
      }
    }
  }
}
//--------------------------------------------------------------------------------------------------
// Tarjan's algorithm for finding the SCC
static void strongcomponent(unsigned v, map<unsigned, std::vector<unsigned> >& adjlist,
		map<unsigned,unsigned>& visited,
		map<unsigned,unsigned>& lowlink,
		vector<unsigned>& stack,
		set<unsigned>& setnodes,
		map<unsigned,unsigned>& scc,
		unsigned& index,unsigned& sccId)
{
	visited[v]=index;
	lowlink[v]=index;
	++index;
	stack.push_back(v);
	setnodes.insert(v);
   vector<unsigned>& neighbors = adjlist[v];
	for (auto node: neighbors){
		if (!visited.count(node)){
			strongcomponent(node,adjlist,visited,lowlink,stack,setnodes,scc,index,sccId);
			lowlink[v]=std::min(lowlink[v],lowlink[node]);
		} else if (/*find(stack.begin(), stack.end(), node)!= stack.end()*/setnodes.count(node)){
			lowlink[v]=std::min(lowlink[v],visited[node]);
		}
	}

	if (lowlink[v]==visited[v]){
		//cerr<<"SCC: ";
		//cerr<<" root "<<v<<" ("<<visited[v]<<", "<<sccId<<") ";
		scc[v]=sccId;

		unsigned node=0;
		while (true) {
			node=stack.back();
		//	cerr<<node<<" ("<<visited[node]<<", "<<sccId<<")";
			stack.pop_back();
		//	cerr<<" ["<<setnodes.size()<<"] ";
			setnodes.erase(node);
			scc[node]=sccId;
			if (node==v)
				break;
		};
		++sccId;
		//cerr<<endl;
	}

}
//--------------------------------------------------------------------------------------------------
Graph::Graph(const std::vector<std::pair<unsigned,unsigned> >& edge_list,unsigned nodes):  n(nodes), m(edge_list.size()) {
  // convert names to internal Ids
	unsigned id=0;
	map<unsigned,unsigned> visited,lowlink,scc;
	std::vector<std::pair<unsigned,unsigned> > edges;
	std::map<unsigned, std::vector<unsigned> > adjList;
	vector<unsigned> stack;
	set<unsigned> setnodes;
	unsigned sccId=0;
	// fill out the adjacency list
	for (auto edge: edge_list)
		adjList[edge.first].push_back(edge.second);

	for (auto edge: edge_list){
		if (!visited.count(edge.first)){
			strongcomponent(edge.first,adjList,visited,lowlink,stack,setnodes,scc,id,sccId);
		}
	}
	for (auto edge:edge_list){
		if (scc[edge.first]!=scc[edge.second]){
			edges.push_back({scc[edge.first],scc[edge.second]});
		}
	}

	sort(edges.begin(), edges.end());
	eliminateDuplicates(edges);

	n=sccId+1;
	cerr<<"   nodes: "<<n<<endl;
	cerr<<"   edges: "<<edges.size()<<endl;

	nb = std::vector<std::vector<unsigned> >(n);
   pd = std::vector<std::vector<unsigned> >(n);
   deg = std::vector<unsigned>(n, 0);
   for (unsigned i = 0; i < n; ++i)
     deg[i] = 0;
   indeg = std::vector<unsigned>(n, 0);
   leaves = std::deque<unsigned>();
   std::vector<bool> is_root(n, true);
   for (auto p: edges){
   	nb[p.first].push_back(p.second);
   	pd[p.second].push_back(p.first);
   	++indeg[p.second];
   	++deg[p.first];
      is_root[p.second] = false;
   }
   for (unsigned i = 0; i < n; ++i) {
      if (!deg[i]) {
        leaves.push_back(i);
      }
      if (is_root[i]) {
        roots.push_back(i);
      }
   }
}
//--------------------------------------------------------------------------------------------------
/**
 * Desctructor
 */
//--------------------------------------------------------------------------------------------------
Graph::~Graph() {
}
//--------------------------------------------------------------------------------------------------
/**
 * Accessor for adjacency lists
 *
 * @param v vertex
 * @return adjacency list (std::vector)
 */
//--------------------------------------------------------------------------------------------------
const std::vector<unsigned>* Graph::get_neighbors(unsigned v) const {
  assert(v<n);
  return &nb[v];
}
//--------------------------------------------------------------------------------------------------
/**
 * Accessor for root vertices
 *
 * @return roots of the graph (vertices with zero indegree)
 */
//--------------------------------------------------------------------------------------------------
std::vector<unsigned>* Graph::get_roots() {
  return &roots;
}
//--------------------------------------------------------------------------------------------------
