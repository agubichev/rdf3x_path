#ifndef H_rts_dijkstra_DijkstraEngine
#define H_rts_dijkstra_DijkstraEngine
//---------------------------------------------------------------------------
// RDF-3X
// (c) 2008 Thomas Neumann. Web site: http://www.mpi-inf.mpg.de/~neumann/rdf3x
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
//---------------------------------------------------------------------------
#include "rts/segment/Segment.hpp"
#include "rts/database/Database.hpp"
#include <vector>
#include <string>
#include <sstream>
#include <fstream>
#include <vector>
#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include <set>
#include <map>
#include <stack>
#include <assert.h>
#include <math.h>
#include <algorithm>
//---------------------------------------------------------------------------
class DijkstraEngine {
public:
	/// comparing nodes by distance to source
	template <typename T> struct CompareByDistance {
	    bool operator()(const std::pair<T, T> &lhs, const std::pair<T, T> &rhs) {
	        if (lhs.first == rhs.first)
	            return lhs.second < rhs.second;
	        return lhs.first < rhs.first;
	    }
	};

protected:
	/// database to be scanned
	Database& db;
	/// order of the search - backwards/forwards
	Database::DataOrder order;
	/// approximate forward selectivity of path scans
	std::vector<unsigned> selectivity;
	/// approximate backward selectivity of path scans
	std::vector<unsigned> backward_selectivity;
	/// Dijkstra's shortest distances
	std::map<unsigned,unsigned> shortestDistances;
	/// Dijkstra's settled nodes
	std::set<unsigned> settledNodes;
	/// Dijkstra's working set
	std::set<std::pair<unsigned, unsigned>,CompareByDistance<unsigned> > workingSet;

	/// is the node handled already?
	bool isHandled(unsigned node) {return (settledNodes.find(node)!=settledNodes.end());}
	/// get the shortest distance from the start to the node
	unsigned getShortestDist(unsigned node) {return shortestDistances.count(node)?shortestDistances[node]:~0u;}

public:
	/// Constructor
	DijkstraEngine(Database& db,Database::DataOrder order): db(db), order(order){}

	virtual void computeSP(unsigned source) = 0;

	std::map<unsigned,unsigned>& getSPMap(){return shortestDistances;}

	virtual void countSelectivity(unsigned root)=0;

	std::vector<unsigned>& getBackwardSelectivity(){return backward_selectivity;}

	std::vector<unsigned>& getSelectivity(){return selectivity;}

};
#endif
