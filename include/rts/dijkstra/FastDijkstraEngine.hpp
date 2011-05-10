#ifndef H_rts_dijkstra_FastDijkstraEngine
#define H_rts_dijkstra_FastDijkstraEngine
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
#include "rts/dijkstra/DijkstraEngine.hpp"
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
class FastDijkstraEngine: public DijkstraEngine {
public:
	/// Dijkstra's shortest path tree
	std::map<unsigned,std::set<unsigned> > tree;
	std::map<unsigned,std::set<unsigned> > parentsURI;

	/// Dijkstra's init
	void init(unsigned source);

	void getNeighbors();

	void updateNeighbors(unsigned node,unsigned nodeIndex);

	struct Neighbors{
		std::vector<std::vector<std::pair<unsigned,unsigned> > > connections;
		std::vector<unsigned> ids;
	};

	Neighbors n;
	std::set<unsigned> curNodes;
	std::set<unsigned>::iterator curnodes_iter;
	unsigned curIndex;

	std::set<unsigned> URI;

	/// do we need to estimate selectivity?
	bool needSelectivity;

	void precomputeSelectivity();

public:
	/// Constructor
	FastDijkstraEngine(Database& db,Database::DataOrder order,bool needSelectivity): DijkstraEngine(db, order), needSelectivity(needSelectivity){}

	void computeSP(unsigned source);

	void countSelectivity(unsigned root);

};
#endif
