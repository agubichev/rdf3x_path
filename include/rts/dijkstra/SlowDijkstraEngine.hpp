#ifndef H_rts_dijkstra_SlowDijkstraEngine
#define H_rts_dijkstra_SlowDijkstraEngine
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
class SlowDijkstraEngine: public DijkstraEngine {
private:
	/// map of predecessors
	std::map<unsigned,unsigned> predecessors;
	/// Dijkstra's init
	void init(unsigned source);
	unsigned getPredecessor(unsigned node);
	void setPredecessor(unsigned a, unsigned b);

	void setShortestDistance(unsigned node, unsigned oldDistance, unsigned distance);
	void updateNeighbors(unsigned node);
public:
	/// Constructor
	SlowDijkstraEngine(Database& db,Database::DataOrder order): DijkstraEngine(db, order){}

	void computeSP(unsigned source);

	void countSelectivity(unsigned /*root*/){};

};
#endif
