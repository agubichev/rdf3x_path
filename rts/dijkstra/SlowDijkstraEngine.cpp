#include "rts/dijkstra/SlowDijkstraEngine.hpp"
#include "rts/database/Database.hpp"
#include "rts/segment/DictionarySegment.hpp"
#include "infra/osdep/Timestamp.hpp"
#include "rts/segment/FactsSegment.hpp"
#include "rts/operator/AggregatedIndexScan.hpp"
#include "rts/operator/IndexScan.hpp"
#include "rts/operator/MergeJoin.hpp"
#include "rts/runtime/Runtime.hpp"
#include "rts/segment/AggregatedFactsSegment.hpp"
#include "rts/segment/FullyAggregatedFactsSegment.hpp"
#include <iostream>
#include <vector>
#include <iosfwd>
#include <sstream>

#include <fstream>
#include <list>
#include <set>
#include <algorithm>
#include <cassert>
#include <cmath>
#include <queue>
#include <set>
#include <map>
#include <limits>
#include <climits>
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
static vector<pair<unsigned, unsigned> > findDestinations(unsigned node, Database& db) {
    vector<pair<unsigned, unsigned> > literals;

    FactsSegment::Scan scan;
    if (scan.first(db.getFacts(Database::Order_Subject_Predicate_Object), node, 0, 0)) do {
            if (scan.getValue1() != node) break;
            //  if (contains(allNodes, scan.getValue2())) continue;
            literals.push_back(pair<unsigned, unsigned>(scan.getValue2(), scan.getValue3()));
        } while (scan.next());

    return literals;
}
//---------------------------------------------------------------------------
static vector<pair<unsigned, unsigned> > findPredecessors(unsigned node, Database& db) {
    vector<pair<unsigned, unsigned> > literals;
    FactsSegment::Scan scan;
    if (scan.first(db.getFacts(Database::Order_Object_Predicate_Subject), node, 0, 0)) do {
            if (scan.getValue1() != node) break;
            literals.push_back(pair<unsigned, unsigned> (scan.getValue2(), scan.getValue3()));
        } while (scan.next());

    return literals;
}
//---------------------------------------------------------------------------
unsigned SlowDijkstraEngine::getPredecessor(unsigned node) {
    return predecessors[node];
}
//---------------------------------------------------------------------------
void SlowDijkstraEngine::setPredecessor(unsigned a, unsigned b) {
    predecessors[a] = b;
}
//---------------------------------------------------------------------------
void SlowDijkstraEngine::setShortestDistance(unsigned node, unsigned oldDistance, unsigned distance) {
    //    cout<<"               deleting node "<<node<<", dist = "<<oldDistance<<endl;
	workingSet.erase(pair<unsigned, unsigned>(oldDistance, node));

    shortestDistances[node] = distance;
    workingSet.insert(pair<unsigned, unsigned>(distance, node));

}
//---------------------------------------------------------------------------
void SlowDijkstraEngine::init(unsigned start) {
    settledNodes.clear();
    shortestDistances.clear();
    predecessors.clear();
    workingSet.clear();
    shortestDistances[start] = 0;

    workingSet.insert(pair<unsigned, unsigned>(0, start));
}
//---------------------------------------------------------------------------
void SlowDijkstraEngine::updateNeighbors(unsigned node) {

    //   cout << "Working with neighbors for the node " << db.getDictionary().mapId(node) << endl;
	Timestamp t1;
    vector<pair<unsigned, unsigned> > dest;
    if (order==Database::Order_Subject_Predicate_Object){
    	dest = findDestinations(node, db);
    }
    else
    	dest = findPredecessors(node,db);
    Timestamp t2;
//    cerr<<"time for getting neighbors: "<<(t2-t1)<<" ms, size: "<<dest.size()<<endl;
    for (vector<pair<unsigned, unsigned> >::const_iterator iter = dest.begin(), last = dest.end(); iter != last; iter++) {

        if (DijkstraEngine::isHandled(iter->second))
            continue;
        //       cout << "  cur node = " << db.getDictionary().mapId(iter->second) << endl;
        unsigned shortDist = DijkstraEngine::getShortestDist(node) + 1;

        unsigned oldDist = DijkstraEngine::getShortestDist(iter->second);
        if (shortDist < oldDist) {
            //         cout << "the distance was " << oldDist << ", updated to " << shortDist << endl;
            setShortestDistance(iter->second, oldDist, shortDist);
            setPredecessor(iter->second, node);
        }
    }
    Timestamp t3;
//    cerr<<"updating neighbors for "<<node<<" time: "<<(t3-t1)<<" ms"<<endl;
}
//---------------------------------------------------------------------------
void SlowDijkstraEngine::computeSP(unsigned source) {
	init(source);
    unsigned curNode = 0;
    unsigned maxHeapSize = 0;
    while (!workingSet.empty()) {
        curNode = (workingSet.begin())->second;
        if (workingSet.size() > maxHeapSize)
            maxHeapSize = workingSet.size();
        workingSet.erase(workingSet.begin());

        assert(!DijkstraEngine::isHandled(curNode));

        settledNodes.insert(curNode);
        updateNeighbors(curNode);

    }

//    cerr<<"settled nodes: "<<settledNodes.size()<<endl;
}
