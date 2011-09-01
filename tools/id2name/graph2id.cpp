#include "rts/database/Database.hpp"
#include "rts/segment/DictionarySegment.hpp"
#include "rts/segment/FactsSegment.hpp"
#include "rts/operator/AggregatedIndexScan.hpp"
#include "rts/operator/IndexScan.hpp"
#include "rts/operator/MergeJoin.hpp"
#include "rts/operator/PlanPrinter.hpp"
#include "rts/runtime/Runtime.hpp"
#include "rts/segment/AggregatedFactsSegment.hpp"
#include "rts/segment/FullyAggregatedFactsSegment.hpp"
#include <iostream>
#include <string>
#include <sstream>
#include <fstream>
#include <vector>
#include <stdlib.h>
#include <algorithm>
#include <set>
#include <list>
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
// get neighbors of the node
void getAdjacent(unsigned n, set<unsigned>& nodes, Database& db){
	FactsSegment::Scan scanSPO, scanOPS;
	if (scanSPO.first(db.getFacts(Database::Order_Subject_Predicate_Object),n,0,0)) do{
		if (scanSPO.getValue1()!=n)
			break;
		if (!scanSPO.getValue2())
			continue;
		nodes.insert(scanSPO.getValue3());
	} while (scanSPO.next());

	if (scanOPS.first(db.getFacts(Database::Order_Object_Predicate_Subject),n,0,0)) do{
		if (scanOPS.getValue1()!=n)
			break;
		if (!scanOPS.getValue2())
			continue;
		nodes.insert(scanOPS.getValue3());
	} while (scanOPS.next());

}
//---------------------------------------------------------------------------
/// get the list of internal id triples in the METIS format
int main(int argc,char* argv[]){
	if ((argc<2)) {
	    cout << "usage: " << argv[0] << " <database> " << endl;
	    return 1;
	}

    // Open the database
	Database db;
	if (!db.open(argv[1])) {
	   cout << "unable to open " << argv[1] << endl;
	   return 1;
	}

	ostringstream os;
	os<<argv[1]<<"_notype_id.txt";
	cerr<<os.str()<<endl;
	ofstream out(os.str().c_str());

	FactsSegment::Scan scanSPO, scanPSO, scanOPS;

	unsigned numOfEdges=0;
	set<unsigned> allNodes;
	unsigned maxPredicateID=0;
	// count nodes, in both orders (SPO and OPS)
	if (scanSPO.first(db.getFacts(Database::Order_Subject_Predicate_Object),0,0,0)) do{
		if (scanSPO.getValue1()% 10000==0)
			cerr<<scanSPO.getValue1()<<endl;
		allNodes.insert(scanSPO.getValue1());
		allNodes.insert(scanSPO.getValue3());
		if (scanSPO.getValue2() > maxPredicateID)
			maxPredicateID=scanSPO.getValue2();
	} while (scanSPO.next());

	if (scanOPS.first(db.getFacts(Database::Order_Object_Predicate_Subject),0,0,0)) do{
		if (scanOPS.getValue1()% 10000==0)
			cerr<<scanOPS.getValue1()<<endl;
		allNodes.insert(scanOPS.getValue1());
		allNodes.insert(scanOPS.getValue3());
	} while (scanOPS.next());

	{
		// count edges
		AggregatedFactsSegment::Scan scanSO;
		FactsSegment::Scan edgeScan;
		unsigned prevSubj=0, prevObj=0;
		set<pair<unsigned,unsigned> > visited;
		if (edgeScan.first(db.getFacts(Database::Order_Subject_Object_Predicate),0,0,0)) do{
			pair<unsigned, unsigned> p(edgeScan.getValue1(),edgeScan.getValue2());
			pair<unsigned, unsigned> revp(edgeScan.getValue2(),edgeScan.getValue1());
			if (!edgeScan.getValue3())
				continue;
			if (visited.count(p) || visited.count(revp))
				continue;
			if (edgeScan.getValue1()<=maxPredicateID||edgeScan.getValue2()<=maxPredicateID)
				continue;

			if (prevSubj==edgeScan.getValue1() && prevObj==edgeScan.getValue2())
				continue;

			prevSubj=edgeScan.getValue1();
			prevObj=edgeScan.getValue2();
			visited.insert(p);
			visited.insert(revp);
			numOfEdges++;
		} while (edgeScan.next());
		visited.clear();
	}


	cerr<<"number of edges: "<<numOfEdges<<endl;
	cerr<<"max pred id:"<<maxPredicateID<<endl;
	cerr<<"number of nodes: "<<allNodes.size()-maxPredicateID<<" max element: "<<*(max_element(allNodes.begin(), allNodes.end()))<<endl;
	unsigned maxNodeID=*(max_element(allNodes.begin(), allNodes.end()));

	// put the list of adjacent nodes
	out<<maxNodeID-maxPredicateID<<" "<<numOfEdges<<endl;
	map<unsigned, unsigned> id2Newid;
	map<unsigned, list<unsigned> > oldLists;

	unsigned curNode=0;
	for (set<unsigned>::iterator it=allNodes.begin(); it != allNodes.end(); it++){
		if (*it<=maxPredicateID)
			continue;

		curNode=*it;
		if (*it % 100000==0)
			cerr<<*it<<endl;
		set<unsigned> neighbors;
		getAdjacent(*it,neighbors,db);

		for (set<unsigned>::iterator neighbors_it=neighbors.begin(); neighbors_it != neighbors.end(); neighbors_it++){
			if (*neighbors_it <= maxPredicateID)
				continue;
			out<<((*neighbors_it)-maxPredicateID)<<" ";
		}
		out<<endl;
	}

	out.flush();
}
