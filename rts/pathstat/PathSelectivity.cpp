#include "rts/database/Database.hpp"
#include "rts/pathstat/PathSelectivity.hpp"
#include "rts/segment/DictionarySegment.hpp"
#include "rts/segment/FactsSegment.hpp"
#include "rts/dijkstra/FastDijkstraEngine.hpp"
#include "rts/operator/AggregatedIndexScan.hpp"
#include "rts/operator/IndexScan.hpp"
#include "rts/operator/MergeJoin.hpp"
#include "rts/operator/PlanPrinter.hpp"
#include "rts/runtime/Runtime.hpp"
#include "rts/segment/PathSelectivitySegment.hpp"
#include "rts/segment/AggregatedFactsSegment.hpp"
#include "rts/segment/FullyAggregatedFactsSegment.hpp"
#include <iostream>
#include "infra/osdep/Timestamp.hpp"
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
using namespace std;
//---------------------------------------------------------------------------
#define ensure(x) if (!(x)) assert(false)
//---------------------------------------------------------------------------
static void clusterPredicates(set<unsigned>& toURI, set<unsigned>& toLiterals, Database& db){
	FactsSegment::Scan scan;
	unsigned predicate=0;
	bool decided=false;
	if (scan.first(db.getFacts(Database::Order_Predicate_Subject_Object),0,0,0)) do {
		if (!decided){
			assert(predicate==scan.getValue1());
			unsigned o=scan.getValue3();
			const char* start=0,*stop=0; Type::ID type; unsigned subType;
			db.getDictionary().lookupById(o,start,stop,type,subType);
			if (type==Type::Literal)
				toLiterals.insert(scan.getValue1());
			else
				toURI.insert(scan.getValue1());
			decided=true;
		}
		if (decided&&predicate!=scan.getValue1()){
			// new predicate
			decided=false;
			predicate=scan.getValue1();
		}
	} while (scan.next());
}
//---------------------------------------------------------------------------
static void getURILeaves(const set<unsigned>& URIPredicates, set<unsigned>& leaves, Database& db){
	FactsSegment::Scan scan;
	unsigned start=0;
	FactsSegment::Scan scanfirst;
	scanfirst.first(db.getFacts(Database::Order_Subject_Predicate_Object),0,0,0);
	start=scanfirst.getValue1();
	set<unsigned> literalObjects;
	bool onlyLiteral=true;
	set<unsigned> pred;
	if (scan.first(db.getFacts(Database::Order_Subject_Predicate_Object),0,0,0)) do {
		if (scan.getValue1()==start){
			if (URIPredicates.count(scan.getValue2())){
				onlyLiteral=false;
			}
		}
		else {
			if (onlyLiteral){
				leaves.insert(start);
			}
			pred.clear();
			onlyLiteral=true;
			start=scan.getValue1();
			if (URIPredicates.count(scan.getValue2())){
				onlyLiteral=false;
			}
		}
	} while (scan.next());
	if (onlyLiteral)
		leaves.insert(start);
}
//---------------------------------------------------------------------------
/// compute selectivity of path scan for two directions: backward (following reversed links), forward
void PathSelectivity::computeSelectivity(Database& db, vector<unsigned>& back_selectivity, vector<unsigned>& forw_selectivity){

	set<unsigned> URI;
	set<unsigned> literals;

	clusterPredicates(URI,literals, db);
	set<unsigned> leaves;
	getURILeaves(URI, leaves, db);

	cerr<<"#leaves: "<<leaves.size()<<endl;
	FastDijkstraEngine scan(db, Database::Order_Object_Predicate_Subject, true);

	scan.precomputeSelectivity();

	for (set<unsigned>::iterator it=leaves.begin(); it != leaves.end(); it++){
		cerr<<"node: "<<*it<<endl;
		scan.computeSP(*it);
		cerr<<"map size: "<<scan.getSPMap().size()<<endl;
		scan.countSelectivity(*it);
	}
    forw_selectivity=scan.getBackwardSelectivity();
	back_selectivity=scan.getSelectivity();
}
//---------------------------------------------------------------------------
