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
#include "infra/osdep/Timestamp.hpp"
#include <string>
#include <sstream>
#include <fstream>
#include <vector>
#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include <set>
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
static unsigned numberOfNodes(Database& db){
	// Collect all nodes
	set<unsigned> allNodes;
	{
	   FactsSegment::Scan scan;
	   if (scan.first(db.getFacts(Database::Order_Subject_Predicate_Object),0,0,0)) do {
	      allNodes.insert(scan.getValue1());
	      allNodes.insert(scan.getValue3());
	   } while (scan.next());
	}

	cerr<<"nodes: "<<allNodes.size()<<endl;
	return allNodes.size();
}
//---------------------------------------------------------------------------
static string lookupById(Database& db,unsigned id)
   // Lookup a string id
{
   const char* start=0,*stop=0; Type::ID type; unsigned subType;
   db.getDictionary().lookupById(id,start,stop,type,subType);
   return string(start,stop);
}
//---------------------------------------------------------------------------
static void randomLookups(Database& db){
	srand(time(0));
	unsigned dbSize=numberOfNodes(db);
	Timestamp t1;
	for (unsigned i=0; i < 4500; i++){
		if (i%1000==0)
			cerr<<"i "<<i<<endl;
		unsigned id=rand()%dbSize;
		const char* start=0,*stop=0; Type::ID type; unsigned subType;
		db.getDictionary().lookupById(id,start,stop,type,subType);
	}
	Timestamp t2;
	cerr<<"time: "<<t2-t1<<" ms"<<endl;
}
//---------------------------------------------------------------------------
/// lookup the name for internal id
int main(int argc,char* argv[]){
	if ((argc<3)) {
	    cout << "usage: " << argv[0] << " <database> <id>" << endl;
	    return 1;
	}

    // Open the database
	Database db;
	if (!db.open(argv[1])) {
	   cout << "unable to open " << argv[1] << endl;
	   return 1;
	}

	cout<<"name: "<<lookupById(db, atoi(argv[2]))<<endl;

	randomLookups(db);
}
