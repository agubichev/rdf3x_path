#include "rts/database/Database.hpp"
#include "rts/segment/PathSelectivitySegment.hpp"
#include "rts/dijkstra/DijkstraEngine.hpp"
#include "rts/segment/FactsSegment.hpp"
#include "infra/util/Type.hpp"
#include "rts/segment/DictionarySegment.hpp"
#include "infra/osdep/Timestamp.hpp"
#include <iostream>
#include <algorithm>
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
	      allNodes.insert(scan.getValue2());
	   } while (scan.next());
	}
	{
	   FactsSegment::Scan scan;
	   if (scan.first(db.getFacts(Database::Order_Object_Predicate_Subject),0,0,0)) do {
	      allNodes.insert(scan.getValue1());
	      allNodes.insert(scan.getValue3());
	   } while (scan.next());
	}

	cerr<<"nodes: "<<allNodes.size()<<endl;
	return allNodes.size();
}
//---------------------------------------------------------------------------
/// find the error of selectivity estimation for path scans
int main(int argc,char* argv[]){
	if ((argc<4)) {
	    cout << "usage: " << argv[0] << " <database> <direction> (0 for forward, 1 for backward) <number of tests>" << endl;
	    return 1;
	}

    // Open the database
	Database db;
	if (!db.open(argv[1])) {
	   cout << "unable to open " << argv[1] << endl;
	   return 1;
	}


	PathSelectivitySegment::Direction dir;
	Database::DataOrder order;
	if (atoi(argv[2])==0){
		dir=PathSelectivitySegment::Forward;
		order=Database::Order_Subject_Predicate_Object;
	}
	else if (atoi(argv[2])==1){
		dir=PathSelectivitySegment::Backward;
		order=Database::Order_Object_Predicate_Subject;
	}
	else {
		cerr<<"incorrect direction value: only 0 or 1 allowed."<<endl;
		return 1;
	}

	unsigned testNum=atoi(argv[3]);
	srand(time(0));

	unsigned nodenum=numberOfNodes(db);
	for (unsigned i=0; i < testNum; i++){
		unsigned startid=rand()%nodenum;
		const char* start=0,*stop=0; Type::ID type; unsigned subType;
		db.getDictionary().lookupById(startid,start,stop,type,subType);
		if (type != Type::URI){
			i--;
			continue;
		}
		unsigned  approx;
		db.getPathSelectivity().lookupSelectivity(startid,dir,approx);

		approx=(approx==0)?1:approx;
		DijkstraEngine scan1(db, order);
		scan1.computeSP(startid);
		unsigned exact=scan1.getSPMap().size();
		if (exact<10){
			i--;
			continue;
		}
		cout<<startid<<" "<<exact<<" "<<approx<<" "<<fabs((int)(exact)-(int)approx)/min(exact,approx)<<endl;
	}


}
