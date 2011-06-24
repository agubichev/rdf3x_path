#include "rts/database/Database.hpp"
#include "rts/segment/PathSelectivitySegment.hpp"
#include "rts/dijkstra/DijkstraEngine.hpp"
#include "rts/dijkstra/SlowDijkstraEngine.hpp"
#include "rts/dijkstra/FastDijkstraEngine.hpp"
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
/// find the avg running time of Dijkstra algo vs Join-based Dijkstra
int main(int argc,char* argv[]){
	if ((argc<4)) {
	    cout << "usage: " << argv[0] << " <database> <type> <number of tests>" << endl;
	    cout << "       type 0 for Dijkstra, 1 for Join-based Dijkstra"<<endl;
	    return 1;
	}

    // Open the database
	Database db;
	if (!db.open(argv[1])) {
	   cout << "unable to open " << argv[1] << endl;
	   return 1;
	}


	Database::DataOrder order=Database::Order_Object_Predicate_Subject;

	unsigned type=atoi(argv[2]);

	unsigned testNum=atoi(argv[3]);


	srand(time(0));


	unsigned nodenum=3000000; //numberOfNodes(db);
	double avgTime=0;
	for (unsigned i=0; i < testNum; i++){
		// init the dijkstra engine
		DijkstraEngine* eng;
		if (type==0)
			eng=new SlowDijkstraEngine(db,order);
		else
			eng=new FastDijkstraEngine(db,order,false);
		// choose the node and check whether it is URI
		unsigned startid=rand()%nodenum;
		const char* start=0,*stop=0; Type::ID type; unsigned subType;
		db.getDictionary().lookupById(startid,start,stop,type,subType);
		if (type != Type::URI){
			i--;
			continue;
		}

		// find shortest path tree
		Timestamp t1;
		eng->computeSP(startid);
		Timestamp t2;
		// if result is too small, skip it
		unsigned treeSize=eng->getSPMap().size();
		if (treeSize<1000||treeSize<10){
			i--;
			continue;
		}

		avgTime+=(t2-t1);
		cout<<startid<<": "<<t2-t1<<" ms, "<<"tree size: "<<treeSize<<endl;
	}
	avgTime/=testNum;

	cerr<<"Average time: "<<avgTime<<endl;

}

/*
 * 2072809: 7 ms
596294: 5 ms
968637: 5 ms
1429392: 9 ms
2309822: 4 ms
1517908: 7 ms
567684: 7 ms
2760485: 10 ms
2797247: 3 ms
 *
 */
