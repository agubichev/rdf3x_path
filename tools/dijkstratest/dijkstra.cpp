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
/// find the avg running time of Dijkstra algo vs Join-based Dijkstra
int main(int argc,char* argv[]){
	if ((argc<4)) {
	    cout << "usage: " << argv[0] << " <database> <type> <URI>" << endl;
	    cout << "       type 0 for Dijkstra, 1 for Join-based Dijkstra"<<endl;
	    return 1;
	}

    // Open the database
	Database db;
	if (!db.open(argv[1])) {
	   cout << "unable to open " << argv[1] << endl;
	   return 1;
	}

	Database::DataOrder order=Database::Order_Subject_Predicate_Object;

	unsigned type=atoi(argv[2]);

	// init the dijkstra engine
	DijkstraEngine* eng;
	if (type==0)
		eng=new SlowDijkstraEngine(db,order);
	else
		eng=new FastDijkstraEngine(db,order,false);

	// lookup node id
	unsigned node=0;
	db.getDictionary().lookup(argv[3],Type::URI,0,node);

	cerr<<node<<endl;
	// find shortest path tree
	Timestamp t1;
	eng->computeSP(node);
	Timestamp t2;

	unsigned treeSize=eng->getSPMap().size();
	cout<<node<<": "<<t2-t1<<" ms, "<<"tree size: "<<treeSize<<endl;

}
