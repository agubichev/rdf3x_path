#include "rts/database/Database.hpp"
#include "rts/segment/PathSelectivitySegment.hpp"
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
/// lookup the selectivity for internal id
int main(int argc,char* argv[]){
	if ((argc<4)) {
	    cout << "usage: " << argv[0] << " <database> <id> <dir>" << endl;
	    return 1;
	}

    // Open the database
	Database db;
	if (!db.open(argv[1])) {
	   cout << "unable to open " << argv[1] << endl;
	   return 1;
	}

	unsigned node=atoi(argv[2]);
	PathSelectivitySegment::Direction dir=static_cast<PathSelectivitySegment::Direction>(atoi(argv[3]));

	cerr<<"node, dir: "<<node<<" "<<dir<<endl;
	unsigned sel=0;
	db.getPathSelectivity().lookupSelectivity(node, dir, sel);
	cerr<<"selectivity: "<<sel<<endl;
}
