#include "rts/database/Database.hpp"
#include "rts/segment/PathSelectivitySegment.hpp"
#include "rts/dijkstra/DijkstraEngine.hpp"
#include "rts/dijkstra/SlowDijkstraEngine.hpp"
#include "rts/dijkstra/FastDijkstraEngine.hpp"
#include "rts/segment/FactsSegment.hpp"
#include "infra/util/Type.hpp"
#include "rts/segment/DictionarySegment.hpp"
#include "infra/osdep/Timestamp.hpp"
#include "rts/operator/FastDijkstraScan.hpp"
#include "rts/operator/PlanPrinter.hpp"
#include "rts/runtime/Runtime.hpp"
#include "rts/segment/FactsSegment.hpp"
#include "cts/infra/QueryGraph.hpp"
#include "rts/segment/DictionarySegment.hpp"
#include <vector>
#include "rts/operator/IndexScan.hpp"
#include "rts/operator/MergeJoin.hpp"
#include "rts/operator/PlanPrinter.hpp"
#include "infra/osdep/Timestamp.hpp"
#include <map>
#include <set>
#include <iostream>
#include <algorithm>
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
/// get the destination nodes of the node
static void findDestinations(unsigned node, set<unsigned>& res, Database& db) {
    FactsSegment::Scan scan;
    if (scan.first(db.getFacts(Database::Order_Object_Predicate_Subject), node, 0, 0)) do {
            if (scan.getValue1() != node) break;
           	res.insert(scan.getValue3());
        } while (scan.next());
}
//---------------------------------------------------------------------------
namespace {
//---------------------------------------------------------------------------
/// iterator over constant set of nodes
class ConstantOperator: public Operator{
	private:
	/// The  register
	Register* reg;
	/// The set to iterate
	const set<unsigned>& neighbors;
	set<unsigned>::const_iterator cur;

	public:
	/// Constructor
	ConstantOperator(Register* reg,const set<unsigned>& neighbors) : Operator(0),reg(reg),neighbors(neighbors),cur(neighbors.begin()){}
	/// Destructor
	~ConstantOperator() { }
	/// Find the first tuple
	unsigned first();
	/// Find the next tuple
	unsigned next();
	/// Print the operator
	void print(PlanPrinter& out);
	/// Handle a merge hint
	void addMergeHint(Register* /*l*/,Register* /*r*/) {};
	/// Register parts of the tree that can be executed asynchronous
	void getAsyncInputCandidates(Scheduler& /*scheduler*/) {};
};
//---------------------------------------------------------------------------
void ConstantOperator::print(PlanPrinter& out){
	out.beginOperator("ConstantOperator",expectedOutputCardinality,observedOutputCardinality);
}
//---------------------------------------------------------------------------
unsigned ConstantOperator::first(){
	return next();
}
//---------------------------------------------------------------------------
unsigned ConstantOperator::next(){
	if (cur!=neighbors.end()){
		reg->value=*cur;
		cur++;
		return 1;
	}
	return 0;
}
//---------------------------------------------------------------------------
}
///compare the runtimes of join-based and multiple node lookup method
void runExperiment(Database& db, set<unsigned> nodes, unsigned type){
	if (type==0){
		// test the multiple lookups
		set<unsigned> res;
		Timestamp t1;
		for (set<unsigned>::iterator it=nodes.begin(); it!=nodes.end(); it++){
			findDestinations(*it,res,db);
		}
		Timestamp t2;
		cerr<<"multiple lookups time: "<<t2-t1<<" ms, size: "<<res.size()<<endl;
	}
	else {
		// test the merge join
		set<unsigned> res;
		Timestamp t1;
		Register reg;
		Register rs,rp,ro;
		rs.reset(); rp.reset(); ro.reset();
		IndexScan* scan2=IndexScan::create(db,Database::Order_Object_Predicate_Subject,&rs,false,&rp,false,&ro,false,0);

		ConstantOperator* scan1=new ConstantOperator(&reg,nodes);
		vector<Register*> lt,rt;

		rt.push_back(&rp);rt.push_back(&rs);
		MergeJoin join(scan1,&reg,lt,scan2,&ro,rt,0);

		if (join.first()) do {
			res.insert(rs.value);
	    } while (join.next());
		Timestamp t2;
		cerr<<"join method time: "<<t2-t1<<" ms, size: "<<res.size()<<endl;
	}

}
//---------------------------------------------------------------------------
int main(int argc,char* argv[]){
	if ((argc<3)) {
	    cout << "usage: " << argv[0] << " <database>  <type>" << endl<<
	    		" type=0 for multiple-lookup and 1 for join method"<<endl;
	    return 1;
	}

    // Open the database
	Database db;
	if (!db.open(argv[1])) {
	   cout << "unable to open " << argv[1] << endl;
	   return 1;
	}
//	// lookup node id
//	unsigned node=0;
//	db.getDictionary().lookup(argv[2],Type::URI,0,node);

	// type of test
	unsigned type=atoi(argv[2]);

	//run 1000 experiments
	for (unsigned i=0; i < 1000; i++){
		// assume the number of URI nodes in db is around 5 mln
		unsigned node=rand()%50000000;
		// find the input set of nodes
		set<unsigned> dest;
		findDestinations(node,dest,db);
		if (dest.size()>1000 || dest.size()<100){
			i--;
			continue;
		}

		runExperiment(db,dest,type);

	}
}
