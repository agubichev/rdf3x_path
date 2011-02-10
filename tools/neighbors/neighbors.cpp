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
#include <vector>
#include <set>
#include <algorithm>
#include <cassert>
#include <cmath>
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
/// Desired star size
static const unsigned starSize = 6;
//---------------------------------------------------------------------------
namespace {
//---------------------------------------------------------------------------
class ConstantOperator: public Operator{
	private:
	/// The  register
	Register* reg;
	/// The filter
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
	void addMergeHint(Register* l,Register* r) {};
	/// Register parts of the tree that can be executed asynchronous
	void getAsyncInputCandidates(Scheduler& scheduler) {};
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
//---------------------------------------------------------------------------
static string lookupLiteral(Database& db,unsigned id)
   // Lookup a literal value
{
   const char* start=0,*end=start; Type::ID type; unsigned subType;
   db.getDictionary().lookupById(id,start,end,type,subType);

   if (type==Type::URI)
      return "<"+string(start,end)+">"; else
      return "\""+string(start,end)+"\"";
}
//---------------------------------------------------------------------------
struct Neighbors{
	vector<unsigned> ids;
	vector<vector<pair<unsigned, unsigned> > > connections;
};
static Neighbors n;


static void findNeighbors(Database& db,set<unsigned>& nodes, set<unsigned>& neighbors){
	Register ls,lp,lo;
	ls.reset(); lp.reset(); lo.reset();
	IndexScan* scan1=IndexScan::create(db,Database::Order_Subject_Predicate_Object,&ls,false,&lp,false,&lo,false,0);

	Register reg;
	cerr<<"nodes size: "<<nodes.size()<<endl;
	ConstantOperator* scan2=new ConstantOperator(&reg,nodes);

	vector<Register*> lt,rt; lt.push_back(&lp);lt.push_back(&lo);
//	rt.push_back(&a);rt.push_back(&b);
	MergeJoin join(scan2,&reg,rt,scan1,&ls,lt,0);

	vector<unsigned> res;
	unsigned old=ls.value;
	unsigned i=0;
	bool firstiter=true;
	n.ids.clear();
	n.connections.clear();
	n.connections.resize(nodes.size());

	for (set<unsigned>::iterator it=nodes.begin();it!=nodes.end();it++)
		n.ids.push_back(*it);

//	if (scan1->first()) do {
//		cerr<<ls.value<<" "<<lp.value<<" "<<lo.value<<endl;
//		cerr<<lookupLiteral(db,ls.value)<<" "<<lookupLiteral(db,lp.value)<<" "<<lookupLiteral(db,lo.value)<<endl;
//	} while (scan1->next());

	if (join.first()) do {
		if (old==ls.value||firstiter){
		   old=ls.value;
		   n.connections[i].push_back(pair<unsigned,unsigned>(lp.value,lo.value));
		   firstiter=false;
		}
		else {
		   old=ls.value;
		   i++;
		   n.connections[i].push_back(pair<unsigned,unsigned>(lp.value,lo.value));
		}
//	   cerr<<"neighbors: "<<ls.value<<" "<<lp.value<<" "<<lo.value<<endl;
//	   cerr<<"neighbors: "<<lookupLiteral(db, ls.value)<<" "<<lookupLiteral(db, lp.value)<<" "<<lookupLiteral(db, lo.value)<<endl;
	   neighbors.insert(lo.value);

	} while (join.next());

	for (unsigned j=0; j<n.connections.size(); j++){
		cerr<<"node "<<lookupLiteral(db,n.ids[j])<<" has "<<n.connections[j].size()<<" neighbors"<<endl;
//		for (unsigned k=0; k<n.connections[j].size(); k++)
//			cerr<<"    "<<lookupLiteral(db,(n.connections[j])[k].first)<<" "<<lookupLiteral(db,(n.connections[j])[k].second)<<endl;
	}
//	   neighbors.clear();

}
//---------------------------------------------------------------------------

int main(int argc,char* argv[])
{
   if (argc<3) {
      cout << "usage: " << argv[0] << " <rdfstore> <start>" << endl;
      return 1;
   }

   // Open the database
   Database db;
   if (!db.open(argv[1])) {
      cout << "unable to open " << argv[1] << endl;
      return 1;
   }

//   unsigned count=0;
//   if (scan1->first()) do{
//	   cerr<<ls.value<<" "<<lp.value<<" "<<lo.value<<endl;
//	   cerr<<lookupLiteral(db,ls.value)<<" "<<lookupLiteral(db,lp.value)<<" "<<lookupLiteral(db,lo.value)<<endl;
//	   count++;
//   }while (scan1->next()&&count<20);


   set<unsigned> nodes;
//   nodes.insert(atoi(argv[2]));
//   nodes.insert(128);
//   nodes.insert(323);

//   nodes.insert(363);
//   nodes.insert(448);
//   nodes.insert(4348);
//   nodes.insert(4349);
//   nodes.insert(4352);
//   nodes.insert(4354);
//   nodes.insert(11757);
//   nodes.insert(12822);
//   nodes.insert(12823);
//   nodes.insert(12843);
//   nodes.insert(21091);
//   nodes.insert(35288);
//   nodes.insert(44922);
//   nodes.insert(182488);
//   nodes.insert(200614);
//   nodes.insert(337085);
//   nodes.insert(372554);
//   nodes.insert(393717);
//   nodes.insert(449311);
//   nodes.insert(696325);
//   nodes.insert(717001);
//   nodes.insert(767304);
//   nodes.insert(973165);
//   nodes.insert(1031797);
//   nodes.insert(1197305);
//   nodes.insert(1215849);
//   nodes.insert(1374649);
//   nodes.insert(1451916);
//   nodes.insert(1503518);
//   nodes.insert(1943326);
//   nodes.insert(2052258);
//   nodes.insert(2254987);
//   nodes.insert(2450002);
//   nodes.insert(2471914);
//   nodes.insert(2511022);
//   nodes.insert(2531829);
//   nodes.insert(2590952);
   nodes.insert(2930060);
//   nodes.insert(2931239);
//   nodes.insert(2931889);
//   nodes.insert(2932504);
   nodes.insert(3016927);

//   nodes.insert(4);
//   nodes.insert(5);
   set<unsigned> res;

   for (unsigned i=0; i<1; i++) {
	   findNeighbors(db,nodes,res);
	   nodes.clear();
//	   cerr<<"res size: "<<res.size()<<endl;
//	   cout<<"----------------------------------------------------------------"<<endl;
//	   cout<<"res "<<i<<endl;
//	   for (set<unsigned>::iterator it=res.begin();it!=res.end();it++)
//		   cout<<lookupLiteral(db,*it)<<endl;
	   nodes=res;
	   res.clear();
   }
}
//---------------------------------------------------------------------------
