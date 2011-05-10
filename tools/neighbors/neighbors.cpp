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
#include "infra/osdep/Timestamp.hpp"
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
//---------------------------------------------------------------------------
static void findNeighbors(Database& db,set<unsigned>& nodes, set<unsigned>& neighbors, Database::DataOrder order){
	Register rs,rp,ro;
	Register *s=0, *o=0;

	rs.reset(); rp.reset(); ro.reset();
	IndexScan* scan1=IndexScan::create(db,Database::Order_Object_Predicate_Subject,&rs,false,&rp,false,&ro,false,0);

	Register reg;
//	cerr<<"nodes size: "<<nodes.size()<<endl;
	ConstantOperator* scan2=new ConstantOperator(&reg,nodes);

	vector<Register*> lt,rt;
	if (order==Database::Order_Subject_Predicate_Object){
		s=&rs;
		o=&ro;
		cerr<<"SPO"<<endl;
	}
	else {
		cerr<<"OPS"<<endl;
		s=&ro;
		o=&rs;
	}

	rt.push_back(o);
	rt.push_back(&rp);

	MergeJoin join(scan2,&reg,lt,scan1,s,rt,0);

//	if (scan1->first()) do {
//		cerr<<s->value<<" "<<rp.value<<" "<<o->value<<endl;
//		cerr<<lookupLiteral(db,s->value)<<" "<<lookupLiteral(db,rp.value)<<" "<<lookupLiteral(db,o->value)<<endl;
//	} while (scan1->next());

	Timestamp t1;
	if (join.first()) do {
	   cerr<<"neighbors: "<<s->value<<" "<<rp.value<<" "<<o->value<<endl;
	   cerr<<"neighbors: "<<lookupLiteral(db,s->value)<<" "<<lookupLiteral(db,rp.value)<<" "<<lookupLiteral(db,o->value)<<endl;
	   neighbors.insert(o->value);
	} while (join.next());
	Timestamp t2;
	cerr<<"time: "<<t2-t1<<" ms"<<endl;

}
//---------------------------------------------------------------------------

int main(int argc,char* argv[])
{
   if (argc<2) {
      cout << "usage: " << argv[0] << " <rdfstore> " << endl;
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

   nodes.insert(131704);

//   nodes.insert(2);
   nodes.insert(5);
   set<unsigned> res;

//   for (unsigned i=0; i<1; i++) {

//   Timestamp t1;
   findNeighbors(db,nodes,res,Database::Order_Object_Predicate_Subject);
//   Timestamp t2;
//   cerr<<"time: "<<t2-t1<<" ms"<<endl;
   cerr<<"nodes size: "<<nodes.size()<<endl;
   cerr<<"res size: "<<res.size()<<endl;
//	   nodes.clear();
////	   cerr<<"res size: "<<res.size()<<endl;
////	   cout<<"----------------------------------------------------------------"<<endl;
////	   cout<<"res "<<i<<endl;
////	   for (set<unsigned>::iterator it=res.begin();it!=res.end();it++)
////		   cout<<lookupLiteral(db,*it)<<endl;
//	   nodes=res;
//	   res.clear();
//   }
}
//---------------------------------------------------------------------------
