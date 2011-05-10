#include "rts/database/Database.hpp"
#include "rts/segment/DictionarySegment.hpp"
#include "rts/segment/FactsSegment.hpp"
#include "rts/dijkstra/FastDijkstraEngine.hpp"
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
#include <map>
#include <stack>
#include <assert.h>
#include <math.h>
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
static void getPredecessors(unsigned node, set<unsigned>& pred, Database& db){
	FactsSegment::Scan scan;
	if (scan.first(db.getFacts(Database::Order_Object_Predicate_Subject),node,0,0)) do {
       if (scan.getValue1() != node) break;
          pred.insert(scan.getValue3());
	} while (scan.next());
}
//---------------------------------------------------------------------------
static void getDestinations(unsigned node, set<unsigned>& dest, Database& db){
	{
	   FactsSegment::Scan scan;
	   if (scan.first(db.getFacts(Database::Order_Subject_Predicate_Object),node,0,0)) do {
           if (scan.getValue1() != node) break;
           dest.insert(scan.getValue3());
	   } while (scan.next());
	}

}
//---------------------------------------------------------------------------
static void getRoots(set<unsigned>& roots, Database& db){

	FactsSegment::Scan scan;
	unsigned start=0;

	FactsSegment::Scan scanfirst;
	scanfirst.first(db.getFacts(Database::Order_Subject_Predicate_Object),0,0,0);
	start=scanfirst.getValue1()-1;
	if (scan.first(db.getFacts(Database::Order_Object_Predicate_Subject),0,0,0)) do {
        //if (scan.getValue1() != node) break;
        //pred.insert(scan.getValue3());
		if (scan.getValue1()!=start){
		   for (unsigned i=start+1; i<scan.getValue1(); i++){
		      roots.insert(i);
		   }
		   start=scan.getValue1();
		}
	} while (scan.next());

	if (scanfirst.first(db.getFacts(Database::Order_Subject_Predicate_Object),start,0,0)) do {
		roots.insert(scanfirst.getValue1());
	} while (scanfirst.next());
}
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
	bool flag1=true, flag2;
	set<unsigned> literalObjects;
	bool onlyLiteral=true;
	unsigned iter=0;
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
	// process the last triple
	if (onlyLiteral)
		leaves.insert(start);
}
//---------------------------------------------------------------------------
static unsigned numberOfNeighbors(unsigned node, Database& db, Database::DataOrder order){
	set<unsigned> neighbors;
	FactsSegment::Scan scan1;
    if (scan1.first(db.getFacts(order), node, 0, 0)) do {
            if (scan1.getValue1() != node) break;
            neighbors.insert(scan1.getValue3());

    } while (scan1.next());
    return neighbors.size();
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
//namespace{
////---------------------------------------------------------------------------
//class DijkstraEngine {
//public:
//	/// comparing nodes by distance to source
//	template <typename T> struct CompareByDistance {
//	    bool operator()(const pair<T, T> &lhs, const pair<T, T> &rhs) {
//	        if (lhs.first == rhs.first)
//	            return lhs.second < rhs.second;
//	        return lhs.first < rhs.first;
//	    }
//	};
//
//private:
//	/// Dijkstra's settled nodes
//	set<unsigned> settledNodes;
//	/// Dijkstra's shortest path tree
//	map<unsigned,set<unsigned> > tree;
//	map<unsigned,set<unsigned> > parentsURI;
//
//	/// Dijkstra's working set
//	set<pair<unsigned, unsigned>,CompareByDistance<unsigned> > workingSet;
//	/// Dijkstra's shortest distances
//	map<unsigned,unsigned> shortestDistances;
//	/// Dijkstra's init
//	void init(unsigned source);
//	/// is the node handled already?
//	bool isHandled(unsigned node);
//	/// get the shortest dist from the start to the node
//	unsigned getShortestDist(unsigned node);
//
//	void getNeighbors();
//
//	void updateNeighbors(unsigned node,unsigned nodeIndex);
//
//	struct Neighbors{
//		vector<vector<pair<unsigned,unsigned> > > connections;
//		vector<unsigned> ids;
//	};
//
//	Neighbors n;
//	set<unsigned> curNodes;
//	set<unsigned>::iterator curnodes_iter;
//	unsigned curIndex;
//	Database& db;
//	Database::DataOrder order;
//
//	vector<unsigned> selectivity;
//	vector<unsigned> backward_selectivity;
//
//	set<unsigned> URI;
//
//public:
//	/// Constructor
//	DijkstraEngine(Database& db,Database::DataOrder order): db(db), order(order){
//	}
//
//	void computeSP(unsigned source);
//
//	map<unsigned,unsigned>& getSPMap(){return shortestDistances;}
//
//	void printTree();
//
//	void countSelectivity(unsigned root);
//
//	void precomputeSelectivity();
//
//	void copySelectivity();
//
//	vector<unsigned>& getBackwardSelectivity(){return backward_selectivity;}
//
//	vector<unsigned>& getSelectivity(){return selectivity;}
//
//};
////---------------------------------------------------------------------------
//namespace {
////---------------------------------------------------------------------------
//class ConstantOperator: public Operator{
//	private:
//	/// The  register
//	Register* reg;
//	/// The filter
//	const set<unsigned>& neighbors;
//	set<unsigned>::const_iterator cur;
//
//	public:
//	/// Constructor
//	ConstantOperator(Register* reg,const set<unsigned>& neighbors) : Operator(0),reg(reg),neighbors(neighbors),cur(neighbors.begin()){}
//	/// Destructor
//	~ConstantOperator() { }
//	/// Find the first tuple
//	unsigned first();
//	/// Find the next tuple
//	unsigned next();
//	/// Print the operator
//	void print(PlanPrinter& out);
//	/// Handle a merge hint
//	void addMergeHint(Register* /*l*/,Register* /*r*/) {};
//	/// Register parts of the tree that can be executed asynchronous
//	void getAsyncInputCandidates(Scheduler& /*scheduler*/) {};
//};
////---------------------------------------------------------------------------
//void ConstantOperator::print(PlanPrinter& out){
//	out.beginOperator("ConstantOperator",expectedOutputCardinality,observedOutputCardinality);
//}
////---------------------------------------------------------------------------
//unsigned ConstantOperator::first(){
//	return next();
//}
////---------------------------------------------------------------------------
//unsigned ConstantOperator::next(){
//	if (cur!=neighbors.end()){
//		reg->value=*cur;
//		cur++;
//		return 1;
//	}
//	return 0;
//}
////---------------------------------------------------------------------------
//}
////---------------------------------------------------------------------------
//void DijkstraEngine::precomputeSelectivity(){
//	/// assume the order was OPS
//	set<unsigned> literal;
//	clusterPredicates(URI,literal,db);
//	FactsSegment::Scan scan, scan1;
//	unsigned count=0;
//	// find all the URI nodes
//	if (scan1.first(db.getFacts(Database::Order_Subject_Predicate_Object),0,0,0)) do {
//		count=scan1.getValue1();
////		cerr<<"subjects: "<<count<<" "<<lookupById(db,count)<<endl;
//	} while (scan1.next());
//
//	if (scan.first(db.getFacts(Database::Order_Object_Predicate_Subject),count,0,0)) do {
//		count=scan.getValue1();
//	} while (scan.next());
//	cerr<<"forward count: "<<count<<endl;
//    cerr<<"backward count: "<<count<<endl;
//
//    backward_selectivity.resize(count+1);
//
//    selectivity.resize(count+1);
//
//    // init the selectivity with Literal children count
//	if (scan.first(db.getFacts(Database::Order_Subject_Predicate_Object),0,0,0)) do {
//		if (!URI.count(scan.getValue2())){
//			// this edge leads to the literal
//			backward_selectivity[scan.getValue1()]+=1;
//		}
//	} while (scan.next());
//}
////---------------------------------------------------------------------------
//void DijkstraEngine::printTree(){
//	for (map<unsigned, set<unsigned> >::iterator it=tree.begin(); it!=tree.end(); it++){
//		cerr<<it->first<<": ";
//		for (set<unsigned>::iterator itv=it->second.begin(); itv!=it->second.end(); itv++)
//			cerr<<*itv<<" ";
//		cerr<<endl;
//	}
//}
////---------------------------------------------------------------------------
//void DijkstraEngine::countSelectivity(unsigned root){
//	/// assume order was OPS
//	stack<unsigned> s, internal;
//	s.push(root);
//	unsigned nodes=0;
//	map<unsigned, bool> visited;
//
//	while (!s.empty()){
//		unsigned node = s.top();
//		s.pop();
//		visited[node]=true;
//		set<unsigned>& kids=tree[node];
//		for (set<unsigned>::iterator it=kids.begin(); it!=kids.end(); it++){
//			nodes++;
//			if (!visited[*it])
//				s.push(*it);
//		}
//
//		set<unsigned>& parURI = parentsURI[node];
//		for (set<unsigned>::iterator it=parURI.begin(); it!=parURI.end(); it++){
//				backward_selectivity[node]+=backward_selectivity[*it];
//		}
//
//		backward_selectivity[node]+=1;
//
//
//		internal.push(node);
//	}
//	visited.clear();
//
//	while (!internal.empty()){
//		unsigned node=internal.top();
//		internal.pop();
//
////		if (visited[node])
////			continue;
////
////		visited[node]=true;
//
//		set<unsigned>& kids=tree[node];
//
//		for (set<unsigned>::iterator it=kids.begin(); it!=kids.end(); it++){
//			selectivity[node]+=selectivity[*it];
//		}
//
//		selectivity[node]+=kids.size()+1;
//	}
//}
////---------------------------------------------------------------------------
//void DijkstraEngine::getNeighbors(){
//	Register reg;
//	Register rs,rp,ro;
//	Register *s=0, *o=0;
//	rs.reset(); rp.reset(); ro.reset();
//	IndexScan* scan1=IndexScan::create(db,order,&rs,false,&rp,false,&ro,false,0);
//
//	ConstantOperator* scan2=new ConstantOperator(&reg,curNodes);
//	vector<Register*> lt,rt;
//
//	// forward or backward search?
//	if (order==Database::Order_Subject_Predicate_Object){
//		s=&rs;
//		o=&ro;
//	}
//	else {
//		s=&ro;
//		o=&rs;
//	}
//
//	rt.push_back(&rp);rt.push_back(o);
//	MergeJoin join(scan2,&reg,lt,scan1,s,rt,0);
//	n.connections.clear();
//	n.connections.resize(curNodes.size());
//	unsigned old=s->value;
//	unsigned i=0;
//	bool firstiter=true;
//
//	for (set<unsigned>::iterator it = curNodes.begin(); it != curNodes.end(); it++)
//		n.ids.push_back(*it);
//
//	if (join.first()) do {
//		if (old==s->value||firstiter){
//		   old=s->value;
//		   while (n.ids[i]!=s->value) i++;
//		   n.connections[i].push_back(pair<unsigned,unsigned>(rp.value,o->value));
//		   firstiter=false;
//		}
//		else {
//		   old=s->value;
//		   i++;
//		   while (n.ids[i]!=s->value) i++;
//		   n.connections[i].push_back(pair<unsigned,unsigned>(rp.value,o->value));
//		}
//    } while (join.next());
//
//}
////---------------------------------------------------------------------------
//void DijkstraEngine::init(unsigned source) {
//	// init the computation
//	tree.clear();
//	parentsURI.clear();
//	workingSet.clear();
//	settledNodes.clear();
//	shortestDistances.clear();
//	curNodes.clear();
//	workingSet.insert(pair<unsigned, unsigned>(0,source));
//	shortestDistances[source]=0;
//	curNodes.insert(source);
//	curnodes_iter=curNodes.begin();
//	curIndex=0;
//}
////---------------------------------------------------------------------------
//unsigned DijkstraEngine::getShortestDist(unsigned node){
//	if (shortestDistances.find(node)!=shortestDistances.end())
//		return shortestDistances[node];
//	return ~0u;
//}
////---------------------------------------------------------------------------
//bool DijkstraEngine::isHandled(unsigned node){
//// the node is "black" in Dijkstra's algo
//	return (settledNodes.find(node)!=settledNodes.end());
//}
////---------------------------------------------------------------------------
//void DijkstraEngine::updateNeighbors(unsigned node,unsigned nodeindex){
//	vector<pair<unsigned,unsigned> >& succ=n.connections[nodeindex];
//	for (vector<pair<unsigned,unsigned> >::iterator iter=succ.begin(),limit=succ.end(); iter!=limit; iter++){
//		tree[node].insert(iter->second);
//		if (URI.count(iter->first))
//			parentsURI[iter->second].insert(node);
//		if (isHandled(iter->second))
//			continue;
//		unsigned shortDist=getShortestDist(node)+1;
//		unsigned oldShortDist=getShortestDist(iter->second);
//		if (shortDist<oldShortDist) {
//			workingSet.erase(pair<unsigned,unsigned>(oldShortDist,iter->second));
//			shortestDistances[iter->second]=shortDist;
//			workingSet.insert(pair<unsigned,unsigned>(shortDist,iter->second));
//		}
//	}
//}
////---------------------------------------------------------------------------
//void DijkstraEngine::computeSP(unsigned source){
//	init(source);
//	if (curnodes_iter==curNodes.begin()){
//		getNeighbors();
//	}
//
//	while (!workingSet.empty()) {
//		if (curnodes_iter==curNodes.end() && !workingSet.empty()){
//			curNodes.clear();
//			// copying the neighbors.
//			// can we just use workingSet???
//			for (unsigned i=0; i<n.connections.size(); i++){
//				for (unsigned j=0; j<n.connections[i].size(); j++){
//					if (!isHandled((n.connections[i])[j].second))
//						curNodes.insert((n.connections[i])[j].second);
//				}
//			}
//			n.connections.clear();
//			n.ids.clear();
//			curIndex=0;
//
//			getNeighbors();
//			curnodes_iter=curNodes.begin();
//		}
//
//		while (curnodes_iter!=curNodes.end()){
//			unsigned curNode=*curnodes_iter;
//			unsigned curDist=getShortestDist(curNode); // we need to store it in curNodes!!!
//
//			workingSet.erase(pair<unsigned,unsigned>(curDist,curNode));
//			curnodes_iter++;
//
//			if (isHandled(curNode)){
//				curIndex++;
//				continue;
//			}
//
//			updateNeighbors(curNode,curIndex);
//			curIndex++;
//
//			settledNodes.insert(curNode);
//		}
//	}
//}
////---------------------------------------------------------------------------
//}
//---------------------------------------------------------------------------
static void computeBackwardSelectivity(Database& db, vector<unsigned>& back_selectivity, vector<unsigned>& forward_selectivity){
	set<unsigned> URI;
	set<unsigned> literals;

	clusterPredicates(URI,literals, db);
	set<unsigned> leaves;
	getURILeaves(URI, leaves, db);

	cerr<<"leaves: "<<leaves.size()<<endl;
	FastDijkstraEngine scan(db, Database::Order_Object_Predicate_Subject,true);

	scan.precomputeSelectivity();
	cerr<<"leaves: "<<leaves.size()<<endl;

	for (set<unsigned>::iterator it=leaves.begin(); it != leaves.end(); it++){

		cerr<<"node: "<<*it<<endl;
		cerr<<"node name: "<<lookupById(db,*it)<<endl;
		scan.computeSP(*it);
		cerr<<"map size: "<<scan.getSPMap().size()<<endl;

		scan.countSelectivity(*it);
	}
	forward_selectivity=scan.getBackwardSelectivity();
	back_selectivity=scan.getSelectivity();
}
//---------------------------------------------------------------------------
int main(int argc,char* argv[]){
	if ((argc<3)) {
	    cout << "usage: " << argv[0] << " <database> <iterations>" << endl;
	    return 1;
	}

    // Open the database
	Database db;
	if (!db.open(argv[1])) {
	   cout << "unable to open " << argv[1] << endl;
	   return 1;
	}
	vector<unsigned> back_selectivity;
	vector<unsigned> forw_selectivity;

	computeBackwardSelectivity(db,back_selectivity,forw_selectivity);

	cerr<<"backw, forw: "<<back_selectivity[14747037]<<" "<<forw_selectivity[14747037]<<endl;

//	for (unsigned i=0; i < back_selectivity.size(); i++)
//		cerr<<lookupById(db,i)<<" "<<back_selectivity[i]<<endl;
////	cerr<<back_selectivity[i]<<" "<<back_selectivity[i+1]<<endl;
//	cerr<<"-----------------------------\n";
//	for (unsigned i=0; i < forw_selectivity.size(); i++)
//		cerr<<lookupById(db,i)<<" "<<forw_selectivity[i]<<endl;

//	computeForwardSelectivity(db,forw_selectivity);
//	cerr<<forw_selectivity[2]<<endl;
//	computeForwardSelectivity(db, forw_selectivity);

//	DijkstraEngine scan(db, Database::Order_Subject_Predicate_Object);
//
//	scan.computeSP(atoi(argv[2]));
//	cerr<<scan.getSPMap().size()<<endl;

//	set<unsigned> URI;
//	set<unsigned> literals;
//
//	clusterPredicates(URI,literals, db);
//	cerr<<"URI:"<<endl;
//	for (set<unsigned>::iterator it=URI.begin(); it != URI.end(); it++)
//		cerr<<lookupById(db,*it)<<endl;
//	cerr<<"Literals:"<<endl;
//	for (set<unsigned>::iterator it=literals.begin(); it != literals.end(); it++)
//		cerr<<lookupById(db,*it)<<endl;

//	set<unsigned> leaves;
//	getURILeaves(URI, leaves, db);
//
//	cerr<<"leaves size:"<<leaves.size()<<endl;

//	for (set<unsigned>::iterator it=leaves.begin(); it!=leaves.end(); it++){
//		cerr<<lookupById(db,*it)<<endl;
//	}

//	DijkstraEngine scan(db, Database::Order_Object_Predicate_Subject);
//
//	scan.computeSP(128);
//	cerr<<"map size: "<<scan.getSPMap().size()<<endl;
////	scan.printTree();
//	scan.countSelectivity(128);
//
//	map<unsigned,unsigned>& sel=scan.getSelectivity();
//
//	cerr<<"approx selectivity: "<<sel[18713]<<endl;
//
//	DijkstraEngine scan1(db, Database::Order_Object_Predicate_Subject);
//
//	scan1.computeSP(18713);
//	cerr<<"exact selectivity: "<<scan1.getSPMap().size()<<endl;
//

	///TESTING
	srand(time(0));

	unsigned nodenum=numberOfNodes(db);
	for (unsigned i=0; i < 1000; i++){
		unsigned startid=rand()%nodenum;
		const char* start=0,*stop=0; Type::ID type; unsigned subType;
		db.getDictionary().lookupById(startid,start,stop,type,subType);
		if (type != Type::URI){
			i--;
			continue;
		}
		int approx=back_selectivity[startid];
		approx=(approx==0)?1:approx;
		FastDijkstraEngine scan1(db, Database::Order_Object_Predicate_Subject,false);
		scan1.computeSP(startid);
		int exact=scan1.getSPMap().size();
		if (exact<10){
			i--;
			continue;
		}
		cerr<<startid<<" "<<exact<<" "<<approx<<" "<<fabs(exact-approx)/min(exact,approx)<<endl;
	}
//	for (map<unsigned,unsigned>::iterator it=sel.begin(); it!=sel.end(); it++)
//		cerr<<it->first<<" "<<it->second<<endl;
//	set<unsigned> roots;
//	getURILeaves(URI,roots,db);
//	cerr<<"roots: "<<roots.size()<<endl;
//	for (set<unsigned>::iterator it=roots.begin(); it!=roots.end(); it++){
//		cerr<<*it<<endl;
//		DijkstraEngine scan(db, Database::Order_Object_Predicate_Subject);
//		scan.computeSP(*it);
//		cerr<<"map size: "<<scan.getSPMap().size()<<endl;
//
//	}

}
