#include "rts/database/Database.hpp"
#include "rts/dijkstra/FastDijkstraEngine.hpp"
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
#include <map>
#include <stack>
#include <assert.h>
#include <math.h>
//---------------------------------------------------------------------------
using namespace std;
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
//---------------------------------------------------------------------------
void FastDijkstraEngine::precomputeSelectivity(){
	/// assume the order was OPS
	set<unsigned> literal;
	clusterPredicates(URI,literal,db);
	FactsSegment::Scan scan, scan1;
	unsigned count=0;
	// find all the URI nodes
	if (scan1.first(db.getFacts(Database::Order_Subject_Predicate_Object),0,0,0)) do {
		count=scan1.getValue1();
//		cerr<<"subjects: "<<count<<" "<<lookupById(db,count)<<endl;
	} while (scan1.next());

	if (scan.first(db.getFacts(Database::Order_Object_Predicate_Subject),count,0,0)) do {
		count=scan.getValue1();
	} while (scan.next());
	cerr<<"forward count: "<<count<<endl;
    cerr<<"backward count: "<<count<<endl;

    backward_selectivity.resize(count+1);

    selectivity.resize(count+1);

    // init the selectivity with Literal children count
	if (scan.first(db.getFacts(Database::Order_Subject_Predicate_Object),0,0,0)) do {
		if (!URI.count(scan.getValue2())){
			// this edge leads to the literal
			backward_selectivity[scan.getValue1()]+=1;
		}
	} while (scan.next());
}
//---------------------------------------------------------------------------
void FastDijkstraEngine::countSelectivity(unsigned root){
	/// assume order was OPS
	stack<unsigned> s, internal;
	s.push(root);
	unsigned nodes=0;
	map<unsigned, bool> visited;

	while (!s.empty()){
		unsigned node = s.top();
		s.pop();
		visited[node]=true;
		set<unsigned>& kids=tree[node];
		for (set<unsigned>::iterator it=kids.begin(); it!=kids.end(); it++){
			nodes++;
			if (!visited[*it])
				s.push(*it);
		}

		set<unsigned>& parURI = parentsURI[node];
		for (set<unsigned>::iterator it=parURI.begin(); it!=parURI.end(); it++){
				backward_selectivity[node]+=backward_selectivity[*it];
		}

		backward_selectivity[node]+=1;

		internal.push(node);
	}
	visited.clear();

	while (!internal.empty()){
		unsigned node=internal.top();
		internal.pop();

		if (visited[node])
			continue;

		visited[node]=true;

		set<unsigned>& kids=tree[node];

		for (set<unsigned>::iterator it=kids.begin(); it!=kids.end(); it++){
			selectivity[node]+=selectivity[*it];
		}

		selectivity[node]+=kids.size()+1;
	}
}
//---------------------------------------------------------------------------
void FastDijkstraEngine::getNeighbors(){
	Register reg;
	Register rs,rp,ro;
	Register *s=0, *o=0;
	rs.reset(); rp.reset(); ro.reset();
	IndexScan* scan1=IndexScan::create(db,order,&rs,false,&rp,false,&ro,false,0);

	ConstantOperator* scan2=new ConstantOperator(&reg,curNodes);
	vector<Register*> lt,rt;

	// forward or backward search?
	if (order==Database::Order_Subject_Predicate_Object){
		s=&rs;
		o=&ro;
	}
	else {
		s=&ro;
		o=&rs;
	}

	rt.push_back(&rp);rt.push_back(o);
	MergeJoin join(scan2,&reg,lt,scan1,s,rt,0);
	n.connections.clear();
	n.connections.resize(curNodes.size());
	unsigned old=s->value;
	unsigned i=0;
	bool firstiter=true;

	for (set<unsigned>::iterator it = curNodes.begin(); it != curNodes.end(); it++)
		n.ids.push_back(*it);
//	cerr<<"n.connections size: "<<n.connections.size()<<endl;
	if (join.first()) do {
		if (old==s->value||firstiter){
		   old=s->value;
		   while (n.ids[i]!=s->value) i++;
		   n.connections[i].push_back(pair<unsigned,unsigned>(rp.value,o->value));
		   firstiter=false;
		}
		else {
		   old=s->value;
		   i++;
		   while (n.ids[i]!=s->value) i++;
		   n.connections[i].push_back(pair<unsigned,unsigned>(rp.value,o->value));
		}
    } while (join.next());

}
//---------------------------------------------------------------------------
void FastDijkstraEngine::init(unsigned source) {
	// init the computation
	tree.clear();
	parentsURI.clear();
	workingSet.clear();
	settledNodes.clear();
	shortestDistances.clear();
	curNodes.clear();
	workingSet.insert(pair<unsigned, unsigned>(0,source));
	shortestDistances[source]=0;
	curNodes.insert(source);
	curnodes_iter=curNodes.begin();
	curIndex=0;
	n.connections.clear();
	n.ids.clear();
}
//---------------------------------------------------------------------------
void FastDijkstraEngine::updateNeighbors(unsigned node,unsigned nodeindex){
	vector<pair<unsigned,unsigned> >& succ=n.connections[nodeindex];
	for (vector<pair<unsigned,unsigned> >::iterator iter=succ.begin(),limit=succ.end(); iter!=limit; iter++){
		// if we need to estimate selectivity, save URI predecessors
		if (needSelectivity) {
			tree[node].insert(iter->second);
			if (URI.count(iter->first))
				parentsURI[iter->second].insert(node);
		}
		if (DijkstraEngine::isHandled(iter->second))
			continue;
		unsigned shortDist=DijkstraEngine::getShortestDist(node)+1;
		unsigned oldShortDist=DijkstraEngine::getShortestDist(iter->second);
		if (shortDist<oldShortDist) {
			workingSet.erase(pair<unsigned,unsigned>(oldShortDist,iter->second));
			shortestDistances[iter->second]=shortDist;
			workingSet.insert(pair<unsigned,unsigned>(shortDist,iter->second));
		}
	}
}
//---------------------------------------------------------------------------
void FastDijkstraEngine::computeSP(unsigned source){
	init(source);
	if (curnodes_iter==curNodes.begin()){
		getNeighbors();
	}

	while (!workingSet.empty()) {
		if (curnodes_iter==curNodes.end() && !workingSet.empty()){
			curNodes.clear();
			// copying the neighbors.
			// can we just use workingSet???
			for (unsigned i=0; i<n.connections.size(); i++){
				for (unsigned j=0; j<n.connections[i].size(); j++){
					if (!DijkstraEngine::isHandled((n.connections[i])[j].second))
						curNodes.insert((n.connections[i])[j].second);
				}
			}
			n.connections.clear();
			n.ids.clear();
			curIndex=0;

			getNeighbors();
			curnodes_iter=curNodes.begin();
		}

		while (curnodes_iter!=curNodes.end()){
			unsigned curNode=*curnodes_iter;
			unsigned curDist=DijkstraEngine::getShortestDist(curNode); // we need to store it in curNodes!!!

			workingSet.erase(pair<unsigned,unsigned>(curDist,curNode));
			curnodes_iter++;

			if (DijkstraEngine::isHandled(curNode)){
				curIndex++;
				continue;
			}

			updateNeighbors(curNode,curIndex);
			curIndex++;

			settledNodes.insert(curNode);
		}
	}
}
