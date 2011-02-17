#include "rts/operator/FastDijkstraScan.hpp"
#include "rts/operator/PlanPrinter.hpp"
#include "rts/runtime/Runtime.hpp"
#include "rts/segment/FactsSegment.hpp"
#include "cts/infra/QueryGraph.hpp"
#include "rts/segment/DictionarySegment.hpp"
#include <iostream>
#include <vector>
#include "rts/operator/IndexScan.hpp"
#include "rts/operator/MergeJoin.hpp"
#include "rts/operator/PlanPrinter.hpp"
#include "infra/osdep/Timestamp.hpp"
#include <map>
#include <set>
#include <algorithm>
#include <stdlib.h>
using namespace std;
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
void FastDijkstraScan::print(PlanPrinter& out)
   // Print the operator tree. Debugging only.
{
   out.beginOperator("DijkstraScan",expectedOutputCardinality,observedOutputCardinality);
   out.endOperator();
}
//---------------------------------------------------------------------------
FastDijkstraScan::FastDijkstraScan(Database& db,Database::DataOrder order,Register* value1,bool bound1,VectorRegister* value2,bool bound2,Register* value3,bool bound3,double expectedOutputCardinality,QueryGraph::Filter* pathfilter)
   : Operator(expectedOutputCardinality),dict(db.getDictionary()),value1(value1),value2(value2),value3(value3),bound1(bound1),bound2(bound2),bound3(bound3),order(order),facts(db.getFacts(order)),pathfilter(pathfilter)
   // Constructor
{
}
//---------------------------------------------------------------------------
FastDijkstraScan::~FastDijkstraScan()
   // Destructor
{

}
//---------------------------------------------------------------------------
unsigned FastDijkstraScan::first()
{
	return 0;
}
//---------------------------------------------------------------------------
unsigned FastDijkstraScan::next()
{
	return 0;
}
//---------------------------------------------------------------------------
class FastDijkstraScan::DijkstraPrefix: public FastDijkstraScan {
private:
	/// comparing nodes by distance to source
	struct CompareByDistance {
	    bool operator()(const pair<unsigned,unsigned> &lhs, const pair<unsigned, unsigned> &rhs) {
	        if (lhs.first == rhs.first)
	            return lhs.second < rhs.second;
	        return lhs.first < rhs.first;
	    }
	};
	/// struct to keep predecessors info
	struct PreviousNode{
		/// node id
		unsigned node;
		/// edge id
		unsigned edge;
	};
	/// Struct to keep values of path predicates on bfs tree prefix
	struct PredicateOnNode{
		// value on current node
		bool onnode;
		// value on the tree prefix, separately for every predicate in the path filter
		map<unsigned,bool> onprefix;
		// length of the prefix - for length constrains
		unsigned prefixlength;
	};
	/// struct to keep constraints that helps to cut off the search space
	struct Constraints{
		// length bound
		unsigned maxlen;
		// containsOnly constraint  - one set for each path variable
		map<unsigned,set<unsigned> > containsonly;
		// any constraint violated?
		bool violated;
		// should we cut off the branch starting with this node?
		set<unsigned> cutOff;
	};
	/// Precomputed predicated on prefixes
	map<unsigned,PredicateOnNode> predicates;
	/// cutoff for length, if specified
	unsigned lenmax;
	/// Neighbors
	vector<pair<unsigned,unsigned> > neighbors;
	/// Iterator on neighbors
	vector<pair<unsigned, unsigned> >::const_iterator iter;
	/// Dijkstra's settled nodes
	set<unsigned> settledNodes;
	/// Dijkstra's predecessors
	map<unsigned,PreviousNode> predecessors;
	/// Dijkstra's working set
	set<pair<unsigned, unsigned>,CompareByDistance > workingSet;
	/// Dijkstra's shortest distances
	map<unsigned,unsigned> shortestDistances;
	/// Find neighbors (according to scan order) of given order
	void findNeighbors(unsigned node);
	/// Dijkstra's init
	void init();
	/// updating neighbors in Dijkstra's algo
	void updateNeighbors(unsigned node);
	/// is the node handled already?
	bool isHandled(unsigned node);
	/// get the shortest dist from the start to the node
	unsigned getShortestDist(unsigned node);

	void getNeighbors();
	void updateN(unsigned node,unsigned nodeIndex);
	void findConstraints(const QueryGraph::Filter& filter, Constraints& constr);
	struct Neighbors{
		vector<vector<pair<unsigned,unsigned> > > connections;
		vector<unsigned> ids;
	};
	Neighbors n;
	set<unsigned> curNodes;
	set<unsigned>::iterator curnodes_iter;
	unsigned curIndex;
	Database& db;
	unsigned workingsetmax;
	bool output;
	Constraints constr;
public:
	/// Constructor
	DijkstraPrefix(Database& db,Database::DataOrder order,Register* value1,bool bound1,VectorRegister* value2,bool bound2,Register* value3,bool bound3,double expectedOutputCardinality,QueryGraph::Filter* pathfilter): FastDijkstraScan(db,order,value1,bound1,value2,bound2,value3,bound3,expectedOutputCardinality,pathfilter), db(db) {
	}
	/// First tuple
	unsigned first();
	/// Next tuple
	unsigned next();
	/// Evaluate the predicate
	bool evaledge(const QueryGraph::Filter& filter,PreviousNode& node,PredicateOnNode& predicate,unsigned curNode);
};
//---------------------------------------------------------------------------
void FastDijkstraScan::DijkstraPrefix::findNeighbors(unsigned node) {
	// get the neighbors according to Database Order
	neighbors.clear();
	FactsSegment::Scan scan1;
    if (scan1.first(facts, node, 0, 0)) do {
            if (scan1.getValue1() != node) break;
            neighbors.push_back(pair<unsigned, unsigned>(scan1.getValue2(), scan1.getValue3()));
        } while (scan1.next());
    iter=neighbors.begin();
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
void FastDijkstraScan::DijkstraPrefix::getNeighbors(){
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
void FastDijkstraScan::DijkstraPrefix::findConstraints(const QueryGraph::Filter& filter, FastDijkstraScan::DijkstraPrefix::Constraints& constr){
	if (filter.type==QueryGraph::Filter::And){
		findConstraints(*filter.arg1,constr);
		findConstraints(*filter.arg2,constr);
	}
	else if (filter.type==QueryGraph::Filter::Builtin_length){
		constr.maxlen=std::max(constr.maxlen,filter.arg2->id);
	}
	else if (filter.type==QueryGraph::Filter::Builtin_containsonly){
		constr.containsonly[filter.arg1->id].insert(filter.arg2->id);
	}
	return;
}
//---------------------------------------------------------------------------
void FastDijkstraScan::DijkstraPrefix::init() {
	// init the computation. Original value of the pathfilter set to false
	predecessors.clear();
	workingSet.clear();
	settledNodes.clear();
	shortestDistances.clear();
	curNodes.clear();
	workingSet.insert(pair<unsigned, unsigned>(0,value1->value));
	shortestDistances[value1->value]=0;
	PredicateOnNode p; p.onnode=false; p.prefixlength=0;
	predicates[value1->value]=p;
	curNodes.insert(value1->value);
	curnodes_iter=curNodes.begin();
	curIndex=0;
	workingsetmax=0;
	constr.maxlen=0;
	constr.violated=false;
	findConstraints(*pathfilter, constr);
	output=false;
}
//---------------------------------------------------------------------------
unsigned FastDijkstraScan::DijkstraPrefix::getShortestDist(unsigned node){
	if (shortestDistances.find(node)!=shortestDistances.end())
		return shortestDistances[node];
	return ~0u;
}
//---------------------------------------------------------------------------
bool FastDijkstraScan::DijkstraPrefix::isHandled(unsigned node){
// the node is "black" in Dijkstra's algo
	return (settledNodes.find(node)!=settledNodes.end());
}
//---------------------------------------------------------------------------
void FastDijkstraScan::DijkstraPrefix::updateNeighbors(unsigned node)
// update the distance to neighbors
{
	findNeighbors(node);
	for (vector<pair<unsigned,unsigned> >::iterator iter=neighbors.begin(),limit=neighbors.end(); iter!=limit; iter++){

		if (isHandled(iter->second))
			continue;
		unsigned shortDist=getShortestDist(node)+1;
		unsigned oldShortDist=getShortestDist(iter->second);

		if (shortDist<oldShortDist) {
			workingSet.erase(pair<unsigned,unsigned>(oldShortDist,iter->second));
			shortestDistances[iter->second]=shortDist;
			workingSet.insert(pair<unsigned,unsigned>(shortDist,iter->second));
			// evaluate the predicate on prefix and last edge
			PreviousNode prevnode;
			prevnode.edge=iter->first;
			prevnode.node=node;
			PredicateOnNode p;
			p.prefixlength=predicates[node].prefixlength+1;
			p.onprefix=predicates[node].onprefix;
			if (pathfilter)
				p.onnode=evaledge(*pathfilter,prevnode,p,iter->second);

			predicates[iter->second]=p;
			predecessors[iter->second]=prevnode;
		}
	}
}
//---------------------------------------------------------------------------
void FastDijkstraScan::DijkstraPrefix::updateN(unsigned node,unsigned nodeindex){
	vector<pair<unsigned,unsigned> >& succ=n.connections[nodeindex];
	for (vector<pair<unsigned,unsigned> >::iterator iter=succ.begin(),limit=succ.end(); iter!=limit; iter++){
		if (isHandled(iter->second))
			continue;
		unsigned shortDist=getShortestDist(node)+1;
		unsigned oldShortDist=getShortestDist(iter->second);
		if (shortDist<oldShortDist) {
			workingSet.erase(pair<unsigned,unsigned>(oldShortDist,iter->second));
			shortestDistances[iter->second]=shortDist;

			PreviousNode prevnode;
			prevnode.edge=iter->first;
			prevnode.node=node;
			PredicateOnNode p;
			p.prefixlength=predicates[node].prefixlength+1;
			p.onprefix=predicates[node].onprefix;
			if (pathfilter)
				p.onnode=evaledge(*pathfilter,prevnode,p,iter->second);

			// don't add to the working set nodes that lead to cut-off branches
			if (pathfilter && !constr.cutOff.count(iter->second))
				workingSet.insert(pair<unsigned,unsigned>(shortDist,iter->second));
			predicates[iter->second]=p;
			predecessors[iter->second]=prevnode;
		}
	}
}
//---------------------------------------------------------------------------
unsigned FastDijkstraScan::DijkstraPrefix::first(){
	// value1 is always fixed
	observedOutputCardinality=0;
	cerr<<"FIRST"<<endl;
	init();
	return next();
}
//---------------------------------------------------------------------------
bool FastDijkstraScan::DijkstraPrefix::evaledge(const QueryGraph::Filter& filter,PreviousNode& node,PredicateOnNode& p,unsigned curNode){
	// computing the value of pathfilter for current node based on prefix and last edge
	unsigned filterid;
	switch (filter.type){
		case QueryGraph::Filter::And: return evaledge(*filter.arg1,node,p,curNode)&&evaledge(*filter.arg2,node,p,curNode);
		case QueryGraph::Filter::Or: return evaledge(*filter.arg1,node,p,curNode)||evaledge(*filter.arg2,node,p,curNode);
		case QueryGraph::Filter::Builtin_containsany:
			filterid=filter.arg2->id;
			if (p.onprefix[filterid])
				// this edge already exists in prefix - no need to check anything
				return true;
			if (node.edge==filterid || curNode==filterid){
				// current edge is the one we look for, update the prefix
				p.onprefix[filterid]=true;
				return true;
			}
			return false;
		case QueryGraph::Filter::Builtin_containsonly:
			filterid=filter.arg2->id;
			if (!p.onprefix[filterid]&&node.node!=value1->value){
				// the prefix already does not contain the edge, no need to check further
				return false;
			}
			if (node.edge==filterid){
				p.onprefix[filterid]=true;
			}
			else {
				p.onprefix[filterid]=false;
				constr.cutOff.insert(curNode);
			}
			return p.onprefix[filterid];
		case QueryGraph::Filter::Builtin_length:
			//compare the current prefix length with the constraint from the filter
			if (p.prefixlength>filter.arg2->id){
				//do we also violate the global maxlen constraint?
				if (p.prefixlength>constr.maxlen && constr.maxlen>0)
					constr.violated=true;
				return false;
			}
			return true;
		default:
			return false;
	}
}
//---------------------------------------------------------------------------
unsigned FastDijkstraScan::DijkstraPrefix::next()
	// Return next tuple
{
	if (curnodes_iter==curNodes.begin()){
		getNeighbors();
	}

	// if it's a point-to-point search and we've already found the result
	if (output && bound3)
		return 0;
	else
		output=false;

	while (!workingSet.empty()) {
		if (curnodes_iter==curNodes.end() && !workingSet.empty()){
			if (constr.violated)
				return 0;
			curNodes.clear();
			// copying the neighbors.
			// can we just use workingSet???
			for (unsigned i=0; i<n.connections.size(); i++){
				for (unsigned j=0; j<n.connections[i].size(); j++){
					if (!isHandled((n.connections[i])[j].second) && !constr.cutOff.count((n.connections[i])[j].second))
						curNodes.insert((n.connections[i])[j].second);
				}
			}
			n.connections.clear();
			n.ids.clear();
			curIndex=0;

			Timestamp t1;
			getNeighbors();
			Timestamp t2;
			cerr<<"getting neighbors: "<<t2-t1<<" ms"<<endl;
			cerr<<"curNodes: "<<curNodes.size()<<endl;
			curnodes_iter=curNodes.begin();
		}

//	for (set<pair<unsigned, unsigned>,CompareByDistance >::iterator itw=workingSet.begin(); itw!=workingSet.end(); itw++)
//		cerr<<"("<<(*itw).first<<", "<<(*itw).second<<") ";
//	cerr<<endl;

		while (curnodes_iter!=curNodes.end()){
			unsigned curNode=*curnodes_iter;
			unsigned curDist=getShortestDist(curNode); // we need to store it in curNodes!!!

			workingSet.erase(pair<unsigned,unsigned>(curDist,curNode));
			curnodes_iter++;

			if (isHandled(curNode)){
				curIndex++;
				continue;
			}

			updateN(curNode,curIndex);

			if (workingSet.size()>workingsetmax)
				workingsetmax=workingSet.size();
			curIndex++;

			settledNodes.insert(curNode);

			// this is not point-to-point search
			if (!bound3)
				value3->value=curNode;

			if (!pathfilter)
				// no pathfilter specified, output every reachable node
				output=true;
			else if (predicates[value3->value].onnode)
				// there is a pathfilter and current node satisfies it
				output=true;

			if (bound3&&(curNode!=value3->value)){
				// this is a point-to-point search and current value does not pass
				output=false;
			}
			unsigned prev=0;
			if (output) {
				observedOutputCardinality++;
				// form the path from the end to the beginning
				value2->value.clear();

				while (predecessors.find(curNode)!=predecessors.end()){
					PreviousNode node=predecessors[curNode];
					prev=node.node;
					value2->value.push_front(node.edge);
					if (prev!=value1->value)
						value2->value.push_front(prev);
					curNode=prev;
				}
				return 1;
			}
		}
	}

//	cerr<<"settled nodes: "<<settledNodes.size()<<endl;
//	cerr<<"max working set size: "<<workingsetmax<<endl;
//	for (unsigned j=0; j<n.connections.size(); j++){
//		cerr<<"node "<<lookupLiteral(db,n.ids[j])<<" has "<<n.connections[j].size()<<" neighbors"<<endl;
//	//		for (unsigned k=0; k<n.connections[j].size(); k++)
//	//			cerr<<"    "<<lookupLiteral(db,(n.connections[j])[k].first)<<" "<<lookupLiteral(db,(n.connections[j])[k].second)<<endl;
//	}

    return 0;
}
//---------------------------------------------------------------------------
FastDijkstraScan* FastDijkstraScan::create(Database& db,Database::DataOrder order,Register* subject,bool subjectBound,VectorRegister* predicate,bool predicateBound,Register* object,bool objectBound,double expectedOutputCardinality,QueryGraph::Filter* pathfilter)
   // Constructor
{
   // Setup the slot bindings
   Register* value1=0,*value3=0;
   VectorRegister* value2=0;
   bool bound1=false,bound2=false,bound3=false;
   switch (order) {
      case Database::Order_Subject_Predicate_Object:
    	  // it is a direct bfs
         value1=subject; value2=predicate; value3=object;
         bound1=subjectBound; bound2=predicateBound; bound3=objectBound;
         break;
      case Database::Order_Object_Predicate_Subject:
    	  // it is a reversed bfs
         value1=object; value2=predicate; value3=subject;
         bound1=objectBound; bound2=predicateBound; bound3=subjectBound;
         break;
      case Database::Order_Subject_Object_Predicate:
      case Database::Order_Object_Subject_Predicate:
      case Database::Order_Predicate_Subject_Object:
      case Database::Order_Predicate_Object_Subject:
    	  return 0; //never happens
   }
   // Construct the appropriate operator
   FastDijkstraScan* result = new DijkstraPrefix(db,order,value1,bound1,value2,bound2,value3,bound3,expectedOutputCardinality,pathfilter);
   return result;
}
