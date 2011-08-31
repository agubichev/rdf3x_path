#include "rts/operator/DijkstraScan.hpp"
#include "rts/operator/PlanPrinter.hpp"
#include "rts/runtime/Runtime.hpp"
#include "rts/segment/FactsSegment.hpp"
#include "cts/infra/QueryGraph.hpp"
#include "rts/segment/DictionarySegment.hpp"
#include <iostream>
#include <vector>
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

void DijkstraScan::print(PlanPrinter& out)
   // Print the operator tree. Debugging only.
{
   out.beginOperator("DijkstraScan",expectedOutputCardinality,observedOutputCardinality);
   out.endOperator();
}
//---------------------------------------------------------------------------
DijkstraScan::DijkstraScan(Database& db,Database::DataOrder order,Register* value1,bool bound1,VectorRegister* value2,bool bound2,Register* value3,bool bound3,double expectedOutputCardinality,Register* pathnode,Operator* subplan,QueryGraph::Filter* pathfilter)
   : Operator(expectedOutputCardinality),value1(value1),value3(value3),value2(value2),bound1(bound1),bound2(bound2),bound3(bound3),facts(db.getFacts(order)),order(order),pathfilter(pathfilter),dict(db.getDictionary()),subplan(subplan),pathnode(pathnode)
   // Constructor
{
}
//---------------------------------------------------------------------------
DijkstraScan::~DijkstraScan()
   // Destructor
{
	if (subplan)
		delete subplan;
}
//---------------------------------------------------------------------------
unsigned DijkstraScan::first()
{
	return 0;
}
//---------------------------------------------------------------------------
unsigned DijkstraScan::next()
{
	return 0;
}
//---------------------------------------------------------------------------
class DijkstraScan::DijkstraPrefix: public DijkstraScan {
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
	void findConstraints(const QueryGraph::Filter& filter, Constraints& constr);

	/// do we have output in the next()
	bool output;
	Constraints constr;
	set<unsigned> startNodes;

public:
	/// Constructor
	DijkstraPrefix(Database& db,Database::DataOrder order,Register* value1,bool bound1,VectorRegister* value2,bool bound2,Register* value3,bool bound3,double expectedOutputCardinality,Register* pathnode,Operator* subplan,QueryGraph::Filter* pathfilter): DijkstraScan(db,order,value1,bound1,value2,bound2,value3,bound3,expectedOutputCardinality,pathnode,subplan,pathfilter) {}
	/// First tuple
	unsigned first();
	/// Next tuple
	unsigned next();
	/// Evaluate the predicate
	bool evaledge(const QueryGraph::Filter& filter,PreviousNode& node,PredicateOnNode& predicate,unsigned curNode);

};
//---------------------------------------------------------------------------
void DijkstraScan::DijkstraPrefix::findNeighbors(unsigned node) {
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
void DijkstraScan::DijkstraPrefix::findConstraints(const QueryGraph::Filter& filter, DijkstraScan::DijkstraPrefix::Constraints& constr){
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
void DijkstraScan::DijkstraPrefix::init() {
	cerr<<"slow Dijkstra"<<endl;
	// init the computation. Original value of the pathfilter set to false
	predecessors.clear();
	workingSet.clear();
	settledNodes.clear();
	shortestDistances.clear();
	startNodes.clear();

	// start nodes are defined by some condition in the subplan
	if (subplan){
		if (subplan->first()) do {
			startNodes.insert(pathnode->value);
		} while (subplan->next());
	}
	else
		startNodes.insert(value1->value);

	cerr<<"StartNodes size: "<<startNodes.size()<<endl;

	for (set<unsigned>::iterator it=startNodes.begin(); it!=startNodes.end(); it++){
		workingSet.insert(pair<unsigned, unsigned>(0,*it));
		shortestDistances[*it]=0;
		PredicateOnNode p; p.onnode=false; p.prefixlength=0;
		predicates[*it]=p;
	}
	constr.maxlen=0;
	constr.violated=false;
	if (pathfilter)
		findConstraints(*pathfilter, constr);
	cerr<<"constrains for scan "<<value1->value<<", "<<value3->value<<": "<<constr.containsonly.size()<<endl;
//	cerr<<(*constr.containsonly.begin()).first<<" "<<*((*constr.containsonly.begin()).second.begin())<<endl;
	output=false;
}
//---------------------------------------------------------------------------
unsigned DijkstraScan::DijkstraPrefix::getShortestDist(unsigned node){
	if (shortestDistances.find(node)!=shortestDistances.end())
		return shortestDistances[node];
	return ~0u;
}
//---------------------------------------------------------------------------
bool DijkstraScan::DijkstraPrefix::isHandled(unsigned node){
// the node is "black" in Dijkstra's algo
	return (settledNodes.find(node)!=settledNodes.end());
}
//---------------------------------------------------------------------------
void DijkstraScan::DijkstraPrefix::updateNeighbors(unsigned node)
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
//			workingSet.insert(pair<unsigned,unsigned>(shortDist,iter->second));
			// evaluate the predicate on prefix and last edge
			PreviousNode prevnode;
			prevnode.edge=iter->first;
			prevnode.node=node;
			PredicateOnNode p;
			p.prefixlength=predicates[node].prefixlength+1;
			p.onprefix=predicates[node].onprefix;
			if (pathfilter)
				p.onnode=evaledge(*pathfilter,prevnode,p,iter->second);
			// don't add to the working set nodes that lead to cut-off branches
			if (!pathfilter || (pathfilter && !constr.cutOff.count(iter->second)))
				workingSet.insert(pair<unsigned,unsigned>(shortDist,iter->second));

			predicates[iter->second]=p;
			predecessors[iter->second]=prevnode;
		}
	}
}
//---------------------------------------------------------------------------
unsigned DijkstraScan::DijkstraPrefix::first(){
	// value1 is always fixed
	observedOutputCardinality=0;
	init();
	return next();
}
//---------------------------------------------------------------------------
bool DijkstraScan::DijkstraPrefix::evaledge(const QueryGraph::Filter& filter,PreviousNode& node,PredicateOnNode& p,unsigned curNode){
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
				//current edge is the one we look for, update the prefix
				p.onprefix[filterid]=true;
				return true;
			}
			return false;
		case QueryGraph::Filter::Builtin_containsonly:
			filterid=filter.arg2->id;
			if (!p.onprefix[filterid]&&!startNodes.count(node.node)){
				// the prefix already does not contain the edge, no need to check further
				return false;
			}
			if (node.edge==filterid)
				p.onprefix[filterid]=true;
			else{
				p.onprefix[filterid]=false;
				constr.cutOff.insert(curNode);
			}
			return p.onprefix[filterid];
		case QueryGraph::Filter::Builtin_length:
			//compare the current prefix length with the constrain from the filter
			return (p.prefixlength<=filter.arg2->id);
		default:
			return false;
	}
}
//---------------------------------------------------------------------------
unsigned DijkstraScan::DijkstraPrefix::next()
	// Return next tuple
{
	unsigned curNode;
	while (!workingSet.empty()){
		// prepare the path
		value2->value.clear();
		curNode=(workingSet.begin())->second;
		workingSet.erase(workingSet.begin());
		settledNodes.insert(curNode);
		// next step in breadth: neighbors
		updateNeighbors(curNode);

		// this is not point-to-point search
		if (!bound3)
			value3->value=curNode;

		unsigned prev=0;
		bool output=false;
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

		if (output){
			// output it!
			observedOutputCardinality++;
			// form the path from the end to the beginning
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

    return 0;
}
//---------------------------------------------------------------------------
DijkstraScan* DijkstraScan::create(Database& db,Database::DataOrder order,Register* subject,bool subjectBound,VectorRegister* predicate,bool predicateBound,Register* object,bool objectBound,double expectedOutputCardinality,Register* pathnode,Operator* subplan,QueryGraph::Filter* pathfilter)
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
   DijkstraScan* result = new DijkstraPrefix(db,order,value1,bound1,value2,bound2,value3,bound3,expectedOutputCardinality,pathnode,subplan,pathfilter);
   return result;
}
