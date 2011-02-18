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
class DijkstraScan {
public:
	/// comparing nodes by distance to source
	template <typename T> struct CompareByDistance {
	    bool operator()(const pair<T, T> &lhs, const pair<T, T> &rhs) {
	        if (lhs.first == rhs.first)
	            return lhs.second < rhs.second;
	        return lhs.first < rhs.first;
	    }
	};

private:
	/// struct to keep predecessors info
	struct PreviousNode{
		/// node id
		unsigned node;
		/// edge id
		unsigned edge;
	};
	/// Neighbors
	vector<pair<unsigned,unsigned> > neighbors;
	/// Iterator on neighbors
	vector<pair<unsigned, unsigned> >::const_iterator iter;
	/// Dijkstra's settled nodes
	set<unsigned> settledNodes;
	/// Dijkstra's predecessors
	map<unsigned,PreviousNode> predecessors;
	/// Dijkstra's working set
	set<pair<unsigned, unsigned>,CompareByDistance<unsigned> > workingSet;
	/// Dijkstra's shortest distances
	map<unsigned,unsigned> shortestDistances;
	/// Find neighbors (according to scan order) of given order
	void findNeighbors(unsigned node);
	/// Dijkstra's init
	void init(unsigned source);
	/// is the node handled already?
	bool isHandled(unsigned node);
	/// get the shortest dist from the start to the node
	unsigned getShortestDist(unsigned node);

	void getNeighbors();
	void updateNeighbors(unsigned node,unsigned nodeIndex);

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
	Database::DataOrder order;

public:
	/// Constructor
	DijkstraScan(Database& db,Database::DataOrder order, unsigned dbSize): db(db), order(order) {
		for (unsigned i=0; i<dbSize; i++)
			shortestDistances[i]=~0u;
	}

	void computeSP(unsigned source);

	map<unsigned,unsigned>& getSPMap(){return shortestDistances;}
};
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
void DijkstraScan::getNeighbors(){
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
void DijkstraScan::init(unsigned source) {
	// init the computation
	predecessors.clear();
	workingSet.clear();
	settledNodes.clear();
	shortestDistances.clear();
	curNodes.clear();
	workingSet.insert(pair<unsigned, unsigned>(0,source));
	shortestDistances[source]=0;
	curNodes.insert(source);
	curnodes_iter=curNodes.begin();
	curIndex=0;
	workingsetmax=0;
}
//---------------------------------------------------------------------------
unsigned DijkstraScan::getShortestDist(unsigned node){
	if (shortestDistances.find(node)!=shortestDistances.end())
		return shortestDistances[node];
	return ~0u;
}
//---------------------------------------------------------------------------
bool DijkstraScan::isHandled(unsigned node){
// the node is "black" in Dijkstra's algo
	return (settledNodes.find(node)!=settledNodes.end());
}
//---------------------------------------------------------------------------
void DijkstraScan::updateNeighbors(unsigned node,unsigned nodeindex){
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

			workingSet.insert(pair<unsigned,unsigned>(shortDist,iter->second));
			predecessors[iter->second]=prevnode;
		}
	}
}
//---------------------------------------------------------------------------
void DijkstraScan::computeSP(unsigned source){

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
					if (!isHandled((n.connections[i])[j].second))
						curNodes.insert((n.connections[i])[j].second);
				}
			}
			n.connections.clear();
			n.ids.clear();
			curIndex=0;

			Timestamp t1;
			getNeighbors();
			Timestamp t2;
//			cerr<<"getting neighbors: "<<t2-t1<<" ms"<<endl;
//			cerr<<"curNodes: "<<curNodes.size()<<endl;
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

			updateNeighbors(curNode,curIndex);

			if (workingSet.size()>workingsetmax)
				workingsetmax=workingSet.size();
			curIndex++;

			settledNodes.insert(curNode);
		}
	}

//	cerr<<"settled nodes: "<<settledNodes.size()<<endl;
//	cerr<<"max working set size: "<<workingsetmax<<endl;
//	for (unsigned j=0; j<n.connections.size(); j++){
//		cerr<<"node "<<lookupLiteral(db,n.ids[j])<<" has "<<n.connections[j].size()<<" neighbors"<<endl;
//	//		for (unsigned k=0; k<n.connections[j].size(); k++)
//	//			cerr<<"    "<<lookupLiteral(db,(n.connections[j])[k].first)<<" "<<lookupLiteral(db,(n.connections[j])[k].second)<<endl;
//	}

}
//---------------------------------------------------------------------------
/// lookup the name for internal id
int main(int argc,char* argv[]){
	if ((argc<3)) {
	    cout << "usage: " << argv[0] << " <database> <itrations>" << endl;
	    return 1;
	}

    // Open the database
	Database db;
	if (!db.open(argv[1])) {
	   cout << "unable to open " << argv[1] << endl;
	   return 1;
	}

	srand(time(0));


	unsigned dbSize=numberOfNodes(db);
	unsigned iter=atoi(argv[2]);

	set<unsigned> nodes;
	vector<double> centrality;
	centrality.resize(dbSize);

	set<pair<double,unsigned>, DijkstraScan::CompareByDistance<double> > centrality_rank;

	for (unsigned i=0; i<iter; i++){
		unsigned source=rand()%dbSize;
		if (nodes.count(source)){
			i--;
			cerr<<"continuing"<<endl;
			continue;

		}
		const char* start=0,*stop=0; Type::ID type; unsigned subType;
		db.getDictionary().lookupById(source,start,stop,type,subType);
		if (type == Type::Literal){
			i--;
			continue;
		}

		if (numberOfNeighbors(source,db,Database::Order_Subject_Predicate_Object)<10){
			i--;
//			cerr<<"too few neighbors"<<endl;
			continue;
		}
		DijkstraScan scan(db, Database::Order_Subject_Predicate_Object, dbSize);


		scan.computeSP(source);

		map<unsigned,unsigned> &dist=scan.getSPMap();

		for (map<unsigned, unsigned>::iterator it=dist.begin(); it!=dist.end(); it++)
			nodes.insert(it->first);

//		for (unsigned i=0; i<centrality.size(); i++){
//				centrality[i]+=dist[i];
//		}

//	for (map<unsigned,unsigned>::iterator it=dist.begin(); it!=dist.end(); it++){
//		cerr<<it->first<<" "<<it->second<<endl;
//	}

		cerr<<"source: "<<source<<", nodes visited: "<<dist.size()<<", overall nodes: "<<nodes.size()<<endl;
	}

	for (unsigned i=0; i<centrality.size(); i++){
		centrality[i] /= iter;
		centrality_rank.insert(pair<double,unsigned>(centrality[i],i));
	}

	unsigned i=0;
	for (set<pair<double,unsigned>, DijkstraScan::CompareByDistance<double> >::iterator it = centrality_rank.begin(); it!=centrality_rank.end();it++){
		i++;
		cerr<<it->second<<" "<<it->first<<endl;
		if (i>10)
			break;
	}



}
