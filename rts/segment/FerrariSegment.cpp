#include "rts/segment/FerrariSegment.hpp"
#include "rts/database/Database.hpp"
#include "rts/database/DatabaseBuilder.hpp"
#include "rts/database/DatabasePartition.hpp"
#include "rts/operator/MergeJoin.hpp"
#include "rts/operator/AggregatedIndexScan.hpp"
#include "rts/segment/FactsSegment.hpp"
#include "rts/segment/DictionarySegment.hpp"
#include "rts/segment/FullyAggregatedFactsSegment.hpp"
#include "rts/ferrari/Graph.hpp"
#include "rts/ferrari/Index.hpp"
#include "infra/osdep/Timestamp.hpp"
#include "rts/runtime/Runtime.hpp"
#include <algorithm>
#include <vector>
#include <iostream>
//---------------------------------------------------------------------------
// RDF-3X
// (c) 2013 Andrey Gubichev. Web site: http://www.mpi-inf.mpg.de/~neumann/rdf3x
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
static bool contains(const vector<unsigned>& allNodes,unsigned id)
   // Is the id in the list?
{
   vector<unsigned>::const_iterator pos=lower_bound(allNodes.begin(),allNodes.end(),id);
   return ((pos!=allNodes.end())&&((*pos)==id));
}
//---------------------------------------------------------------------------
static string lookupId(Database& db,unsigned id)
   // Lookup a string id
{
   const char* start=0,*stop=0; Type::ID type; unsigned subType;
   db.getDictionary().lookupById(id,start,stop,type,subType);
   return string(start,stop);
}
//---------------------------------------------------------------------------
static void findPredicates(Database& db, vector<unsigned>& predicates){
	predicates.clear();
   Register ls,lo,lp,rs,ro,rp;
   ls.reset(); lp.reset(); lo.reset(); rs.reset(); rp.reset(); ro.reset();
   AggregatedIndexScan* scan1=AggregatedIndexScan::create(db,Database::Order_Object_Predicate_Subject,0,false,&lp,false,&lo,false,0);
   AggregatedIndexScan* scan2=AggregatedIndexScan::create(db,Database::Order_Subject_Predicate_Object,&rs,false,&rp,false,0,false,0);
   vector<Register*> lt,rt; lt.push_back(&lp); rt.push_back(&rp);
   MergeJoin join(scan1,&lo,lt,scan2,&rs,rt,0);

   map<unsigned,unsigned> predCount;
   if (join.first()) do {
      if (lp.value==rp.value){
      	predCount[lp.value]++;
      }
   } while (join.next());

   for (auto t:predCount){
   	cerr<<lookupId(db,t.first)<<" "<<t.second<<endl;
   	predicates.push_back(t.first);
   }
   cerr<<"#predicates: "<<predicates.size()<<endl;
}
//---------------------------------------------------------------------------
FerrariSegment::FerrariSegment(DatabasePartition& partition)
   : Segment(partition)
   // Constructor
{
}
//---------------------------------------------------------------------------
Segment::Type FerrariSegment::getType() const
   // Get the type
{
   return Segment::Type_Ferrari;
}
//---------------------------------------------------------------------------
void FerrariSegment::computeFerrari(Database& db){
   unsigned nodeCount=0;
   {
      FullyAggregatedFactsSegment::Scan scan;
      if (scan.first(db.getFullyAggregatedFacts(Database::Order_Subject_Predicate_Object))) do {
      	if (scan.getValue1()>nodeCount)
      		nodeCount=scan.getValue1();
      } while (scan.next());
   }
   nodeCount++;
   cerr<<"nodes: "<<nodeCount<<endl;
   vector<unsigned> predicates;
   findPredicates(db, predicates);

   {
      FactsSegment::Scan scan;
      unsigned current=~0u;
      bool global = true;
      unsigned seeds=5;
      vector<pair<unsigned,unsigned> > edge_list;
      if (scan.first(db.getFacts(Database::Order_Predicate_Subject_Object),0,0,0)) do {
         // A new node?
         if (scan.getValue1()!=current) {
         	// add new Graph
         	if (~current&&contains(predicates,current)){
            	cerr<<"predicate: "<<lookupId(db,current)<<" "<<current<<endl;
            	Timestamp t1;
            	Graph* g = new Graph(edge_list, nodeCount);
            	Timestamp t2;
            	cerr<<"   time to build the graph: "<<t2-t1<<" ms"<<endl;
            	// construct an index
            	Timestamp a;
            	Index *bm = new Index(g, seeds, ~0u, global);
            	bm->build();
         	}
            current=scan.getValue1();
         	edge_list.clear();
         }
         edge_list.push_back({scan.getValue2(),scan.getValue3()});
      }while (scan.next());
   }

}
