#include "cts/codegen/CodeGen.hpp"
#include "cts/infra/QueryGraph.hpp"
#include "cts/plangen/PlanGen.hpp"
#include "cts/semana/SemanticAnalysis.hpp"
#include "rts/database/Database.hpp"
#include "rts/runtime/Runtime.hpp"
#include "rts/operator/AggregatedIndexScan.hpp"
#include "rts/operator/MergeJoin.hpp"
#include "rts/operator/Operator.hpp"
#include "rts/operator/ResultsPrinter.hpp"
#include "rts/operator/Scheduler.hpp"
#include "rts/segment/AggregatedFactsSegment.hpp"
#include "rts/segment/FullyAggregatedFactsSegment.hpp"
#include "infra/osdep/MemoryMappedFile.hpp"
#include <iostream>
#include <fstream>
#include <map>
#include <algorithm>
//---------------------------------------------------------------------------
// RDF-3X
// (c) 2009 Thomas Neumann. Web site: http://www.mpi-inf.mpg.de/~neumann/rdf3x
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
static double qError(double a,double b)
   // Compute the q-error
{
   if (a<b)
      swap(a,b);
   if (b<=0.0)
      return 1000000; // infty
   return a/b;
}
//---------------------------------------------------------------------------
struct PairInfo { unsigned p1,p2,count; };
static inline bool orderChunks(const pair<const PairInfo*,const PairInfo*>& ac,const pair<const PairInfo*,const PairInfo*>& bc) { const PairInfo& a=*ac.first,&b=*bc.first; return (a.p1>b.p1)||((a.p1==b.p1)&&(a.p2>b.p2)); }
//---------------------------------------------------------------------------
static void doPairs(Database& db)
   // Find common pairs
{
   Runtime runtime(db);
   runtime.allocateRegisters(6);
   Register* S1=runtime.getRegister(0),*P1=runtime.getRegister(1); //,*O1=runtime.getRegister(2);
   Register* S2=runtime.getRegister(3),*P2=runtime.getRegister(4); //,*O2=runtime.getRegister(5);

   vector<pair<const PairInfo*,const PairInfo*> > chunks;
   {
      AggregatedIndexScan* scan1=AggregatedIndexScan::create(db,Database::Order_Subject_Predicate_Object,S1,false,P1,false,0,false,0);
      AggregatedIndexScan* scan2=AggregatedIndexScan::create(db,Database::Order_Subject_Predicate_Object,S2,false,P2,false,0,false,0);
      vector<Register*> tail1,tail2; tail1.push_back(P1); tail2.push_back(P2);
      MergeJoin join(scan1,S1,tail1,scan2,S2,tail2,0);

      const unsigned maxSize = 10000000;
      unsigned matches=0,total=0;
      map<pair<unsigned,unsigned>,unsigned> classes;
      ofstream out("bin/pairs.raw");
      const PairInfo* ofs=0;
      for (unsigned count=join.first();count;count=join.next()) {
         if (P2->value<P1->value) continue;
         matches++;
         total+=count;
         classes[pair<unsigned,unsigned>(P1->value,P2->value)]++;

         if (classes.size()==maxSize) {
            const PairInfo* start=ofs;
            for (map<pair<unsigned,unsigned>,unsigned>::const_iterator iter=classes.begin(),limit=classes.end();iter!=limit;++iter) {
               PairInfo p;
               p.p1=(*iter).first.first;
               p.p2=(*iter).first.second;
               p.count=(*iter).second,
               out.write(reinterpret_cast<char*>(&p),sizeof(p));
               ofs++;
            }
            const PairInfo* stop=ofs;
            chunks.push_back(pair<const PairInfo*,const PairInfo*>(start,stop));
            classes.clear();
         }
      }
      if (!classes.empty()) {
         const PairInfo* start=ofs;
         for (map<pair<unsigned,unsigned>,unsigned>::const_iterator iter=classes.begin(),limit=classes.end();iter!=limit;++iter) {
            PairInfo p;
            p.p1=(*iter).first.first;
            p.p2=(*iter).first.second;
            p.count=(*iter).second,
            out.write(reinterpret_cast<char*>(&p),sizeof(p));
            ofs++;
         }
         const PairInfo* stop=ofs;
         chunks.push_back(pair<const PairInfo*,const PairInfo*>(start,stop));
         classes.clear();
      }
      cout << "found " << matches << " matches in " << total << " result triples." << endl;
   }

   {
      MemoryMappedFile in;
      in.open("bin/pairs.raw");
      ofstream out("bin/pairs");
      for (vector<pair<const PairInfo*,const PairInfo*> >::iterator iter=chunks.begin(),limit=chunks.end();iter!=limit;++iter) {
         (*iter).first=reinterpret_cast<const PairInfo*>(in.getBegin())+((*iter).first-static_cast<const PairInfo*>(0));
         (*iter).second=reinterpret_cast<const PairInfo*>(in.getBegin())+((*iter).second-static_cast<const PairInfo*>(0));
      }
      make_heap(chunks.begin(),chunks.end(),orderChunks);
      PairInfo current; bool hasCurrent=false;
      unsigned classCount=0;
      while (!chunks.empty()) {
         PairInfo next=*chunks.front().first;
         pop_heap(chunks.begin(),chunks.end(),orderChunks);
         if ((++chunks.back().first)==(chunks.back().second))
            chunks.pop_back(); else
            push_heap(chunks.begin(),chunks.end(),orderChunks);

         if (hasCurrent) {
            if ((current.p1==next.p1)&&(current.p2==next.p2)) {
               current.count+=next.count;
            } else {
               out.write(reinterpret_cast<char*>(&current),sizeof(current));
               classCount++;
               current=next;
            }
         } else {
            current=next;
            hasCurrent=true;
         }
      }
      if (hasCurrent) {
         out.write(reinterpret_cast<char*>(&current),sizeof(current));
         classCount++;
      }
      cout << classCount << " predicate combinations." << endl;
   }
   remove("bin/pairs.raw");
}
//---------------------------------------------------------------------------
static void doAnalyze(Database& db)
{
   map<unsigned,unsigned> predicateFrequencies;
   {
      AggregatedFactsSegment::Scan scan;
      if (scan.first(db.getAggregatedFacts(Database::Order_Predicate_Subject_Object))) do {
         predicateFrequencies[scan.getValue1()]++;
      } while (scan.next());
   }
   cout << predicateFrequencies.size() << " predicates" << endl;

   MemoryMappedFile in;
   if (!in.open("bin/pairs")) {
      cerr << "unable to open bin/pairs" << endl;
      return;
   }
   const PairInfo* begin=reinterpret_cast<const PairInfo*>(in.getBegin());
   const PairInfo* end=reinterpret_cast<const PairInfo*>(in.getEnd());
   cout << (end-begin) << " predicate pairs in total" << endl;


   map<unsigned,unsigned> predicatePartners;
   {
      unsigned count=0;
      for (const PairInfo* iter=begin;iter!=end;++iter) {
         if (iter->count>1)
            count++;

         predicatePartners[iter->p1]++;
         if (iter->p1<iter->p2)
            predicatePartners[iter->p2]++;
      }
      cout << count << " combinations occur more than once" << endl;
   }

   unsigned t1=begin[1000000].p1,t2=begin[1000000].p2,tc=begin[1000000].count;
   cout << t1 << " " << t2 << " " << tc << endl
        << predicateFrequencies[t1] << " " << predicateFrequencies[t2] << " " << predicatePartners[t1] << " " << predicatePartners[t2] << endl
        << ((predicatePartners[t2]<predicatePartners[t1])?(predicateFrequencies[t1]/predicatePartners[t2]):(predicateFrequencies[t2]/predicatePartners[t1])) << endl;

   {
      unsigned outlierCount=0;
      for (const PairInfo* iter=begin;iter!=end;++iter) {
         unsigned expected;
         if (predicatePartners[iter->p2]<predicatePartners[iter->p1])
            expected=predicateFrequencies[iter->p1]/predicatePartners[iter->p2]; else
            expected=predicateFrequencies[iter->p2]/predicatePartners[iter->p1];
         ++expected; // round up
         if (expected<1) expected=1;
         if ((iter->count>10)&&(iter->count>2*expected)) outlierCount++;
      }
      cout << outlierCount << " combinations occur more often than expected" << endl;
   }
}
//---------------------------------------------------------------------------
int main(int argc,char* argv[])
{
   // Check the arguments
   if (argc<3) {
      cout << "usage: " << argv[0] << " <database> <predpair.hist>" << endl;
      return 1;
   }

   // Open the database
   Database db;
   if (!db.open(argv[1],true)) {
      cerr << "unable to open database " << argv[1] << endl;
      return 1;
   }

   if (string(argv[2])=="--pairs") {
      doPairs(db);
      return 0;
   } else if (string(argv[2])=="--analyze") {
      doAnalyze(db);
      return 0;
   }

   // Open the file
   ifstream in(argv[2]);
   if (!in.is_open()) {
      cerr << "unable to open " << argv[2] << endl;
      return 1;
   }

   // Skip the header
   string s;
   getline(in,s);

   // Process the lines
   cout << "# predA\tpredB\ttrue\tinputest\test\tqerrorInput\tqerrorEst" << endl;

   while (true) {
      // Read the original Data
      unsigned preda,predb,freqa,freqb;
      double sela,selb,trueCard,estCard,e1,e2;
      if (!(in >> preda >> predb >> freqa >> freqb >> sela >> selb >> trueCard >> estCard >> e1 >> e2))
         break;

      // Build a query select ?v1 ?v2 { ?v0 preda ?v1. ?v0 predb ?v2 }
      QueryGraph qg;
      qg.addProjection(1); qg.addProjection(2);
      {
         QueryGraph::Node n1;
         n1.subject=0; n1.constSubject=false;
         n1.predicate=preda; n1.constPredicate=true;
         n1.object=1; n1.constObject=false;
         qg.getQuery().nodes.push_back(n1);
      }
      {
         QueryGraph::Node n2;
         n2.subject=0; n2.constSubject=false;
         n2.predicate=predb; n2.constPredicate=true;
         n2.object=2; n2.constObject=false;
         qg.getQuery().nodes.push_back(n2);
      }
      qg.constructEdges();

      // Run the optimizer
      PlanGen plangen;
      Plan* plan=plangen.translate(db,qg);
      if (!plan) {
         cerr << "plan generation failed" << endl;
         return 1;
      }
      Operator::disableSkipping=true;

      // Build a physical plan
      Runtime runtime(db);
      Operator* operatorTree=CodeGen().translate(runtime,qg,plan,true);
      Operator* realRoot=dynamic_cast<ResultsPrinter*>(operatorTree)->getInput();

      // And execute it
      Scheduler scheduler;
      scheduler.execute(realRoot);

      // Output the counts
      trueCard=realRoot->getObservedOutputCardinality();
      cout << preda << "\t" << predb << "\t" << trueCard << "\t" << estCard << "\t" << realRoot->getExpectedOutputCardinality() << "\t" << qError(estCard,trueCard) << "\t" << qError(realRoot->getExpectedOutputCardinality(),trueCard) << endl;

      delete operatorTree;
   }
}
//---------------------------------------------------------------------------
