#include "cts/codegen/CodeGen.hpp"
#include "cts/infra/QueryGraph.hpp"
#include "cts/parser/SPARQLLexer.hpp"
#include "cts/parser/SPARQLParser.hpp"
#include "cts/plangen/PlanGen.hpp"
#include "cts/semana/SemanticAnalysis.hpp"
#include "infra/osdep/Timestamp.hpp"
#include "rts/database/Database.hpp"
#include "rts/runtime/Runtime.hpp"
#include "rts/operator/Operator.hpp"
#include "rts/operator/PlanPrinter.hpp"
#include "rts/ferrari/Graph.h"
#include "rts/ferrari/Index.h"
#include "rts/ferrari/IntervalList.h"
#include "rts/segment/FactsSegment.hpp"
#include "rts/segment/FullyAggregatedFactsSegment.hpp"
#include "rts/operator/AggregatedIndexScan.hpp"
#include "rts/segment/DictionarySegment.hpp"
#include "rts/operator/IndexScan.hpp"
#include "rts/operator/MergeJoin.hpp"
#ifdef CONFIG_LINEEDITOR
#include "lineeditor/LineInput.hpp"
#endif
#include <iostream>
#include <fstream>
#include <cstdlib>
#include <vector>
#include <algorithm>
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
bool smallAddressSpace()
   // Is the address space too small?
{
   return sizeof(void*)<8;
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
static bool contains(const vector<unsigned>& allNodes,unsigned id)
   // Is the id in the list?
{
   vector<unsigned>::const_iterator pos=lower_bound(allNodes.begin(),allNodes.end(),id);
   return ((pos!=allNodes.end())&&((*pos)==id));
}
//---------------------------------------------------------------------------
static string readInput(istream& in)
   // Read a stream into a string
{
   string result;
   while (true) {
      string s;
      getline(in,s);
      result+=s;
      if (!in.good())
         break;
      result+='\n';
   }
   return result;
}
//---------------------------------------------------------------------------
static bool readLine(string& query)
   // Read a single line
{
#ifdef CONFIG_LINEEDITOR
   // Use the lineeditor interface
   static lineeditor::LineInput editHistory(L">");
   return editHistory.readUtf8(query);
#else
   // Default fallback
   cerr << ">"; cerr.flush();
   return getline(cin,query);
#endif
}
//---------------------------------------------------------------------------
static void showHelp()
   // Show internal commands
{
   cout << "Recognized commands:" << endl
        << "help          shows this help" << endl
        << "select ...    runs a SPARQL query" << endl
        << "explain ...   shows the execution plan for a SPARQL query" << endl
        << "exit          exits the query interface" << endl;
}
//---------------------------------------------------------------------------
static void runQuery(Database& db,const string& query,bool explain)
   // Evaluate a query
{
   QueryGraph queryGraph;
   {
      // Parse the query
      SPARQLLexer lexer(query);
      SPARQLParser parser(lexer);
      try {
         parser.parse();
      } catch (const SPARQLParser::ParserException& e) {
         cerr << "parse error: " << e.message << endl;
         return;
      }

      // And perform the semantic anaylsis
      try {
         SemanticAnalysis semana(db);
         semana.transform(parser,queryGraph);
      } catch (const SemanticAnalysis::SemanticException& e) {
         cerr << "semantic error: " << e.message << endl;
         return;
      }
      if (queryGraph.knownEmpty()) {
         if (explain)
            cerr << "static analysis determined that the query result will be empty" << endl; else
            cout << "<empty result>" << endl;
         return;
      }
   }

   // Run the optimizer
   PlanGen plangen;
   Plan* plan=plangen.translate(db,queryGraph);
   if (!plan) {
      cerr << "internal error plan generation failed" << endl;
      return;
   }

   // Build a physical plan
   Runtime runtime(db);
   Operator* operatorTree=CodeGen().translate(runtime,queryGraph,plan,false);

   // Explain if requested
   if (explain) {
      DebugPlanPrinter out(runtime,false);
      operatorTree->print(out);
      if (operatorTree->first()) {
         while (operatorTree->next()) ;
      }

   } else {
      // Else execute it
      if (operatorTree->first()) {
         while (operatorTree->next()) ;
      }
   }

   delete operatorTree;
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
   	if (t.second>10)
   		predicates.push_back(t.first);
   }
   cerr<<"#predicates: "<<predicates.size()<<endl;
}
//---------------------------------------------------------------------------
static void prepareFerrari(Database& db,vector<unsigned>& predicates){
   vector<Graph*> graphs;
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

   {
      FactsSegment::Scan scan;
      unsigned current=~0u;
      bool global = true;
      unsigned seeds=5;
      unsigned minId=~0u,maxId=0;
      vector<pair<unsigned,unsigned> > edge_list;
      if (scan.first(db.getFacts(Database::Order_Predicate_Subject_Object),0,0,0)) do {
         // A new node?
         if (scan.getValue1()!=current) {
         	// add new Graph
         	if (~current&&contains(predicates,current)){
            	//cerr<<edge_list.size()<<endl;
            	cerr<<"predicate: "<<lookupId(db,current)<<endl;
            	Timestamp t1;
            	Graph* g = new Graph(edge_list, nodeCount);
            	Timestamp t2;
            	cerr<<"   time to build the graph: "<<t2-t1<<" ms"<<endl;
            	// construct an index
            	Timestamp a;
            	Index bm(g, seeds, ~0u, global);
            	bm.build();
            	Timestamp b;
            	cerr<<"   time to construct ferrari: "<<b-a<<" ms"<<endl;
            	//cerr<<"min, max: "<<minId<<" "<<maxId<<endl;
            	delete g;
         	}
            current=scan.getValue1();
         	edge_list.clear();
         	minId=~0u; maxId=0;
         }
         minId=std::min(std::min(scan.getValue3(),scan.getValue2()),minId);
         maxId=std::max(std::max(scan.getValue3(),scan.getValue2()),maxId);

         edge_list.push_back({scan.getValue2(),scan.getValue3()});
      } while (scan.next());

   }
}
//---------------------------------------------------------------------------
int main(int argc,char* argv[])
{
   // Warn first
   if (smallAddressSpace())
      cerr << "Warning: Running RDF-3X on a 32 bit system is not supported and will fail for large data sets. Please use a 64 bit system instead!" << endl;

   // Greeting
   cerr << "RDF-3X query interface" << endl
        << "(c) 2008 Thomas Neumann. Web site: http://www.mpi-inf.mpg.de/~neumann/rdf3x" << endl;

   // Check the arguments
   if ((argc!=2)&&(argc!=3)) {
      cerr << "usage: " << argv[0] << " <database> [queryfile]" << endl;
      return 1;
   }

   // Open the database
   Database db;
   if (!db.open(argv[1],true)) {
      cerr << "unable to open database " << argv[1] << endl;
      return 1;
   }

   vector<unsigned> predicates;
   findPredicates(db,predicates);
   prepareFerrari(db,predicates);

   // Execute a single query?
   if (argc==3) {
      ifstream in(argv[2]);
      if (!in.is_open()) {
         cerr << "unable to open " << argv[2] << endl;
         return 1;
      }
      string query=readInput(in);
      if (query.substr(0,8)=="explain ") {
         runQuery(db,query.substr(8),true);
      } else {
    	 Timestamp t1;
         runQuery(db,query,false);
         Timestamp t2;
         cerr<<"TIME: "<<t2-t1<<" ms"<<endl;
      }
   } else {
      // No, accept user input
      cerr << "Enter 'help' for instructions" << endl;
      while (true) {
         string query;
         if (!readLine(query))
            break;
         if (query=="") continue;

         if ((query=="quit")||(query=="exit")) {
            break;
         } else if (query=="help") {
            showHelp();
         } else if (query.substr(0,8)=="explain ") {
            runQuery(db,query.substr(8),true);
         } else {
            runQuery(db,query,false);
         }
         cout.flush();
      }
   }
}
//---------------------------------------------------------------------------
