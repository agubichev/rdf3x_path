#include "cts/codegen/CodeGen.hpp"
#include "cts/infra/QueryGraph.hpp"
#include "cts/parser/SPARQLLexer.hpp"
#include "cts/parser/SPARQLParser.hpp"
#include "cts/plangen/PlanGen.hpp"
#include "cts/semana/SemanticAnalysis.hpp"
#include "infra/osdep/Timestamp.hpp"
#include "rts/database/Database.hpp"
#include "rts/runtime/Runtime.hpp"
#include "rts/runtime/TemporaryDictionary.hpp"
#include "rts/operator/Operator.hpp"
#include "rts/operator/ResultsPrinter.hpp"
#include <iostream>
#include <fstream>
#include <cstdlib>
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
static void writeHeader(const QueryGraph& graph,const SPARQLParser& parser)
   // Write the query header
{
   bool first=true;
   for (QueryGraph::projection_iterator iter=graph.projectionBegin(),limit=graph.projectionEnd();iter!=limit;++iter) {
      string name=parser.getVariableName(*iter);
      if (first)
         first=false; else
         cout << ' ';
      for (string::const_iterator iter2=name.begin(),limit2=name.end();iter2!=limit2;++iter2) {
         char c=(*iter2);
         if ((c==' ')||(c=='\n')||(c=='\\'))
            cout << '\\';
         cout << c;
      }
   }
   if ((graph.getDuplicateHandling()==QueryGraph::CountDuplicates)||(graph.getDuplicateHandling()==QueryGraph::ShowDuplicates))
      cout << " count";
   cout << endl;
}
//---------------------------------------------------------------------------
static void runQuery(Database& db,const string& query)
   // Evaluate a query
{
   QueryGraph queryGraph;
   // Parse the query
   SPARQLLexer lexer(query);
   SPARQLParser parser(lexer);
   try {
      parser.parse();
   } catch (const SPARQLParser::ParserException& e) {
      cout << "parse error: " << e.message << endl;
      return;
   }

   // And perform the semantic anaylsis
   try {
      SemanticAnalysis semana(db);
      semana.transform(parser,queryGraph);
   } catch (const SemanticAnalysis::SemanticException& e) {
      cout << "semantic error: " << e.message << endl;
      return;
   }
   if (queryGraph.knownEmpty()) {
      cout << "ok" << endl;
      writeHeader(queryGraph,parser);
      cout << "\\." << endl;
      cout.flush();
      return;
   }

   // Run the optimizer
   PlanGen plangen;
   Plan* plan=plangen.translate(db,queryGraph);
   if (!plan) {
      cout << "internal error plan generation failed" << endl;
      return;
   }

   // Build a physical plan
   TemporaryDictionary tempDict(db.getDictionary());
   Runtime runtime(db,0,&tempDict);
   Operator* operatorTree=CodeGen().translate(runtime,queryGraph,plan,false);
   dynamic_cast<ResultsPrinter*>(operatorTree)->setOutputMode(ResultsPrinter::Embedded);

   // Execute it
   cout << "ok" << endl;
   writeHeader(queryGraph,parser);
   if (operatorTree->first()) {
      while (operatorTree->next()) ;
   }
   cout << "\\." << endl;
   cout.flush();

   delete operatorTree;
}
//---------------------------------------------------------------------------
int main(int argc,char* argv[])
{
   cout.sync_with_stdio(false);

   // Check the arguments
   if (argc!=2) {
      cerr << "usage: " << argv[0] << " <database>" << endl;
      return 1;
   }

   // Open the database
   Database db;
   if (!db.open(argv[1],true)) {
      cout << "unable to open database " << argv[1] << endl;
      return 1;
   }
   cout << "RDF-3X protocol 1" << endl;

   // And process queries
   while (true) {
      string query;
      while (true) {
         char c;
         if (!(cin.get(c))) return 0;
         if (c=='\n') break;
         if (c=='\\') {
            if (!(cin.get(c))) return 0;
         }
         query+=c;
      }
      runQuery(db,query);
      cout.flush();
   }
}
//---------------------------------------------------------------------------
