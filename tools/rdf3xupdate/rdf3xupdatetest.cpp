#include "cts/parser/TurtleParser.hpp"
#include "rts/database/Database.hpp"
#include "rts/runtime/BulkOperation.hpp"
#include "cts/codegen/CodeGen.hpp"
#include "cts/infra/QueryGraph.hpp"
#include "cts/parser/SPARQLLexer.hpp"
#include "cts/parser/SPARQLParser.hpp"
#include "cts/plangen/PlanGen.hpp"
#include "cts/semana/SemanticAnalysis.hpp"
#include "rts/operator/Operator.hpp"
#include "rts/runtime/Runtime.hpp"
#include "rts/runtime/TemporaryDictionary.hpp"
#include <iostream>
#include <fstream>
#include <string.h>
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
bool smallAddressSpace()
   // Is the address space too small?
{
   return sizeof(void*)<8;
}
//---------------------------------------------------------------------------
static void processQuery(const string& query, Database& db, Runtime& runtime)
// Process a query
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
        SemanticAnalysis semana(db);
        semana.transform(parser,queryGraph);
        if (queryGraph.knownEmpty()) {
            // cerr << "<empty result>" << endl;
            return;
        }
    }

    // Run the optimizer
    PlanGen plangen;
    Plan* plan=plangen.translate(db,queryGraph);
    if (!plan) {
        cerr << "plan generation failed" << endl;
        return;
    }

    Operator* operatorTree=CodeGen().translate(runtime,queryGraph,plan,false);

    if (operatorTree->first()) {
       while (operatorTree->next()) ;
    }

    delete operatorTree;
}
//---------------------------------------------------------------------------
int main(int argc,char* argv[])
{
   // Warn first
   if (smallAddressSpace())
      cerr << "Warning: Running RDF-3X on a 32 bit system is not supported and will fail for large data sets. Please use a 64 bit system instead!" << endl;

   // Greeting
   cerr << "RDF-3X turtle updater" << endl
        << "(c) 2009 Thomas Neumann. Web site: http://www.mpi-inf.mpg.de/~neumann/rdf3x" << endl;

   // Check the arguments
   if (argc<2) {
      cerr << "usage: " << argv[0] << " <database>  <input>" << endl
           << "without input file data is read from stdin" << endl;
      return 1;
   }

   // Open the database
   Database db;
   if (!db.open(argv[1])) {
      cerr << "unable to open " << argv[1] << endl;
      return 1;
   }

   // And incorporate changes
   DifferentialIndex diff(db);
   TemporaryDictionary tmpdict(diff);
   ifstream in(argv[2]);
   if (!in.is_open()) {
      cerr << "unable to open " << argv[2] << endl;
      return 1;
   }

   BulkOperation bulk(diff), bulk_del(diff), bulk1(diff);
   TurtleParser parser(in);
   string subject,predicate,object,objectSubType;
   Type::ID objectType;
      if (!parser.parse(subject,predicate,object,objectType,objectSubType))
         cerr<<"could not parse the triple!"<<endl;
      bulk.insert(subject,predicate,object,objectType,objectSubType);
     // bulk.markDeleted();
      bulk.commit();

      bulk_del.insert(subject,predicate,object,objectType,objectSubType);
      bulk_del.markDeleted();
      bulk_del.commit();


      Runtime runtime(db,&diff, &diff.getTemporaryDictionary());
      processQuery("select * where {?c ?v ?b}",db,runtime);

      diff.sync();
      processQuery("select * where {?c ?v ?b}",db,runtime);

}
//---------------------------------------------------------------------------
