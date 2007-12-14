#include "cts/codegen/CodeGen.hpp"
#include "cts/infra/QueryGraph.hpp"
#include "cts/parser/SPARQLLexer.hpp"
#include "cts/parser/SPARQLParser.hpp"
#include "cts/semana/SemanticAnalysis.hpp"
#include "infra/osdep/Timestamp.hpp"
#include "rts/database/Database.hpp"
#include "rts/runtime/Runtime.hpp"
#include "rts/operator/Operator.hpp"
#include <iostream>
#include <fstream>
//---------------------------------------------------------------------------
static std::string readInput(std::istream& in)
   // Read the input query
{
   std::string result;
   while (true) {
      std::string s;
      std::getline(in,s);
      if (!in.good())
         break;
      result+=s;
      result+='\n';
   }
   return result;
}
//---------------------------------------------------------------------------
static void evalQuery(Database& db,const std::string& query,bool silent)
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
         std::cout << "parse error: " << e.message << std::endl;
         return;
      }
      if (parser.patternsBegin()==parser.patternsEnd()) {
         std::cout << "<empty tuple produced>" << std::endl;
         return;
      }

      // And perform the semantic anaylsis
      SemanticAnalysis semana(db);
      semana.transform(parser,queryGraph);
      if (queryGraph.nodesBegin()==queryGraph.nodesEnd()) {
         std::cout << "<empty result>" << std::endl;
         return;
      }
   }

   // Build a physical plan
   Runtime runtime(db);
   Operator* plan=CodeGen::translate(runtime,queryGraph,silent);

   // plan->print();

   // And execute it
   Timestamp start;
   if (plan->first()) {
      while (plan->next());
   }
   Timestamp stop;
   std::cout << "Execution time: " << (stop-start) << " ms" << std::endl;


   delete plan;
}
//---------------------------------------------------------------------------
int main(int argc,char* argv[])
{
   // Check the arguments
   if (argc<2) {
      std::cout << "usage: " << argv[0] << " <database> [sparqlfile] [--silent]" << std::endl;
      return 1;
   }
   bool silent=false;
   if ((argc>3)&&(std::string(argv[3])=="--silent"))
      silent=true;

   // Open the database
   Database db;
   if (!db.open(argv[1])) {
      std::cout << "unable to open database " << argv[1] << std::endl;
      return 1;
   }

   // Retrieve the query
   std::string query;
   if (argc>2) {
      std::ifstream in(argv[2]);
      if (!in.is_open()) {
         std::cout << "unable to open " << argv[2] << std::endl;
         return 1;
      }
      query=readInput(in);
   } else {
      query=readInput(std::cin);
   }

   // And evaluate it
   evalQuery(db,query,silent);
}
//---------------------------------------------------------------------------
