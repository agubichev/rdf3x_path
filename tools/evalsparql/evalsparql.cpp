#include "cts/infra/QueryGraph.hpp"
#include "cts/parser/SPARQLLexer.hpp"
#include "cts/parser/SPARQLParser.hpp"
#include "cts/semana/SemanticAnalysis.hpp"
#include "rts/database/Database.hpp"
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
static void evalQuery(Database& db,const std::string& query)
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
}
//---------------------------------------------------------------------------
int main(int argc,char* argv[])
{
   // Check the arguments
   if (argc<2) {
      std::cout << "usage: " << argv[0] << " <database> [sparqlfile]" << std::endl;
      return 1;
   }

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
   evalQuery(db,query);
}
//---------------------------------------------------------------------------
