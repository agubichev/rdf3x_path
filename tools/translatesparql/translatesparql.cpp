#include "cts/infra/QueryGraph.hpp"
#include "cts/semana/SemanticAnalysis.hpp"
#include "cts/parser/SPARQLLexer.hpp"
#include "cts/parser/SPARQLParser.hpp"
#include "rts/database/Database.hpp"
#include <iostream>
#include <fstream>
#include <cstdlib>
#include <map>
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
static string readInput(istream& in)
   // Read the input query
{
   string result;
   while (true) {
      string s;
      getline(in,s);
      if (!in.good())
         break;
      result+=s;
      result+='\n';
   }
   return result;
}
//---------------------------------------------------------------------------
static string buildFactsAttribute(unsigned id,const char* attribute)
    // Build the attribute name for a facts attribute
{
   char buffer[50];
   snprintf(buffer,sizeof(buffer),"f%u.%s",id,attribute);
   return string(buffer);
}
//---------------------------------------------------------------------------
static void dumpMonetDB(const QueryGraph& query)
   // Dump the query
{
   cout << "select ";
   {
      unsigned id=0;
      for (QueryGraph::projection_iterator iter=query.projectionBegin(),limit=query.projectionEnd();iter!=limit;++iter) {
         if (id) cout << ",";
         cout << "s" << id << ".value";
         id++;
      }
   }
   cout << endl;
   cout << "from (" << endl;
   map<unsigned,string> representative;
   {
      unsigned id=0;
      for (vector<QueryGraph::Node>::const_iterator iter=query.getQuery().nodes.begin(),limit=query.getQuery().nodes.end();iter!=limit;++iter) {
         if ((!(*iter).constSubject)&&(!representative.count((*iter).subject)))
            representative[(*iter).subject]=buildFactsAttribute(id,"subject");
         if ((!(*iter).constPredicate)&&(!representative.count((*iter).predicate)))
            representative[(*iter).predicate]=buildFactsAttribute(id,"predicate");
         if ((!(*iter).constObject)&&(!representative.count((*iter).object)))
            representative[(*iter).object]=buildFactsAttribute(id,"object");
         ++id;
      }
   }
   cout << "   select ";
   {
      unsigned id=0;
      for (QueryGraph::projection_iterator iter=query.projectionBegin(),limit=query.projectionEnd();iter!=limit;++iter) {
         if (id) cout << ",";
         cout << representative[*iter] << " as r" << id;
         id++;
      }
   }
   cout << endl;
   cout << "   from ";
   {
      unsigned id=0;
      for (vector<QueryGraph::Node>::const_iterator iter=query.getQuery().nodes.begin(),limit=query.getQuery().nodes.end();iter!=limit;++iter) {
         if (id) cout << ",";
         if ((*iter).constPredicate)
            cout << "p" << (*iter).predicate << " f" << id; else
            cout << "allproperties f" << id;
         ++id;
      }

   }
   cout << endl;
   cout << "   where ";
   {
      unsigned id=0; bool first=true;
      for (vector<QueryGraph::Node>::const_iterator iter=query.getQuery().nodes.begin(),limit=query.getQuery().nodes.end();iter!=limit;++iter) {
         string s=buildFactsAttribute(id,"subject"),p=buildFactsAttribute(id,"predicate"),o=buildFactsAttribute(id,"object");
         if ((*iter).constSubject) {
            if (first) first=false; else cout << " and ";
            cout << s << "=" << (*iter).subject;
         } else if (representative[(*iter).subject]!=s) {
            if (first) first=false; else cout << " and ";
            cout << s << "=" << representative[(*iter).subject];
         }
         if ((*iter).constPredicate) {
         } else if (representative[(*iter).predicate]!=p) {
            if (first) first=false; else cout << " and ";
            cout << p << "=" << representative[(*iter).predicate];
         }
         if ((*iter).constObject) {
            if (first) first=false; else cout << " and ";
            cout << o << "=" << (*iter).object;
         } else if (representative[(*iter).object]!=o) {
            if (first) first=false; else cout << " and ";
            cout << o << "=" << representative[(*iter).object];
         }
         ++id;
      }
   }
   cout << endl << ") facts";
   {
      unsigned id=0;
      for (QueryGraph::projection_iterator iter=query.projectionBegin(),limit=query.projectionEnd();iter!=limit;++iter) {
         cout << ",strings s" << id;
         id++;
      }
   }
   cout << endl;
   cout << "where ";
   {
      unsigned id=0;
      for (QueryGraph::projection_iterator iter=query.projectionBegin(),limit=query.projectionEnd();iter!=limit;++iter) {
         if (id) cout << " and ";
         cout << "s" << id << ".id=facts.r" << id;
         id++;
      }
   }
   cout << ";" << endl;
}
//---------------------------------------------------------------------------
static string databaseName(const char* fileName)
   // Guess the database name from the file name
{
   const char* start=fileName;
   for (const char* iter=fileName;*iter;++iter)
      if ((*iter)=='/')
         start=iter+1;
   const char* stop=start;
   while ((*stop)&&((*stop)!='.'))
      ++stop;
   return string(start,stop);
}
//---------------------------------------------------------------------------
static void dumpPostgres(QueryGraph& query,const string& schema)
   // Dump a postgres query
{
   cout << "\\timing" << endl;
   cout << "select ";
   {
      unsigned id=0;
      for (QueryGraph::projection_iterator iter=query.projectionBegin(),limit=query.projectionEnd();iter!=limit;++iter) {
         if (id) cout << ",";
         cout << "s" << id << ".value";
         id++;
      }
   }
   cout << endl;
   cout << "from (" << endl;
   map<unsigned,string> representative;
   {
      unsigned id=0;
      for (vector<QueryGraph::Node>::const_iterator iter=query.getQuery().nodes.begin(),limit=query.getQuery().nodes.end();iter!=limit;++iter) {
         if ((!(*iter).constSubject)&&(!representative.count((*iter).subject)))
            representative[(*iter).subject]=buildFactsAttribute(id,"subject");
         if ((!(*iter).constPredicate)&&(!representative.count((*iter).predicate)))
            representative[(*iter).predicate]=buildFactsAttribute(id,"predicate");
         if ((!(*iter).constObject)&&(!representative.count((*iter).object)))
            representative[(*iter).object]=buildFactsAttribute(id,"object");
         ++id;
      }
   }
   cout << "   select ";
   {
      unsigned id=0;
      for (QueryGraph::projection_iterator iter=query.projectionBegin(),limit=query.projectionEnd();iter!=limit;++iter) {
         if (id) cout << ",";
         cout << representative[*iter] << " as r" << id;
         id++;
      }
   }
   cout << endl;
   cout << "   from ";
   {
      unsigned id=0;
      for (vector<QueryGraph::Node>::const_iterator iter=query.getQuery().nodes.begin(),limit=query.getQuery().nodes.end();iter!=limit;++iter) {
         if (id) cout << ",";
         cout << schema << ".facts f" << id;
         ++id;
      }

   }
   cout << endl;
   cout << "   where ";
   {
      unsigned id=0; bool first=true;
      for (vector<QueryGraph::Node>::const_iterator iter=query.getQuery().nodes.begin(),limit=query.getQuery().nodes.end();iter!=limit;++iter) {
         string s=buildFactsAttribute(id,"subject"),p=buildFactsAttribute(id,"predicate"),o=buildFactsAttribute(id,"object");
         if ((*iter).constSubject) {
            if (first) first=false; else cout << " and ";
            cout << s << "=" << (*iter).subject;
         } else if (representative[(*iter).subject]!=s) {
            if (first) first=false; else cout << " and ";
            cout << s << "=" << representative[(*iter).subject];
         }
         if ((*iter).constPredicate) {
            if (first) first=false; else cout << " and ";
            cout << p << "=" << (*iter).predicate;
         } else if (representative[(*iter).predicate]!=p) {
            if (first) first=false; else cout << " and ";
            cout << p << "=" << representative[(*iter).predicate];
         }
         if ((*iter).constObject) {
            if (first) first=false; else cout << " and ";
            cout << o << "=" << (*iter).object;
         } else if (representative[(*iter).object]!=o) {
            if (first) first=false; else cout << " and ";
            cout << o << "=" << representative[(*iter).object];
         }
         ++id;
      }
   }
   cout << endl << ") facts";
   {
      unsigned id=0;
      for (QueryGraph::projection_iterator iter=query.projectionBegin(),limit=query.projectionEnd();iter!=limit;++iter) {
         cout << "," << schema <<".strings s" << id;
         id++;
      }
   }
   cout << endl;
   cout << "where ";
   {
      unsigned id=0;
      for (QueryGraph::projection_iterator iter=query.projectionBegin(),limit=query.projectionEnd();iter!=limit;++iter) {
         if (id) cout << " and ";
         cout << "s" << id << ".id=facts.r" << id;
         id++;
      }
   }
   cout << ";" << endl;
}
//---------------------------------------------------------------------------
int main(int argc,char* argv[])
{
   // Check the arguments
   if (argc<3) {
      cout << "usage: " << argv[0] << " <database> <type> [sparqlfile]" << endl;
      return 1;
   }
   bool monetDB=false;
   if (string(argv[2])=="postgres")
      monetDB=false; else
   if (string(argv[2])=="monetdb")
      monetDB=true; else {
      cout << "unknown method " << argv[2] << endl;
      return 1;
   }

   // Open the database
   Database db;
   if (!db.open(argv[1])) {
      cout << "unable to open database " << argv[1] << endl;
      return 1;
   }

   // Retrieve the query
   string query;
   if (argc>3) {
      ifstream in(argv[3]);
      if (!in.is_open()) {
         cout << "unable to open " << argv[3] << endl;
         return 1;
      }
      query=readInput(in);
   } else {
      query=readInput(cin);
   }

   // Parse it
   QueryGraph queryGraph;
   {
      // Parse the query
      SPARQLLexer lexer(query);
      SPARQLParser parser(lexer);
      try {
         parser.parse();
      } catch (const SPARQLParser::ParserException& e) {
         cout << "parse error: " << e.message << endl;
         return 1;
      }

      // And perform the semantic anaylsis
      SemanticAnalysis semana(db);
      semana.transform(parser,queryGraph);
      if (queryGraph.knownEmpty()) {
         cout << "<empty result>" << endl;
         return 1;
      }
   }

   // And dump it
   if (monetDB)
      dumpMonetDB(queryGraph); else
      dumpPostgres(queryGraph,databaseName(argv[1]));
}
//---------------------------------------------------------------------------
