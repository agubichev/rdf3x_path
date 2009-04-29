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
#include "rts/operator/Scheduler.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
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
namespace {
//---------------------------------------------------------------------------
/// Print out cardinality information
class PredicateCollector : public PlanPrinter
{
   private:
   /// The output
   std::ostream& out;
   /// The runtime
   Runtime& runtime;
   /// Currently supported
   vector<bool> supported;
   /// The cardinalities
   vector<pair<unsigned,unsigned> > cardinalities;
   /// The relations
   vector<set<unsigned> > relations;
   /// The predicates
   vector<set<pair<const Register*,unsigned> > > predicates;
   /// The previous predicates
   vector<set<pair<const Register*,unsigned> > > previousPredicates;
   /// Unresolved equal conditions
   vector<set<pair<const Register*,const Register*> > > equal;
   /// Previous unresolved equal conditions
   vector<set<pair<const Register*,const Register*> > > previousEqual;
   /// The current operator characteristics
   string operatorName,operatorArgument;
   /// The argument slot
   unsigned argSlot;
   /// The number of scans
   unsigned scanCount;
   /// Register bindings
   map<const Register*,unsigned> bindings;

   public:
   /// Constructor
   PredicateCollector(std::ostream& out,Runtime& runtime);

   /// Begin a new operator
   void beginOperator(const std::string& name,unsigned expectedOutputCardinality,unsigned observedOutputCardinality);
   /// Add an operator argument annotation
   void addArgumentAnnotation(const std::string& argument);
   /// Add a scan annotation
   void addScanAnnotation(const Register* reg,bool bound);
   /// Add a predicate annotate
   void addEqualPredicateAnnotation(const Register* reg1,const Register* reg2);
   /// Add a materialization annotation
   void addMaterializationAnnotation(const std::vector<Register*>& regs);
   /// Add a generic annotation
   void addGenericAnnotation(const std::string& text);
   /// Close the current operator
   void endOperator();

   /// Format a register (for generic annotations)
   std::string formatRegister(const Register* reg);
   /// Format a constant value (for generic annotations)
   std::string formatValue(unsigned value);
};
//---------------------------------------------------------------------------
PredicateCollector::PredicateCollector(std::ostream& out,Runtime& runtime)
   : out(out),runtime(runtime),scanCount(0)
   // Constructor
{
}
//---------------------------------------------------------------------------
void PredicateCollector::beginOperator(const std::string& name,unsigned expectedOutputCardinality,unsigned observedOutputCardinality)
   // Begin a new operator
{
   supported.push_back(true);
   relations.push_back(set<unsigned>());
   predicates.push_back(set<pair<const Register*,unsigned> >());
   previousPredicates.push_back(set<pair<const Register*,unsigned> >());
   equal.push_back(set<pair<const Register*,const Register*> >());
   previousEqual.push_back(set<pair<const Register*,const Register*> >());
   cardinalities.push_back(pair<unsigned,unsigned>(expectedOutputCardinality,observedOutputCardinality));
   operatorName=name;

   if ((name=="IndexScan")||(name=="AggregatedIndexScan")||(name=="FullyAggregatedIndexScan")) {
      relations.back().insert(++scanCount);
   } else if ((name=="ResultsPrinter")||(name=="HashGroupify")||(name=="Union")) {
      supported.back()=false;
   }
}
//---------------------------------------------------------------------------
void PredicateCollector::addArgumentAnnotation(const std::string& argument)
   // Add an operator argument annotation
{
   operatorArgument=argument;
   argSlot=0;
}
//---------------------------------------------------------------------------
void PredicateCollector::addScanAnnotation(const Register* reg,bool bound)
   // Add a scan annotation
{
   unsigned arg=0;
   if (operatorArgument=="SubjectPredicateObject") {
      if (argSlot==0) arg=0;
      if (argSlot==1) arg=1;
      if (argSlot==2) arg=2;
   } else if (operatorArgument=="SubjectObjectPredicate") {
      if (argSlot==0) arg=0;
      if (argSlot==1) arg=2;
      if (argSlot==2) arg=1;
   } else if (operatorArgument=="PredicateSubjectObject") {
      if (argSlot==0) arg=1;
      if (argSlot==1) arg=0;
      if (argSlot==2) arg=2;
   } else if (operatorArgument=="PredicateObjectSubject") {
      if (argSlot==0) arg=1;
      if (argSlot==1) arg=2;
      if (argSlot==2) arg=0;
   } else if (operatorArgument=="ObjectSubjectPredicate") {
      if (argSlot==0) arg=2;
      if (argSlot==1) arg=0;
      if (argSlot==2) arg=1;
   } else if (operatorArgument=="ObjectPredicateSubject") {
      if (argSlot==0) arg=2;
      if (argSlot==1) arg=1;
      if (argSlot==2) arg=0;
   } else if (operatorArgument=="SubjectPredicate") {
      if (argSlot==0) arg=0;
      if (argSlot==1) arg=1;
   } else if (operatorArgument=="SubjectObject") {
      if (argSlot==0) arg=0;
      if (argSlot==1) arg=2;
   } else if (operatorArgument=="PredicateSubject") {
      if (argSlot==0) arg=1;
      if (argSlot==1) arg=0;
   } else if (operatorArgument=="PredicateObject") {
      if (argSlot==0) arg=1;
      if (argSlot==1) arg=2;
   } else if (operatorArgument=="ObjectSubject") {
      if (argSlot==0) arg=2;
      if (argSlot==1) arg=0;
   } else if (operatorArgument=="ObjectPredicate") {
      if (argSlot==0) arg=2;
      if (argSlot==1) arg=1;
   } else if (operatorArgument=="Subject") {
      if (argSlot==0) arg=0;
   } else if (operatorArgument=="Predicate") {
      if (argSlot==0) arg=1;
   } else if (operatorArgument=="Object") {
      if (argSlot==0) arg=2;
   }

   if (bound)
      predicates.back().insert(pair<const Register*,unsigned>(reg,reg->value));
   bindings[reg]=(3*scanCount)+arg;
   ++argSlot;
}
//---------------------------------------------------------------------------
void PredicateCollector::addEqualPredicateAnnotation(const Register* reg1,const Register* reg2)
   // Add a predicate annotate
{
   equal.back().insert(pair<const Register*,const Register*>(reg1,reg2));
}
//---------------------------------------------------------------------------
void PredicateCollector::addMaterializationAnnotation(const std::vector<Register*>& /*regs*/)
   // Add a materialization annotation
{
}
//---------------------------------------------------------------------------
void PredicateCollector::addGenericAnnotation(const std::string& /*text*/)
   // Add a generic annotation
{
   supported.back()=false;
}
//---------------------------------------------------------------------------
void PredicateCollector::endOperator()
   // Close the current operator
{
   if (supported.back()) {
      map<unsigned,string> relationNames;
      out << "{";
      for (set<unsigned>::const_iterator start=relations.back().begin(),limit=relations.back().end(),iter=start;iter!=limit;++iter) {
         if (iter!=start) out << " ";
         stringstream s;
         s << "R" << (relationNames.size()+1);
         relationNames[*iter]=s.str();
         out << s.str();
         if (relations.size()>1) relations[relations.size()-2].insert(*iter);
      }
      out << "} {";
      bool first=true;
      for (set<pair<const Register*,const Register*> >::const_iterator iter=equal.back().begin(),limit=equal.back().end();iter!=limit;++iter) {
         if (first) first=false; else out << " ";
         unsigned l=bindings[(*iter).first],r=bindings[(*iter).second];
         out << relationNames[l/3] << "." << ("SPO"[l%3]) << "=" << relationNames[r/3] << "." << ("SPO"[r%3]);
         if (previousEqual.size()>1) previousEqual[previousEqual.size()-2].insert(*iter);
      }
      for (set<pair<const Register*,unsigned> >::const_iterator start=predicates.back().begin(),limit=predicates.back().end(),iter=start;iter!=limit;++iter) {
         if (first) first=false; else out << " ";
         unsigned l=bindings[(*iter).first],r=(*iter).second;
         out << relationNames[l/3] << "." << ("SPO"[l%3]) << "=" << r;
         if (previousPredicates.size()>1) previousPredicates[previousPredicates.size()-2].insert(*iter);
      }
      out << "} {";
      first=true;
      for (set<pair<const Register*,const Register*> >::const_iterator iter=previousEqual.back().begin(),limit=previousEqual.back().end();iter!=limit;++iter) {
         if (first) first=false; else out << " ";
         unsigned l=bindings[(*iter).first],r=bindings[(*iter).second];
         out << relationNames[l/3] << "." << ("SPO"[l%3]) << "=" << relationNames[r/3] << "." << ("SPO"[r%3]);
         if (previousEqual.size()>1) previousEqual[previousEqual.size()-2].insert(*iter);
      }
      for (set<pair<const Register*,unsigned> >::const_iterator start=previousPredicates.back().begin(),limit=previousPredicates.back().end(),iter=start;iter!=limit;++iter) {
         if (first) first=false; else out << " ";
         unsigned l=bindings[(*iter).first],r=(*iter).second;
         out << relationNames[l/3] << "." << ("SPO"[l%3]) << "=" << r;
         if (previousPredicates.size()>1) previousPredicates[previousPredicates.size()-2].insert(*iter);
      }
      out << "} " << cardinalities.back().first << " " << cardinalities.back().second << endl;
      supported.pop_back();
   } else {
      supported.pop_back();
      if (!supported.empty())
         supported.back()=false;
   }
   relations.pop_back();
   predicates.pop_back();
   previousPredicates.pop_back();
   equal.pop_back();
   previousEqual.pop_back();
   cardinalities.pop_back();
}
//---------------------------------------------------------------------------
std::string PredicateCollector::formatRegister(const Register* /*reg*/)
   // Format a register (for generic annotations)
{
   return "";
}
//---------------------------------------------------------------------------
std::string PredicateCollector::formatValue(unsigned /*value*/)
   // Format a constant value (for generic annotations)
{
   return "";
}
//---------------------------------------------------------------------------
}
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
static bool evalQuery(Database& db,SPARQLLexer& lexer,ostream& planOut,ostream& statsOut)
   // Evaluate a query
{
   QueryGraph queryGraph;
   std::string::const_iterator queryStart=lexer.getReader();
   {
      // Parse the query
      SPARQLParser parser(lexer);
      try {
         parser.parse(true);
      } catch (const SPARQLParser::ParserException& e) {
         cerr << "parse error: " << e.message << endl;
         return false;
      }

      // And perform the semantic anaylsis
      SemanticAnalysis semana(db);
      semana.transform(parser,queryGraph);
      if (queryGraph.knownEmpty()) {
         cerr << "known empty result ignored" << endl;
         return true;
      }
   }

   // Run the optimizer
   PlanGen plangen;
   Plan* plan=plangen.translate(db,queryGraph);
   if (!plan) {
      cerr << "plan generation failed" << endl;
      return true;
   }
   Operator::disableSkipping=true;

   // Build a physical plan
   Runtime runtime(db);
   Operator* operatorTree=CodeGen().translate(runtime,queryGraph,plan,true);

   // And execute it
   Scheduler scheduler;
   scheduler.execute(operatorTree);
   Timestamp stop;

   // Write the plan
   planOut << "# SPARQL Query:" << endl << "# ";
   std::string::const_iterator queryEnd=lexer.getReader();
   for (string::const_iterator iter=queryStart;iter!=queryEnd;++iter)
      if ((*iter)=='\n')
         planOut << endl << "# "; else
         planOut << *iter;
   planOut << endl << endl << "# Execution plan: <Operator expectedCardinality observedCardinalit [args] [input]>" << endl << endl;
   {
      DebugPlanPrinter out(planOut,runtime,true);
      operatorTree->print(out);
   }

   // Write the selectivities
   {
      statsOut << "# Stats: {relations} {new predicates(s)} {previous predicate(s)} expectedCardinality observedCardinality" << endl;
      PredicateCollector out(statsOut,runtime);
      operatorTree->print(out);
   }

   delete operatorTree;
   return true;
}
//---------------------------------------------------------------------------
static void evalQueries(Database& db,const string& query,ostream& planOut,ostream& statsOut)
   // Evaluate a query file
{
   unsigned count = 0;
   SPARQLLexer lexer(query);
   while (true) {
      if (lexer.hasNext(SPARQLLexer::Eof))
         break;
      if (!evalQuery(db,lexer,planOut,statsOut))
         break;
      ++count;
   }

   if (!count)
      cerr << "warning: no query processed" << endl;
}
//---------------------------------------------------------------------------
int main(int argc,char* argv[])
{
   // Check the arguments
   if (argc<3) {
      cout << "usage: " << argv[0] << " <database> [sparqlfile(s)]" << endl;
      return 1;
   }

   // Open the database
   Database db;
   if (!db.open(argv[1],true)) {
      cerr << "unable to open database " << argv[1] << endl;
      return 1;
   }

   if (argc==3) {
      // Retrieve the query
      string query;
      ifstream in(argv[2]);
      if (!in.is_open()) {
         cerr << "unable to open " << argv[2] << endl;
         return 1;
      }
      query=readInput(in);

      // And evaluate it
      evalQueries(db,query,cout,cerr);
   } else {
      for (int index=2;index<argc;index++) {
         // Retrieve the query
         string query;
         ifstream in(argv[index]);
         if (!in.is_open()) {
            cerr << "unable to open " << argv[index] << endl;
            return 1;
         }
         query=readInput(in);

         // And produce the files
         string planOutFile=string(argv[index])+".plan",predicatesOutFile=string(argv[index])+".predicates";
         ofstream planOut(planOutFile.c_str()),predicatesOut(predicatesOutFile.c_str());
         cerr << "processing " << argv[index] << " to " << planOutFile << " and " << predicatesOutFile << endl;
         evalQueries(db,query,planOut,predicatesOut);
      }
   }
}
//---------------------------------------------------------------------------
