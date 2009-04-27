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
   vector<set<string> > relations;
   /// The predicates
   vector<set<string> > predicates;
   /// Unresolved equal conditions
   vector<vector<pair<const Register*,const Register*> > > equal;
   /// The current operator characteristics
   string operatorName,operatorArgument;
   /// The argument slot
   unsigned argSlot;
   /// The number of scans
   unsigned scanCount;
   /// Register bindings
   map<const Register*,string> bindings;

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
   relations.push_back(set<string>());
   predicates.push_back(set<string>());
   equal.push_back(vector<pair<const Register*,const Register*> >());
   cardinalities.push_back(pair<unsigned,unsigned>(expectedOutputCardinality,observedOutputCardinality));
   operatorName=name;

   if ((name=="IndexScan")||(name=="AggregatedIndexScan")||(name=="FullyAggregatedIndexScan")) {
      stringstream s;
      s << "R" << (++scanCount);
      relations.back().insert(s.str());
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
   const char* arg="";
   if (operatorArgument=="SubjectPredicateObject") {
      if (argSlot==0) arg="S";
      if (argSlot==1) arg="P";
      if (argSlot==2) arg="O";
   } else if (operatorArgument=="SubjectObjectPredicate") {
      if (argSlot==0) arg="S";
      if (argSlot==1) arg="O";
      if (argSlot==2) arg="P";
   } else if (operatorArgument=="PredicateSubjectObject") {
      if (argSlot==0) arg="P";
      if (argSlot==1) arg="S";
      if (argSlot==2) arg="O";
   } else if (operatorArgument=="PredicateObjectSubject") {
      if (argSlot==0) arg="P";
      if (argSlot==1) arg="O";
      if (argSlot==2) arg="S";
   } else if (operatorArgument=="ObjectSubjectPredicate") {
      if (argSlot==0) arg="O";
      if (argSlot==1) arg="S";
      if (argSlot==2) arg="P";
   } else if (operatorArgument=="ObjectPredicateSubject") {
      if (argSlot==0) arg="O";
      if (argSlot==1) arg="P";
      if (argSlot==2) arg="S";
   } else if (operatorArgument=="SubjectPredicate") {
      if (argSlot==0) arg="S";
      if (argSlot==1) arg="P";
   } else if (operatorArgument=="SubjectObject") {
      if (argSlot==0) arg="S";
      if (argSlot==1) arg="O";
   } else if (operatorArgument=="PredicateSubject") {
      if (argSlot==0) arg="P";
      if (argSlot==1) arg="S";
   } else if (operatorArgument=="PredicateObject") {
      if (argSlot==0) arg="P";
      if (argSlot==1) arg="O";
   } else if (operatorArgument=="ObjectSubject") {
      if (argSlot==0) arg="O";
      if (argSlot==1) arg="S";
   } else if (operatorArgument=="ObjectPredicate") {
      if (argSlot==0) arg="O";
      if (argSlot==1) arg="P";
   } else if (operatorArgument=="Subject") {
      if (argSlot==0) arg="S";
   } else if (operatorArgument=="Predicate") {
      if (argSlot==0) arg="P";
   } else if (operatorArgument=="Object") {
      if (argSlot==0) arg="O";
   }

   if (bound) {
      stringstream s;
      s << "R" << scanCount << "." << arg << "=" << reg->value;
      predicates.back().insert(s.str());
   } else {
      stringstream s;
      s << "R" << scanCount << "." << arg;
      bindings[reg]=s.str();
   }
   ++argSlot;
}
//---------------------------------------------------------------------------
void PredicateCollector::addEqualPredicateAnnotation(const Register* reg1,const Register* reg2)
   // Add a predicate annotate
{
   equal.back().push_back(pair<const Register*,const Register*>(reg1,reg2));
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
      for (vector<pair<const Register*,const Register*> >::const_iterator iter=equal.back().begin(),limit=equal.back().end();iter!=limit;++iter)
         predicates.back().insert(bindings[(*iter).first]+"="+bindings[(*iter).second]);
      out << "{";
      for (set<string>::const_iterator start=relations.back().begin(),limit=relations.back().end(),iter=start;iter!=limit;++iter) {
         if (iter!=start)
            out << " ";
         out << (*iter);
         if (relations.size()>1) relations[relations.size()-2].insert(*iter);
      }
      out << "} {";
      for (set<string>::const_iterator start=predicates.back().begin(),limit=predicates.back().end(),iter=start;iter!=limit;++iter) {
         if (iter!=start)
            out << " ";
         out << (*iter);
         if (predicates.size()>1) predicates[predicates.size()-2].insert(*iter);
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
   equal.pop_back();
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
static void evalQuery(Database& db,const string& query,ostream& planOut,ostream& statsOut)
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
      SemanticAnalysis semana(db);
      semana.transform(parser,queryGraph);
      if (queryGraph.knownEmpty()) {
         cerr << "known empty result ignored" << endl;
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
   for (string::const_iterator iter=query.begin(),limit=query.end();iter!=limit;++iter)
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
      statsOut << "# Stats: {relations} {predicates} expectedCardinality observedCardinality" << endl;
      PredicateCollector out(statsOut,runtime);
      operatorTree->print(out);
   }

   delete operatorTree;
}
//---------------------------------------------------------------------------
int main(int argc,char* argv[])
{
   // Check the arguments
   if (argc<2) {
      cout << "usage: " << argv[0] << " <database> [sparqlfile(s)]" << endl;
      return 1;
   }

   // Open the database
   Database db;
   if (!db.open(argv[1],true)) {
      cerr << "unable to open database " << argv[1] << endl;
      return 1;
   }

   // Retrieve the query
   string query;
   if (argc>2) {
      ifstream in(argv[2]);
      if (!in.is_open()) {
         cerr << "unable to open " << argv[2] << endl;
         return 1;
      }
      query=readInput(in);
   } else {
      query=readInput(cin);
   }

   // And evaluate it
   evalQuery(db,query,cout,cerr);
}
//---------------------------------------------------------------------------
