#include "cts/codegen/CodeGen.hpp"
#include "cts/infra/QueryGraph.hpp"
#include "cts/plangen/PlanGen.hpp"
#include "cts/semana/SemanticAnalysis.hpp"
#include "rts/database/Database.hpp"
#include "rts/runtime/Runtime.hpp"
#include "rts/operator/Operator.hpp"
#include "rts/operator/ResultsPrinter.hpp"
#include "rts/operator/Scheduler.hpp"
#include <iostream>
#include <fstream>
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
      cout << predb << "\t" << predb << "\t" << trueCard << "\t" << estCard << "\t" << realRoot->getExpectedOutputCardinality() << "\t" << qError(estCard,trueCard) << "\t" << qError(realRoot->getExpectedOutputCardinality(),trueCard) << endl;
   }
}
//---------------------------------------------------------------------------
