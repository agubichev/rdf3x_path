#ifndef H_rts_operator_DijkstraScan
#define H_rts_operator_DijkstraScan
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
#include "rts/operator/Operator.hpp"
#include "rts/database/Database.hpp"
#include "rts/segment/FactsSegment.hpp"
#include "rts/runtime/Runtime.hpp"
#include "cts/infra/QueryGraph.hpp"
#include "rts/segment/DictionarySegment.hpp"
#include <vector>
//---------------------------------------------------------------------------
class Register;
//---------------------------------------------------------------------------
/// An index scan over the facts table
class DijkstraScan : public Operator
{
   private:
   /// The registers for the different parts of the triple
   Register* value1,*value3;
   VectorRegister* value2;
   /// The different boundings
   bool bound1,bound2,bound3;
   /// The facts segment
   FactsSegment& facts;
   /// The data order
   Database::DataOrder order;
   /// The scan
   FactsSegment::Scan scan;
   /// Filter for paths
   QueryGraph::Filter* pathfilter;
   /// DB dictionary
   DictionarySegment dict;

   /// Constructor
   DijkstraScan(Database& db,Database::DataOrder order,Register* value1,bool bound1,VectorRegister* value2,bool bound2,Register* value3,bool bound3,double expectedOutputCardinality,QueryGraph::Filter* pathfilter);

   // Implementations
   class DijkstraPrefix;
   class DijkstraPoint2Point;

   public:
   /// Destructor
   ~DijkstraScan();

   /// Produce the first tuple
   unsigned first();
   /// Produce the next tuple
   unsigned next();

   /// Print the operator tree. Debugging only.
   void print(PlanPrinter& out);

   /// Add a merge join hint
   void addMergeHint(Register* /*reg1*/,Register* /*reg2*/){};
   /// Register parts of the tree that can be executed asynchronous
   void getAsyncInputCandidates(Scheduler& /*scheduler*/){};

   /// Create a suitable operator
   static DijkstraScan* create(Database& db,Database::DataOrder order,Register* subjectRegister,bool subjectBound,VectorRegister* predicateRegister,bool predicateBound,Register* objectRegister,bool objectBound,double expectedOutputCardinality,QueryGraph::Filter* pathfilter);
};
//---------------------------------------------------------------------------
#endif
