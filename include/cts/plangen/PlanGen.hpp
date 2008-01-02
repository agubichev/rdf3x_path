#ifndef H_cts_plangen_PlanGen
#define H_cts_plangen_PlanGen
//---------------------------------------------------------------------------
#include "cts/plangen/Plan.hpp"
#include "cts/infra/BitSet.hpp"
#include "cts/infra/QueryGraph.hpp"
//---------------------------------------------------------------------------
class Operator;
//---------------------------------------------------------------------------
/// A plan generator that construct a physical plan from a query graph
class PlanGen
{
   private:
   /// A subproblem
   struct Problem {
      /// The next problem in the DP table
      Problem* next;
      /// The known solutions to the problem
      Plan* plans;
      /// The relations involved in the problem
      BitSet relations;
   };
   /// The plans
   PlanContainer plans;
   /// The problems
   StructPool<Problem> problems;
   /// The DP table
   std::vector<Problem*> dpTable;

   PlanGen(const PlanGen&);
   void operator=(const PlanGen&);

   /// Generate base table accesses
   Problem* buildScan(const QueryGraph& query,const QueryGraph::Node& node,unsigned id);

   public:
   /// Constructor
   PlanGen();
   /// Destructor
   ~PlanGen();

   /// Translate a query into an operator tree
   Operator* translate(const QueryGraph& query);
};
//---------------------------------------------------------------------------
#endif
