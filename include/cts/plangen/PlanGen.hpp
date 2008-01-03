#ifndef H_cts_plangen_PlanGen
#define H_cts_plangen_PlanGen
//---------------------------------------------------------------------------
#include "cts/plangen/Plan.hpp"
#include "cts/infra/BitSet.hpp"
#include "cts/infra/QueryGraph.hpp"
#include "rts/database/Database.hpp"
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
   /// A join description
   struct JoinDescription;
   /// The plans
   PlanContainer plans;
   /// The problems
   StructPool<Problem> problems;
   /// The DP table
   std::vector<Problem*> dpTable;

   PlanGen(const PlanGen&);
   void operator=(const PlanGen&);

   /// Add a plan to a subproblem
   void addPlan(Problem* problem,Plan* plan);
   /// Generate an index scan
   void buildIndexScan(Database& db,Database::DataOrder order,Problem* problem,unsigned value1,unsigned value2,unsigned value3);
   /// Generate an aggregated index scan
   void buildAggregatedIndexScan(Database& db,Database::DataOrder order,Problem* problem,unsigned value1,unsigned value2);
   /// Generate base table accesses
   Problem* buildScan(Database& db,const QueryGraph& query,const QueryGraph::Node& node,unsigned id);
   /// Build the informaion about a join
   JoinDescription buildJoinInfo(Database& db,const QueryGraph& query,const QueryGraph::Edge& edge);

   public:
   /// Constructor
   PlanGen();
   /// Destructor
   ~PlanGen();

   /// Translate a query into an operator tree
   Plan* translate(Database& db,const QueryGraph& query);
};
//---------------------------------------------------------------------------
#endif
