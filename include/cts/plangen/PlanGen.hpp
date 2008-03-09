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
   /// The database
   Database* db;
   /// The current query
   const QueryGraph* fullQuery;

   PlanGen(const PlanGen&);
   void operator=(const PlanGen&);

   /// Add a plan to a subproblem
   void addPlan(Problem* problem,Plan* plan);
   /// Generate an index scan
   void buildIndexScan(const QueryGraph::SubQuery& query,Database::DataOrder order,Problem* problem,unsigned value1,unsigned value1C,unsigned value2,unsigned value2C,unsigned value3,unsigned value3C);
   /// Generate an aggregated index scan
   void buildAggregatedIndexScan(const QueryGraph::SubQuery& query,Database::DataOrder order,Problem* problem,unsigned value1,unsigned value1C,unsigned value2,unsigned value2C);
   /// Generate base table accesses
   Problem* buildScan(const QueryGraph::SubQuery& query,const QueryGraph::Node& node,unsigned id);
   /// Build the informaion about a join
   JoinDescription buildJoinInfo(const QueryGraph::SubQuery& query,const QueryGraph::Edge& edge);
   /// Generate an optional part
   Problem* buildOptional(const QueryGraph::SubQuery& query,unsigned id);
   /// Generate a union part
   Problem* buildUnion(const std::vector<QueryGraph::SubQuery>& query,unsigned id);

   /// Greedily add complex filter expressions
   Plan* addComplexFilters(Plan* plan,const QueryGraph::SubQuery& query);
   /// Translate a query into an operator tree
   Plan* translate(const QueryGraph::SubQuery& query);

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
