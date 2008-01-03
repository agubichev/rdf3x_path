#ifndef H_cts_plangen_Plan
#define H_cts_plangen_Plan
//---------------------------------------------------------------------------
#include "infra/util/Pool.hpp"
//---------------------------------------------------------------------------
/// A plan fragment
struct Plan
{
   /// Possible operators
   enum Op { IndexScan, AggregatedIndexScan, NestedLoopJoin, MergeJoin, HashJoin, HashGroupify };
   /// The cardinalits type
   typedef double card_t;
   /// The cost type
   typedef double cost_t;

   /// The root operator
   Op op;
   /// Operator argument
   unsigned opArg;
   /// Its input
   Plan* left,*right;
   /// The resulting cardinality
   card_t cardinality;
   /// The total costs
   cost_t costs;
   /// The ordering
   unsigned ordering;

   /// The next plan in problem chaining
   Plan* next;
};
//---------------------------------------------------------------------------
/// A container for plans. Encapsulates the memory management
class PlanContainer
{
   private:
   /// The pool
   StructPool<Plan> pool;

   public:
   /// Constructor
   PlanContainer();
   /// Destructor
   ~PlanContainer();

   /// Alloca a new plan
   Plan* alloc() { return pool.alloc(); }
   /// Release an allocate plan
   void free(Plan* p) { pool.free(p); }
   /// Release all plans
   void clear();
};
//---------------------------------------------------------------------------
#endif
