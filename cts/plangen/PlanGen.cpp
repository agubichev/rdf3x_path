#include "cts/plangen/PlanGen.hpp"
//---------------------------------------------------------------------------
PlanGen::PlanGen()
   // Constructor
{
}
//---------------------------------------------------------------------------
PlanGen::~PlanGen()
   // Destructor
{
}
//---------------------------------------------------------------------------
static bool isUnused(const QueryGraph& query,const QueryGraph::Node& node,unsigned val)
   // Check if a variable is unused outside its primary pattern
{
   for (QueryGraph::projection_iterator iter=query.projectionBegin(),limit=query.projectionEnd();iter!=limit;++iter)
      if ((*iter)==val)
         return false;
   for (QueryGraph::node_iterator iter=query.nodesBegin(),limit=query.nodesEnd();iter!=limit;++iter) {
      const Node& n=*iter;
      if ((&n)==(&node))
         continue;
      if ((!n.constSubject)&&(val==n.subject)) return false;
      if ((!n.constPredicate)&&(val==n.predicate)) return false;
      if ((!n.constObject)&&(val==n.object)) return false;
   }
   return true;
}
//---------------------------------------------------------------------------
Problem* PlanGen::buildScan(Database& db,const QueryGraph& query,const QueryGraph::Node& node,unsigned id)
   // Generate base table accesses
{
   // Create new problem instance
   Problem* result=problems.alloc();
   result->next=0;
   result->plans=0;
   result->relations=BitSet();
   result->relations.set(id);

   // Check which parts of the pattern are unused
   bool unusedSubject=node.constSubject||isUnused(query,node,node.subject);
   bool unusedPredicate=node.constPredicate||isUnused(query,node,node.predicate);
   bool unusedObject=node.constObject||isUnused(query,node,node.object);

   // Build all relevant scans
   if (unusedObject)
      buildAggregatedIndexScan(db,Database::Order_Subject_Predicate_Object); else
      buildIndexScan(db,Database::Order_Subject_Predicate_Object);
   if (unusedPredicate)
      buildAggregatedIndexScan(db,Databe::Order_Subject_Object_Predicate); else
      buildIndexScan(db,Database::Order_Subject_Object_Predicate);

         Order_Subject_Predicate_Object=0,Order_Subject_Object_Predicate,Order_Object_Predicate_Subject,
      Order_Object_Subject_Predicate,Order_Predicate_Subject_Object,Order_Predicate_Object_Subject

   }
}
//---------------------------------------------------------------------------
Operator* PlanGen::translate(const QueryGraph& query)
   // Translate a query into an operator tree
{
   // Check if we could handle the query
   if ((query.getNodeCount()==0)||(query.getNodeCount()>BitSet::maxWidth))
      return 0;

   // Reset the plan generator
   plans.clear();
   problems.freeAll();
   dpTable.clear();

   // Seed the DP table with scans
   dpTable.resize(query.getNodeCount());
   Problem* last=0;
   unsigned id=0;
   for (QueryGraph::node_iterator iter=query.nodesBegin(),limit=query.nodesEnd();iter!=limit;++iter) {
      Problem* p=buildScan(query,*iter,id);
      if (last)
         last->next=p; else
         dpTable[0]=p;
      last=p;
   }

   // Retrieve the best plan
   if (!dpTable.back())
      return 0;
   Plan* best=0;
   for (Plan* iter=dpTable.back()->plans;iter;iter=iter->next)
      if ((!best)||(iter->costs<best->costs))
         best=iter;
   if (!best)
      return 0;



   return 0;
}
//---------------------------------------------------------------------------
