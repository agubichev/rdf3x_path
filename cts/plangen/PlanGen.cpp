#include "cts/plangen/PlanGen.hpp"
#include "cts/plangen/Costs.hpp"
#include "rts/segment/AggregatedFactsSegment.hpp"
#include "rts/segment/FactsSegment.hpp"
//---------------------------------------------------------------------------
/// Description for a join
struct PlanGen::JoinDescription
{
   /// Sides of the join
   BitSet left,right;
   /// Required ordering
   unsigned ordering;
   /// Selectivity
   double selectivity;
};
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
void PlanGen::addPlan(Problem* problem,Plan* plan)
   // Add a plan to a subproblem
{
   // Check for dominance
   Plan* last=0;
   for (Plan* iter=problem->plans,*next;iter;iter=next) {
      next=iter->next;
      if (iter->ordering==plan->ordering) {
         // Dominated by existing plan?
         if (iter->costs<=plan->costs) {
            plans.free(plan);
            return;
         }
         // No, remove the existing plan
         if (last)
            last->next=iter->next; else
            problem->plans=iter->next;
         plans.free(iter);
      } else last=iter;
   }

   // Add the plan to the problem set
   plan->next=problem->plans;
   problem->plans=plan;
}
//---------------------------------------------------------------------------
void PlanGen::buildIndexScan(Database& db,Database::DataOrder order,Problem* result,unsigned value1,unsigned value2,unsigned value3)
   // Build an index scan
{
   // Initialize a new plan
   Plan* plan=plans.alloc();
   plan->op=Plan::IndexScan;
   plan->opArg=order;
   plan->left=0;
   plan->right=0;
   plan->next=0;

   // Compute the statistics
   unsigned div=0;
   if (!~value1) {
      if (!~value2) {
         if (!~value3) {
            plan->cardinality=1;
            plan->ordering=value3;
         } else {
            plan->cardinality=static_cast<double>(db.getFacts(order).getCardinality())/db.getFacts(order).getLevel2Groups();
            plan->ordering=value3;
         }
      } else {
         plan->cardinality=static_cast<double>(db.getFacts(order).getCardinality())/db.getFacts(order).getLevel1Groups();
         plan->ordering=value2;
         if (!~value3) div=10;
      }
   } else {
      plan->cardinality=db.getFacts(order).getCardinality();
      plan->ordering=value1;
      if (~!value2) {
         if (!~value3)
            div=20; else
            div=10;
      } else if (!~value3) div=10;
   }
   unsigned pages=1+static_cast<unsigned>(db.getFacts(order).getPages()*(plan->cardinality/static_cast<double>(db.getFacts(order).getCardinality())));
   plan->costs=Costs::seekBtree()+Costs::scan(pages);
   if (div)
      plan->cardinality=plan->cardinality/div;

   // And store it
   addPlan(result,plan);
}
//---------------------------------------------------------------------------
void PlanGen::buildAggregatedIndexScan(Database& db,Database::DataOrder order,Problem* result,unsigned value1,unsigned value2)
   // Build an aggregated index scan
{
   // Initialize a new plan
   Plan* plan=plans.alloc();
   plan->op=Plan::AggregatedIndexScan;
   plan->opArg=order;
   plan->left=0;
   plan->right=0;
   plan->next=0;

   // Compute the statistics
   unsigned div=0;
   if (!~value1) {
      if (!~value2) {
         plan->cardinality=1;
         plan->ordering=value2;
      } else {
         plan->cardinality=static_cast<double>(db.getAggregatedFacts(order).getLevel2Groups())/db.getAggregatedFacts(order).getLevel1Groups();
         plan->ordering=value2;
      }
   } else {
      plan->cardinality=db.getAggregatedFacts(order).getLevel2Groups();
      plan->ordering=value1;
      if (~!value2)
         div=10;
   }
   unsigned pages=1+static_cast<unsigned>(db.getAggregatedFacts(order).getPages()*(plan->cardinality/static_cast<double>(db.getAggregatedFacts(order).getLevel2Groups())));
   plan->costs=Costs::seekBtree()+Costs::scan(pages);
   if (div)
      plan->cardinality=plan->cardinality/div;

   // And store it
   addPlan(result,plan);
}
//---------------------------------------------------------------------------
static bool isUnused(const QueryGraph& query,const QueryGraph::Node& node,unsigned val)
   // Check if a variable is unused outside its primary pattern
{
   for (QueryGraph::projection_iterator iter=query.projectionBegin(),limit=query.projectionEnd();iter!=limit;++iter)
      if ((*iter)==val)
         return false;
   for (QueryGraph::node_iterator iter=query.nodesBegin(),limit=query.nodesEnd();iter!=limit;++iter) {
      const QueryGraph::Node& n=*iter;
      if ((&n)==(&node))
         continue;
      if ((!n.constSubject)&&(val==n.subject)) return false;
      if ((!n.constPredicate)&&(val==n.predicate)) return false;
      if ((!n.constObject)&&(val==n.object)) return false;
   }
   return true;
}
//---------------------------------------------------------------------------
PlanGen::Problem* PlanGen::buildScan(Database& db,const QueryGraph& query,const QueryGraph::Node& node,unsigned id)
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

   // Lookup variables
   unsigned s=node.constSubject?~0u:node.subject,p=node.constPredicate?~0u:node.predicate,o=node.constObject?~0u:node.object;

   // Build all relevant scans
   if (unusedObject)
      buildAggregatedIndexScan(db,Database::Order_Subject_Predicate_Object,result,s,p); else
      buildIndexScan(db,Database::Order_Subject_Predicate_Object,result,s,p,o);
   if (unusedPredicate)
      buildAggregatedIndexScan(db,Database::Order_Subject_Object_Predicate,result,s,o); else
      buildIndexScan(db,Database::Order_Subject_Object_Predicate,result,s,o,p);
   if (unusedSubject)
      buildAggregatedIndexScan(db,Database::Order_Object_Predicate_Subject,result,o,p); else
      buildIndexScan(db,Database::Order_Object_Predicate_Subject,result,o,p,s);
   if (unusedPredicate)
      buildAggregatedIndexScan(db,Database::Order_Object_Subject_Predicate,result,o,s); else
      buildIndexScan(db,Database::Order_Object_Subject_Predicate,result,o,s,p);
   if (unusedObject)
      buildAggregatedIndexScan(db,Database::Order_Predicate_Subject_Object,result,p,s); else
      buildIndexScan(db,Database::Order_Predicate_Subject_Object,result,p,s,o);
   if (unusedSubject)
      buildAggregatedIndexScan(db,Database::Order_Predicate_Object_Subject,result,p,o); else
      buildIndexScan(db,Database::Order_Predicate_Object_Subject,result,p,o,s);

   // Update the child pointers as info for the code generation
   for (Plan* iter=result->plans;iter;iter=iter->next)
      iter->right=reinterpret_cast<Plan*>(const_cast<QueryGraph::Node*>(&node));

   return result;
}
//---------------------------------------------------------------------------
PlanGen::JoinDescription PlanGen::buildJoinInfo(Database& db,const QueryGraph& query,const QueryGraph::Edge& edge)
   // Build the informaion about a join
{
   // Fill in the relations involved
   JoinDescription result;
   result.left.set(edge.from-(&(*query.nodesBegin())));
   result.right.set(edge.to-(&(*query.nodesBegin())));

   // Estimate the selectivity
   double sel1=1,sel2=1;
   for (unsigned index=0;index<2;index++) {
      const QueryGraph::Node* n=index?edge.from:edge.to;
      unsigned card;
      if (n->constSubject) {
         if (n->constPredicate) {
            if (n->constObject)
               card=1; else
               card=db.getFacts(Database::Order_Object_Subject_Predicate).getLevel1Groups();
         } else {
            if (n->constObject)
               card=db.getFacts(Database::Order_Predicate_Object_Subject).getLevel1Groups(); else
               card=db.getFacts(Database::Order_Predicate_Object_Subject).getLevel2Groups();
         }
      } else if (n->constPredicate) {
         if (n->constObject)
            card=db.getFacts(Database::Order_Subject_Predicate_Object).getLevel1Groups(); else
            card=db.getFacts(Database::Order_Subject_Object_Predicate).getLevel2Groups();
      } else if (n->constObject) {
         card=db.getFacts(Database::Order_Subject_Predicate_Object).getLevel2Groups();
      } else {
         card=db.getFacts(Database::Order_Subject_Predicate_Object).getCardinality();
      }
      if (index)
         sel2=static_cast<double>(card)/db.getFacts(Database::Order_Subject_Predicate_Object).getCardinality(); else
         sel1=static_cast<double>(card)/db.getFacts(Database::Order_Subject_Predicate_Object).getCardinality();
   }
   result.selectivity=(sel1>sel2)?sel1:sel2;

   // Look up suitable orderings
   if ((!edge.from->constSubject)&&(!edge.to->constSubject)&&(edge.from->subject==edge.to->subject)) {
      result.ordering=edge.from->subject;
   } else if ((!edge.from->constSubject)&&(!edge.to->constPredicate)&&(edge.from->subject==edge.to->predicate)) {
      result.ordering=edge.from->subject;
   } else if ((!edge.from->constSubject)&&(!edge.to->constObject)&&(edge.from->subject==edge.to->object)) {
      result.ordering=edge.from->subject;
   } else if ((!edge.from->constPredicate)&&(!edge.to->constSubject)&&(edge.from->predicate==edge.to->subject)) {
      result.ordering=edge.from->predicate;
   } else if ((!edge.from->constPredicate)&&(!edge.to->constPredicate)&&(edge.from->predicate==edge.to->predicate)) {
      result.ordering=edge.from->predicate;
   } else if ((!edge.from->constPredicate)&&(!edge.to->constObject)&&(edge.from->predicate==edge.to->object)) {
      result.ordering=edge.from->predicate;
   } else if ((!edge.from->constObject)&&(!edge.to->constSubject)&&(edge.from->object==edge.to->subject)) {
      result.ordering=edge.from->object;
   } else if ((!edge.from->constObject)&&(!edge.to->constPredicate)&&(edge.from->object==edge.to->predicate)) {
      result.ordering=edge.from->object;
   } else if ((!edge.from->constObject)&&(!edge.to->constObject)&&(edge.from->object==edge.to->object)) {
      result.ordering=edge.from->object;
   } else {
      // Cross product
      result.ordering=(~0u)-1;
      result.selectivity=1;
   }

   return result;
}
//---------------------------------------------------------------------------
Plan* PlanGen::translate(Database& db,const QueryGraph& query)
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
      Problem* p=buildScan(db,query,*iter,id);
      if (last)
         last->next=p; else
         dpTable[0]=p;
      last=p;
   }

   // Construct the join info
   std::vector<JoinDescription> joins;
   for (QueryGraph::edge_iterator iter=query.edgesBegin(),limit=query.edgesEnd();iter!=limit;++iter)
      joins.push_back(buildJoinInfo(db,query,*iter));

   // Retrieve the best plan
   if (!dpTable.back())
      return 0;
   Plan* best=0;
   for (Plan* iter=dpTable.back()->plans;iter;iter=iter->next)
      if ((!best)||(iter->costs<best->costs))
         best=iter;
   if (!best)
      return 0;

   return best;
}
//---------------------------------------------------------------------------
