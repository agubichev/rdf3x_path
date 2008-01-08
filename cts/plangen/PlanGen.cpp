#include "cts/plangen/PlanGen.hpp"
#include "cts/plangen/Costs.hpp"
#include "rts/segment/AggregatedFactsSegment.hpp"
#include "rts/segment/FactsSegment.hpp"
#include <map>
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
static Plan* buildFilters(PlanContainer& plans,const QueryGraph& query,Plan* plan,unsigned value1,unsigned value2,unsigned value3)
   // Apply filters to index scans
{
   // Apply a filter on the ordering first
   for (QueryGraph::filter_iterator iter=query.filtersBegin(),limit=query.filtersEnd();iter!=limit;++iter)
      if ((*iter).id==plan->ordering) {
         Plan* p2=plans.alloc();
         double cost1=plan->costs+Costs::filter(plan->cardinality);
         double cost2=0.5*plan->costs+(*iter).values.size()*Costs::seekBtree();
         if (cost2<cost1) {
            p2->op=Plan::NestedLoopFilter;
            p2->costs=cost2;
         } else {
            p2->op=Plan::Filter;
            p2->costs=cost1;
         }
         p2->opArg=(*iter).id;
         p2->left=plan;
         p2->right=reinterpret_cast<Plan*>(const_cast<QueryGraph::Filter*>(&(*iter)));
         p2->next=0;
         p2->cardinality=plan->cardinality*0.5;
         p2->ordering=plan->ordering;
         plan=p2;
      }
   // Apply all other applicable filters
   for (QueryGraph::filter_iterator iter=query.filtersBegin(),limit=query.filtersEnd();iter!=limit;++iter)
      if ((((*iter).id==value1)||((*iter).id==value2)||((*iter).id==value3))&&((*iter).id!=plan->ordering)) {
         Plan* p2=plans.alloc();
         p2->op=Plan::Filter;
         p2->opArg=(*iter).id;
         p2->left=plan;
         p2->right=reinterpret_cast<Plan*>(const_cast<QueryGraph::Filter*>(&(*iter)));
         p2->next=0;
         p2->cardinality=plan->cardinality*0.5;
         p2->costs=plan->costs+Costs::filter(plan->cardinality);
         p2->ordering=plan->ordering;
         plan=p2;
      }
   return plan;
}
//---------------------------------------------------------------------------
void PlanGen::buildIndexScan(Database& db,const QueryGraph& query,Database::DataOrder order,Problem* result,unsigned value1,unsigned value2,unsigned value3)
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

   // Apply filters
   plan=buildFilters(plans,query,plan,value1,value2,value3);

   // And store it
   addPlan(result,plan);
}
//---------------------------------------------------------------------------
void PlanGen::buildAggregatedIndexScan(Database& db,const QueryGraph& query,Database::DataOrder order,Problem* result,unsigned value1,unsigned value2)
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

   // Apply filters
   plan=buildFilters(plans,query,plan,value1,value2,~0u);

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
   for (QueryGraph::filter_iterator iter=query.filtersBegin(),limit=query.filtersEnd();iter!=limit;++iter)
      if ((*iter).id==val)
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
      buildAggregatedIndexScan(db,query,Database::Order_Subject_Predicate_Object,result,s,p); else
      buildIndexScan(db,query,Database::Order_Subject_Predicate_Object,result,s,p,o);
   if (unusedPredicate)
      buildAggregatedIndexScan(db,query,Database::Order_Subject_Object_Predicate,result,s,o); else
      buildIndexScan(db,query,Database::Order_Subject_Object_Predicate,result,s,o,p);
   if (unusedSubject)
      buildAggregatedIndexScan(db,query,Database::Order_Object_Predicate_Subject,result,o,p); else
      buildIndexScan(db,query,Database::Order_Object_Predicate_Subject,result,o,p,s);
   if (unusedPredicate)
      buildAggregatedIndexScan(db,query,Database::Order_Object_Subject_Predicate,result,o,s); else
      buildIndexScan(db,query,Database::Order_Object_Subject_Predicate,result,o,s,p);
   if (unusedObject)
      buildAggregatedIndexScan(db,query,Database::Order_Predicate_Subject_Object,result,p,s); else
      buildIndexScan(db,query,Database::Order_Predicate_Subject_Object,result,p,s,o);
   if (unusedSubject)
      buildAggregatedIndexScan(db,query,Database::Order_Predicate_Object_Subject,result,p,o); else
      buildIndexScan(db,query,Database::Order_Predicate_Object_Subject,result,p,o,s);

   // Update the child pointers as info for the code generation
   for (Plan* iter=result->plans;iter;iter=iter->next) {
      Plan* iter2=iter;
      while ((iter2->op==Plan::Filter)||(iter2->op==Plan::NestedLoopFilter))
         iter2=iter2->left;
      iter2->left=static_cast<Plan*>(0)+id;
      iter2->right=reinterpret_cast<Plan*>(const_cast<QueryGraph::Node*>(&node));
   }

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
   if (result.selectivity==1) // Distinguish real joins from cross products
      result.selectivity=0.999;
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
   for (QueryGraph::node_iterator iter=query.nodesBegin(),limit=query.nodesEnd();iter!=limit;++iter,++id) {
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

   // Build larger join trees
   std::vector<unsigned> joinOrderings;
   for (unsigned index=1;index<dpTable.size();index++) {
      std::map<BitSet,Problem*> lookup;
      for (unsigned index2=0;index2<index;index2++) {
         for (Problem* iter=dpTable[index2];iter;iter=iter->next) {
            BitSet leftRel=iter->relations;
            for (Problem* iter2=dpTable[index-index2-1];iter2;iter2=iter2->next) {
               // Overlapping subproblem?
               BitSet rightRel=iter2->relations;
               if (leftRel.overlapsWith(rightRel))
                  continue;

               // Investigate all join candidates
               Problem* problem=0;
               double selectivity=1;
               for (std::vector<JoinDescription>::const_iterator iter3=joins.begin(),limit3=joins.end();iter3!=limit3;++iter3)
                  if (((*iter3).left.subsetOf(leftRel))&&((*iter3).right.subsetOf(rightRel))) {
                     // We can join it...
                     BitSet relations=leftRel.unionWith(rightRel);
                     if (lookup.count(relations)) {
                        problem=lookup[relations];
                     } else {
                        lookup[relations]=problem=problems.alloc();
                        problem->relations=relations;
                        problem->plans=0;
                        problem->next=dpTable[index];
                        dpTable[index]=problem;
                     }
                     // Collect selectivities and join order candidates
                     joinOrderings.clear();
                     joinOrderings.push_back((*iter3).ordering);
                     selectivity=(*iter3).selectivity;
                     for (++iter3;iter3!=limit3;++iter3) {
                        joinOrderings.push_back((*iter3).ordering);
                        selectivity*=(*iter3).selectivity;
                     }
                     break;
                  }
               if (!problem) continue;

               // Combine phyiscal plans
               for (Plan* leftPlan=iter->plans;leftPlan;leftPlan=leftPlan->next) {
                  for (Plan* rightPlan=iter2->plans;rightPlan;rightPlan=rightPlan->next) {
                     // Try a merge joins
                     if (leftPlan->ordering==rightPlan->ordering) {
                        for (std::vector<unsigned>::const_iterator iter=joinOrderings.begin(),limit=joinOrderings.end();iter!=limit;++iter) {
                           if (leftPlan->ordering==(*iter)) {
                              Plan* p=plans.alloc();
                              p->op=Plan::MergeJoin;
                              p->opArg=*iter;
                              p->left=leftPlan;
                              p->right=rightPlan;
                              p->cardinality=leftPlan->cardinality*rightPlan->cardinality*selectivity;
                              p->costs=leftPlan->costs+rightPlan->costs+Costs::mergeJoin(leftPlan->cardinality,rightPlan->cardinality);
                              p->ordering=leftPlan->ordering;
                              addPlan(problem,p);
                              break;
                           }
                        }
                     }
                     // Try a hash join
                     if (selectivity<1) {
                        Plan* p=plans.alloc();
                        p->op=Plan::HashJoin;
                        p->opArg=0;
                        p->left=leftPlan;
                        p->right=rightPlan;
                        p->cardinality=leftPlan->cardinality*rightPlan->cardinality*selectivity;
                        p->costs=leftPlan->costs+rightPlan->costs+Costs::hashJoin(leftPlan->cardinality,rightPlan->cardinality);
                        p->ordering=~0u;
                        addPlan(problem,p);
                     } else {
                        // Nested loop join
                        Plan* p=plans.alloc();
                        p->op=Plan::NestedLoopJoin;
                        p->opArg=0;
                        p->left=leftPlan;
                        p->right=rightPlan;
                        p->cardinality=leftPlan->cardinality*rightPlan->cardinality*selectivity;
                        p->costs=leftPlan->costs+rightPlan->costs+leftPlan->cardinality*rightPlan->costs;
                        p->ordering=leftPlan->ordering;
                        addPlan(problem,p);
                     }
                  }
               }
            }
         }
      }
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

   // Aggregate, if required
   if ((query.getDuplicateHandling()==QueryGraph::CountDuplicates)||(query.getDuplicateHandling()==QueryGraph::NoDuplicates)) {
      Plan* p=plans.alloc();
      p->op=Plan::HashGroupify;
      p->opArg=0;
      p->left=best;
      p->right=0;
      p->cardinality=best->cardinality; // not correct, be we do not use this value anyway
      p->costs=best->costs; // the same here
      p->ordering=~0u;
      best=p;
   }

   return best;
}
//---------------------------------------------------------------------------
