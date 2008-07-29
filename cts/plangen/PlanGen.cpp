#include "cts/plangen/PlanGen.hpp"
#include "cts/plangen/Costs.hpp"
#include "cts/codegen/CodeGen.hpp"
#include "rts/segment/AggregatedFactsSegment.hpp"
#include "rts/segment/FullyAggregatedFactsSegment.hpp"
#include "rts/segment/FactsSegment.hpp"
#include "rts/segment/StatisticsSegment.hpp"
#include <map>
#include <set>
#include <algorithm>
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
// XXX integrate DPhyper-based optimization, query path statistics
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
static Plan* buildFilters(PlanContainer& plans,const QueryGraph::SubQuery& query,Plan* plan,unsigned value1,unsigned value2,unsigned value3)
   // Apply filters to index scans
{
   // Apply a filter on the ordering first
   for (vector<QueryGraph::Filter>::const_iterator iter=query.filters.begin(),limit=query.filters.end();iter!=limit;++iter)
      if (((*iter).id==plan->ordering)&&(!(*iter).exclude)) {
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
   for (vector<QueryGraph::Filter>::const_iterator iter=query.filters.begin(),limit=query.filters.end();iter!=limit;++iter)
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
static void normalizePattern(Database::DataOrder order,unsigned& c1,unsigned& c2,unsigned& c3)
    // Extract subject/predicate/object order
{
   unsigned s=~0u,p=~0u,o=~0u;
   switch (order) {
      case Database::Order_Subject_Predicate_Object: s=c1; p=c2; o=c3; break;
      case Database::Order_Subject_Object_Predicate: s=c1; o=c2; p=c3; break;
      case Database::Order_Object_Predicate_Subject: o=c1; p=c2; s=c3; break;
      case Database::Order_Object_Subject_Predicate: o=c1; s=c2; p=c3; break;
      case Database::Order_Predicate_Subject_Object: p=c1; s=c2; o=c3; break;
      case Database::Order_Predicate_Object_Subject: p=c1; o=c2; s=c3; break;
   }
   c1=s; c2=p; c3=o;
}
//---------------------------------------------------------------------------
static void denormalizePattern(Database::DataOrder order,unsigned& s,unsigned& p,unsigned& o)
    // Extract data order
{
   unsigned c1=~0u,c2=~0u,c3=~0u;
   switch (order) {
      case Database::Order_Subject_Predicate_Object: c1=s; c2=p; c3=o; break;
      case Database::Order_Subject_Object_Predicate: c1=s; c2=o; c3=p; break;
      case Database::Order_Object_Predicate_Subject: c1=o; c2=p; c3=s; break;
      case Database::Order_Object_Subject_Predicate: c1=o; c2=s; c3=p; break;
      case Database::Order_Predicate_Subject_Object: c1=p; c2=s; c3=o; break;
      case Database::Order_Predicate_Object_Subject: c1=p; c2=o; c3=s; break;
   }
   s=c1; p=c2; o=c3;
}
//---------------------------------------------------------------------------
static void maximizePrefix(Database::DataOrder& order,unsigned& c1,unsigned& c2,unsigned& c3)
   // Reshuffle values to maximize the constant prefix
{
   // Reconstruct the original assignments first
   unsigned s=c1,p=c2,o=c3;
   normalizePattern(order,s,p,o);

   // Now find the maximum prefix
   if (~s) {
      if ((~p)||(!~o)) {
         order=Database::Order_Subject_Predicate_Object;
         c1=s; c2=p; c3=o;
      } else {
         order=Database::Order_Subject_Object_Predicate;
         c1=s; c2=o; c3=p;
      }
   } else if (~p) {
      order=Database::Order_Predicate_Object_Subject;
      c1=p; c2=o; c3=s;
   } else if (~o) {
      order=Database::Order_Object_Predicate_Subject;
      c1=o; c2=p; c3=s;
   } else {
      order=Database::Order_Subject_Predicate_Object;
      c1=s; c2=p; c3=o;
   }
}
//---------------------------------------------------------------------------
static unsigned getCardinality(Database& db,Database::DataOrder order,unsigned c1,unsigned c2,unsigned c3)
   // Estimate the cardinality of a predicate
{
   maximizePrefix(order,c1,c2,c3);

   // Lookup the cardinality
   if (~c3) {
      return 1;
   } else if (~c2) {
      AggregatedFactsSegment::Scan scan;
      if (scan.first(db.getAggregatedFacts(order),c1,c2))
         return scan.getCount();
      return 1;
   } else if (~c1) {
      FullyAggregatedFactsSegment::Scan scan;
      if (scan.first(db.getFullyAggregatedFacts(order),c1))
         return scan.getCount();
      return 1;
   } else {
      return db.getFacts(order).getCardinality();
   }
}
//---------------------------------------------------------------------------
void PlanGen::buildIndexScan(const QueryGraph::SubQuery& query,Database::DataOrder order,Problem* result,unsigned value1,unsigned value1C,unsigned value2,unsigned value2C,unsigned value3,unsigned value3C)
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
   unsigned scanned;
   plan->cardinality=scanned=getCardinality(*db,order,value1C,value2C,value3C);
   if (!~value1) {
      if (!~value2) {
         plan->ordering=value3;
      } else {
         scanned=getCardinality(*db,order,value1C,value2C,~0u);
         plan->ordering=value2;
      }
   } else {
      scanned=getCardinality(*db,order,value1C,~0u,~0u);
      plan->ordering=value1;
   }
   unsigned pages=1+static_cast<unsigned>(db->getFacts(order).getPages()*(static_cast<double>(scanned)/static_cast<double>(db->getFacts(order).getCardinality())));
   plan->costs=Costs::seekBtree()+Costs::scan(pages);

   // Apply filters
   plan=buildFilters(plans,query,plan,value1,value2,value3);

   // And store it
   addPlan(result,plan);
}
//---------------------------------------------------------------------------
static unsigned getAggregatedCardinality(Database& db,Database::DataOrder order,unsigned c1,unsigned c2)
   // Estimate the cardinality of a predicate
{
   unsigned c3=~0u;
   maximizePrefix(order,c1,c2,c3);

   // Query the statistics
   StatisticsSegment::Bucket result;
   if ((~c3)||(~c2)) {
      return 1;
   } else if (~c1) {
      db.getStatistics(order).lookup(c1,result);
      if (result.prefix1Card)
         return (result.card+result.prefix1Card-1)/result.prefix1Card;
      return 1;
   } else {
      return db.getAggregatedFacts(order).getLevel2Groups();
   }
}
//---------------------------------------------------------------------------
void PlanGen::buildAggregatedIndexScan(const QueryGraph::SubQuery& query,Database::DataOrder order,Problem* result,unsigned value1,unsigned value1C,unsigned value2,unsigned value2C)
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
   unsigned scanned=getAggregatedCardinality(*db,order,value1C,value2C);
   unsigned fullSize=getCardinality(*db,order,value1C,value2C,~0u);
   if (scanned>fullSize)
      scanned=fullSize-1;
   if (!scanned) scanned=1;
   plan->cardinality=scanned;
   if (!~value1) {
      plan->ordering=value2;
   } else {
      scanned=getAggregatedCardinality(*db,order,value1C,~0u);
      fullSize=getCardinality(*db,order,value1C,~0u,~0u);
      if (scanned>fullSize)
         scanned=fullSize-1;
      if (!scanned) scanned=1;
      plan->ordering=value1;
   }
   unsigned pages=1+static_cast<unsigned>(db->getAggregatedFacts(order).getPages()*(static_cast<double>(scanned)/static_cast<double>(db->getAggregatedFacts(order).getLevel2Groups())));
   plan->costs=Costs::seekBtree()+Costs::scan(pages);

   // Apply filters
   plan=buildFilters(plans,query,plan,value1,value2,~0u);

   // And store it
   addPlan(result,plan);
}
//---------------------------------------------------------------------------
static bool isUnused(const QueryGraph::SubQuery& query,const QueryGraph::Node& node,unsigned val)
   // Check if a variable is unused outside its primary pattern
{
   for (vector<QueryGraph::Filter>::const_iterator iter=query.filters.begin(),limit=query.filters.end();iter!=limit;++iter)
      if ((*iter).id==val)
         return false;
   for (vector<QueryGraph::Node>::const_iterator iter=query.nodes.begin(),limit=query.nodes.end();iter!=limit;++iter) {
      const QueryGraph::Node& n=*iter;
      if ((&n)==(&node))
         continue;
      if ((!n.constSubject)&&(val==n.subject)) return false;
      if ((!n.constPredicate)&&(val==n.predicate)) return false;
      if ((!n.constObject)&&(val==n.object)) return false;
   }
   for (vector<QueryGraph::SubQuery>::const_iterator iter=query.optional.begin(),limit=query.optional.end();iter!=limit;++iter)
      if (!isUnused(*iter,node,val))
         return false;
   for (vector<vector<QueryGraph::SubQuery> >::const_iterator iter=query.unions.begin(),limit=query.unions.end();iter!=limit;++iter)
      for (vector<QueryGraph::SubQuery>::const_iterator iter2=(*iter).begin(),limit2=(*iter).end();iter2!=limit2;++iter2)
         if (!isUnused(*iter2,node,val))
            return false;
   return true;
}
//---------------------------------------------------------------------------
static bool isUnused(const QueryGraph& query,const QueryGraph::Node& node,unsigned val)
   // Check if a variable is unused outside its primary pattern
{
   for (QueryGraph::projection_iterator iter=query.projectionBegin(),limit=query.projectionEnd();iter!=limit;++iter)
      if ((*iter)==val)
         return false;

   return isUnused(query.getQuery(),node,val);
}
//---------------------------------------------------------------------------
PlanGen::Problem* PlanGen::buildScan(const QueryGraph::SubQuery& query,const QueryGraph::Node& node,unsigned id)
   // Generate base table accesses
{
   // Create new problem instance
   Problem* result=problems.alloc();
   result->next=0;
   result->plans=0;
   result->relations=BitSet();
   result->relations.set(id);

   // Check which parts of the pattern are unused
   bool unusedSubject=(!node.constSubject)&&isUnused(*fullQuery,node,node.subject);
   bool unusedPredicate=(!node.constPredicate)&&isUnused(*fullQuery,node,node.predicate);
   bool unusedObject=(!node.constObject)&&isUnused(*fullQuery,node,node.object);

   // Lookup variables
   unsigned s=node.constSubject?~0u:node.subject,p=node.constPredicate?~0u:node.predicate,o=node.constObject?~0u:node.object;
   unsigned sc=node.constSubject?node.subject:~0u,pc=node.constPredicate?node.predicate:~0u,oc=node.constObject?node.object:~0u;

   // Build all relevant scans
   if (unusedObject)
      buildAggregatedIndexScan(query,Database::Order_Subject_Predicate_Object,result,s,sc,p,pc); else
      buildIndexScan(query,Database::Order_Subject_Predicate_Object,result,s,sc,p,pc,o,oc);
   if (unusedPredicate)
      buildAggregatedIndexScan(query,Database::Order_Subject_Object_Predicate,result,s,sc,o,oc); else
      buildIndexScan(query,Database::Order_Subject_Object_Predicate,result,s,sc,o,oc,p,pc);
   if (unusedSubject)
      buildAggregatedIndexScan(query,Database::Order_Object_Predicate_Subject,result,o,oc,p,pc); else
      buildIndexScan(query,Database::Order_Object_Predicate_Subject,result,o,oc,p,pc,s,sc);
   if (unusedPredicate)
      buildAggregatedIndexScan(query,Database::Order_Object_Subject_Predicate,result,o,oc,s,sc); else
      buildIndexScan(query,Database::Order_Object_Subject_Predicate,result,o,oc,s,sc,p,pc);
   if (unusedObject)
      buildAggregatedIndexScan(query,Database::Order_Predicate_Subject_Object,result,p,pc,s,sc); else
      buildIndexScan(query,Database::Order_Predicate_Subject_Object,result,p,pc,s,sc,o,oc);
   if (unusedSubject)
      buildAggregatedIndexScan(query,Database::Order_Predicate_Object_Subject,result,p,pc,o,oc); else
      buildIndexScan(query,Database::Order_Predicate_Object_Subject,result,p,pc,o,oc,s,sc);

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
static double buildMaxSel(double sel,unsigned hits1,unsigned card1,unsigned card2)
   // Update the maximum selectivity
{
   double s1=static_cast<double>(hits1)/(static_cast<double>(card1)*card2);
   return max(sel,s1);
}
//---------------------------------------------------------------------------
PlanGen::JoinDescription PlanGen::buildJoinInfo(const QueryGraph::SubQuery& query,const QueryGraph::Edge& edge)
   // Build the informaion about a join
{
   // Fill in the relations involved
   JoinDescription result;
   result.left.set(edge.from);
   result.right.set(edge.to);

   // Extract patterns
   const QueryGraph::Node& l=query.nodes[edge.from],&r=query.nodes[edge.to];
   Database::DataOrder lo=Database::Order_Subject_Predicate_Object,ro=Database::Order_Subject_Predicate_Object;
   unsigned l1=l.constSubject?l.subject:~0u,l2=l.constPredicate?l.predicate:~0u,l3=l.constObject?l.object:~0u;
   unsigned r1=l.constSubject?r.subject:~0u,r2=r.constPredicate?r.predicate:~0u,r3=r.constObject?r.object:~0u;
   maximizePrefix(lo,l1,l2,l3);
   maximizePrefix(ro,r1,r2,r3);
   unsigned lv1=(!l.constSubject)?l.subject:~0u,lv2=(!l.constPredicate)?l.predicate:~0u,lv3=(!l.constObject)?l.object:~0u;
   unsigned rv1=(!l.constSubject)?r.subject:~0u,rv2=(!r.constPredicate)?r.predicate:~0u,rv3=(!r.constObject)?r.object:~0u;
   denormalizePattern(lo,lv1,lv2,lv3);
   denormalizePattern(ro,rv1,rv2,rv3);

   // Query the statistics
   StatisticsSegment::Bucket ls,rs;
   if (~l3) {
      db->getStatistics(lo).lookup(l1,l2,l3,ls);
   } else if (~l2) {
      db->getStatistics(lo).lookup(l1,l2,ls);
   } else if (~l1) {
      db->getStatistics(lo).lookup(l1,ls);
   } else {
      db->getStatistics(lo).lookup(ls);
   }
   if (~r3) {
      db->getStatistics(ro).lookup(r1,r2,r3,rs);
   } else if (~r2) {
      db->getStatistics(ro).lookup(r1,r2,rs);
   } else if (~r1) {
      db->getStatistics(ro).lookup(r1,rs);
   } else {
      db->getStatistics(ro).lookup(rs);
   }
   if (!ls.card) ls.card=1;
   if (!rs.card) rs.card=1;

   // Estimate the selectivity
   double lsel=(1.0/(ls.card*rs.card)),rsel=lsel;
   for (vector<unsigned>::const_iterator iter=edge.common.begin(),limit=edge.common.end();iter!=limit;++iter) {
      unsigned v=(*iter);
      if (v==lv1) {
         if ((v==r.subject)&&(!r.constSubject))
            lsel=buildMaxSel(lsel,ls.val1S,ls.card,rs.card);
         if ((v==r.predicate)&&(!r.constPredicate))
            lsel=buildMaxSel(lsel,ls.val1P,ls.card,rs.card);
         if ((v==r.object)&&(!r.constObject))
            lsel=buildMaxSel(lsel,ls.val1O,ls.card,rs.card);
      }
      if (v==lv2) {
         if ((v==r.subject)&&(!r.constSubject))
            lsel=buildMaxSel(lsel,ls.val2S,ls.card,rs.card);
         if ((v==r.predicate)&&(!r.constPredicate))
            lsel=buildMaxSel(lsel,ls.val2P,ls.card,rs.card);
         if ((v==r.object)&&(!r.constObject))
            lsel=buildMaxSel(lsel,ls.val2O,ls.card,rs.card);
      }
      if (v==lv3) {
         if ((v==r.subject)&&(!r.constSubject))
            lsel=buildMaxSel(lsel,ls.val3S,ls.card,rs.card);
         if ((v==r.predicate)&&(!r.constPredicate))
            lsel=buildMaxSel(lsel,ls.val3P,ls.card,rs.card);
         if ((v==r.object)&&(!r.constObject))
            lsel=buildMaxSel(lsel,ls.val3O,ls.card,rs.card);
      }
      if (v==rv1) {
         if ((v==l.subject)&&(!l.constSubject))
            rsel=buildMaxSel(rsel,rs.val1S,rs.card,ls.card);
         if ((v==l.predicate)&&(!l.constPredicate))
            rsel=buildMaxSel(rsel,rs.val1P,rs.card,ls.card);
         if ((v==l.object)&&(!l.constObject))
            rsel=buildMaxSel(rsel,ls.val1O,ls.card,ls.card);
      }
      if (v==rv2) {
         if ((v==l.subject)&&(!l.constSubject))
            rsel=buildMaxSel(rsel,rs.val2S,rs.card,ls.card);
         if ((v==l.predicate)&&(!l.constPredicate))
            rsel=buildMaxSel(rsel,rs.val2P,rs.card,ls.card);
         if ((v==l.object)&&(!l.constObject))
            rsel=buildMaxSel(rsel,rs.val2O,rs.card,ls.card);
      }
      if (v==rv3) {
         if ((v==l.subject)&&(!l.constSubject))
            rsel=buildMaxSel(rsel,rs.val3S,rs.card,ls.card);
         if ((v==l.predicate)&&(!l.constPredicate))
            rsel=buildMaxSel(rsel,rs.val3P,rs.card,ls.card);
         if ((v==l.object)&&(!l.constObject))
            rsel=buildMaxSel(rsel,rs.val3O,rs.card,ls.card);
      }
   }
   result.selectivity=(lsel+rsel)/2.0;

   // Look up suitable orderings
   if (!edge.common.empty()) {
      result.ordering=edge.common.front(); // XXX multiple orderings possible
   } else {
      // Cross product
      result.ordering=(~0u)-1;
      result.selectivity=-1;
   }

   return result;
}
//---------------------------------------------------------------------------
PlanGen::Problem* PlanGen::buildOptional(const QueryGraph::SubQuery& query,unsigned id)
   // Generate an optional part
{
   // Solve the subproblem
   Plan* p=translate(query);

   // Create new problem instance
   Problem* result=problems.alloc();
   result->next=0;
   result->plans=p;
   result->relations=BitSet();
   result->relations.set(id);

   return result;
}
//---------------------------------------------------------------------------
static void collectVariables(const QueryGraph::SubQuery& query,set<unsigned>& vars,const void* except)
   // Collect all variables used in a subquery
{
   for (vector<QueryGraph::Filter>::const_iterator iter=query.filters.begin(),limit=query.filters.end();iter!=limit;++iter)
      if (except!=(&(*iter)))
         vars.insert((*iter).id);
   for (vector<QueryGraph::Node>::const_iterator iter=query.nodes.begin(),limit=query.nodes.end();iter!=limit;++iter) {
      const QueryGraph::Node& n=*iter;
      if (except==(&n))
         continue;
      if (!n.constSubject) vars.insert(n.subject);
      if (!n.constPredicate) vars.insert(n.predicate);
      if (!n.constObject) vars.insert(n.object);
   }
   for (vector<QueryGraph::SubQuery>::const_iterator iter=query.optional.begin(),limit=query.optional.end();iter!=limit;++iter)
      if (except!=(&(*iter)))
         collectVariables(*iter,vars,except);
   for (vector<vector<QueryGraph::SubQuery> >::const_iterator iter=query.unions.begin(),limit=query.unions.end();iter!=limit;++iter)
      if (except!=(&(*iter)))
         for (vector<QueryGraph::SubQuery>::const_iterator iter2=(*iter).begin(),limit2=(*iter).end();iter2!=limit2;++iter2)
            if (except!=(&(*iter2)))
               collectVariables(*iter2,vars,except);
}
//---------------------------------------------------------------------------
static void collectVariables(const QueryGraph& query,set<unsigned>& vars,const void* except)
   // Collect all variables used in a query
{
   for (QueryGraph::projection_iterator iter=query.projectionBegin(),limit=query.projectionEnd();iter!=limit;++iter)
      vars.insert(*iter);
   if (except!=(&(query.getQuery())))
      collectVariables(query.getQuery(),vars,except);
}
//---------------------------------------------------------------------------
static Plan* findOrdering(Plan* root,unsigned ordering)
   // Find a plan with a specific ordering
{
   for (;root;root=root->next)
      if (root->ordering==ordering)
         return root;
   return 0;
}
//---------------------------------------------------------------------------
PlanGen::Problem* PlanGen::buildUnion(const vector<QueryGraph::SubQuery>& query,unsigned id)
   // Generate a union part
{
   // Solve the subproblems
   vector<Plan*> parts,solutions;
   for (unsigned index=0;index<query.size();index++) {
      Plan* p=translate(query[index]),*bp=p;
      for (Plan* iter=p;iter;iter=iter->next)
         if (iter->costs<bp->costs)
            bp=iter;
      parts.push_back(bp);
      solutions.push_back(p);
   }

   // Compute statistics
   Plan::card_t card=0;
   Plan::cost_t costs=0;
   for (vector<Plan*>::const_iterator iter=parts.begin(),limit=parts.end();iter!=limit;++iter) {
      card+=(*iter)->cardinality;
      costs+=(*iter)->costs;
   }

   // Create a new problem instance
   Problem* result=problems.alloc();
   result->next=0;
   result->plans=0;
   result->relations=BitSet();
   result->relations.set(id);

   // And create a union operator
   Plan* last=plans.alloc();
   last->op=Plan::Union;
   last->opArg=0;
   last->left=parts[0];
   last->right=parts[1];
   last->cardinality=card;
   last->costs=costs;
   last->ordering=~0u;
   last->next=0;
   result->plans=last;
   for (unsigned index=2;index<parts.size();index++) {
      Plan* nextPlan=plans.alloc();
      nextPlan->left=last->right;
      last->right=nextPlan;
      last=nextPlan;
      last->op=Plan::Union;
      last->opArg=0;
      last->right=parts[index];
      last->cardinality=card;
      last->costs=costs;
      last->ordering=~0u;
      last->next=0;
   }

   // Could we also use a merge union?
   set<unsigned> otherVars,unionVars;
   vector<unsigned> commonVars;
   collectVariables(*fullQuery,otherVars,&query);
   for (vector<QueryGraph::SubQuery>::const_iterator iter=query.begin(),limit=query.end();iter!=limit;++iter)
      collectVariables(*iter,unionVars,0);
   set_intersection(otherVars.begin(),otherVars.end(),unionVars.begin(),unionVars.end(),back_inserter(commonVars));
   if (commonVars.size()==1) {
      unsigned resultVar=commonVars[0];
      // Can we get all plans sorted in this way?
      bool canMerge=true;
      costs=0;
      for (vector<Plan*>::const_iterator iter=solutions.begin(),limit=solutions.end();iter!=limit;++iter) {
         Plan* p;
         if ((p=findOrdering(*iter,resultVar))==0) {
            canMerge=false;
            break;
         }
         costs+=p->costs;
      }
      // Yes, build the plan
      if (canMerge) {
         Plan* last=plans.alloc();
         last->op=Plan::MergeUnion;
         last->opArg=0;
         last->left=findOrdering(solutions[0],resultVar);
         last->right=findOrdering(solutions[1],resultVar);
         last->cardinality=card;
         last->costs=costs;
         last->ordering=resultVar;
         last->next=0;
         result->plans->next=last;
         for (unsigned index=2;index<solutions.size();index++) {
            Plan* nextPlan=plans.alloc();
            nextPlan->left=last->right;
            last->right=nextPlan;
            last=nextPlan;
            last->op=Plan::MergeUnion;
            last->opArg=0;
            last->right=findOrdering(solutions[index],resultVar);
            last->cardinality=card;
            last->costs=costs;
            last->ordering=resultVar;
            last->next=0;
         }
      }
   }

   return result;
}
//---------------------------------------------------------------------------
static bool isComplexFilterApplicable(const QueryGraph::ComplexFilter& filter,const std::set<unsigned> leftVariables,const std::set<unsigned>& rightVariables)
   // Check if a complex filter is applicable here
{
   if ((leftVariables.count(filter.id1))&&(!rightVariables.count(filter.id1))&&(!leftVariables.count(filter.id2))&&(rightVariables.count(filter.id2)))
      return true;
   if ((!leftVariables.count(filter.id1))&&(rightVariables.count(filter.id1))&&(leftVariables.count(filter.id2))&&(!rightVariables.count(filter.id2)))
      return true;
   return false;
}
//---------------------------------------------------------------------------
Plan* PlanGen::addComplexFilters(Plan* plan,const QueryGraph::SubQuery& query)
   // Greedily add complex filter expressions
{
   switch (plan->op) {
      case Plan::ComplexFilter:
         // Should never happen!
         break;
      case Plan::Union:
      case Plan::MergeUnion:
         // A nested subquery starts here, stop
         break;
      case Plan::IndexScan:
      case Plan::AggregatedIndexScan:
      case Plan::Filter:
      case Plan::NestedLoopFilter:
         // We reached a leaf. XXX check for complex filters involving a single pattern
         break;
      case Plan::NestedLoopJoin:
      case Plan::MergeJoin:
      case Plan::HashJoin:
         // A join
         {
            plan->left=addComplexFilters(plan->left,query);
            plan->right=addComplexFilters(plan->right,query);
            std::set<unsigned> leftVariables,rightVariables;
            CodeGen::collectVariables(leftVariables,plan->left);
            CodeGen::collectVariables(rightVariables,plan->right);
            for (vector<QueryGraph::ComplexFilter>::const_iterator iter=query.complexFilters.begin(),limit=query.complexFilters.end();iter!=limit;++iter) {
               if (isComplexFilterApplicable(*iter,leftVariables,rightVariables)) {
                  Plan* p=plans.alloc();
                  p->op=Plan::ComplexFilter;
                  p->opArg=0;
                  p->left=plan;
                  p->right=reinterpret_cast<Plan*>(const_cast<QueryGraph::ComplexFilter*>(&(*iter)));
                  // XXX derive correct statistics, propagate up!
                  p->cardinality=plan->cardinality;
                  p->costs=plan->costs;
                  p->ordering=plan->ordering;
                  p->next=0;
                  plan=p;
               }
            }
         }
         break;
      case Plan::HashGroupify:
         plan->left=addComplexFilters(plan->left,query);
         break;
   }
   return plan;
}
//---------------------------------------------------------------------------
Plan* PlanGen::translate(const QueryGraph::SubQuery& query)
   // Translate a query into an operator tree
{
   // Check if we could handle the query
   if ((query.nodes.size()+query.optional.size()+query.unions.size())>BitSet::maxWidth)
      return 0;

   // Seed the DP table with scans
   vector<Problem*> dpTable;
   dpTable.resize(query.nodes.size()+query.optional.size()+query.unions.size());
   Problem* last=0;
   unsigned id=0;
   for (vector<QueryGraph::Node>::const_iterator iter=query.nodes.begin(),limit=query.nodes.end();iter!=limit;++iter,++id) {
      Problem* p=buildScan(query,*iter,id);
      if (last)
         last->next=p; else
         dpTable[0]=p;
      last=p;
   }
   for (vector<QueryGraph::SubQuery>::const_iterator iter=query.optional.begin(),limit=query.optional.end();iter!=limit;++iter,++id) {
      Problem* p=buildOptional(*iter,id);
      if (last)
         last->next=p; else
         dpTable[0]=p;
      last=p;
   }
   for (vector<vector<QueryGraph::SubQuery> >::const_iterator iter=query.unions.begin(),limit=query.unions.end();iter!=limit;++iter,++id) {
      Problem* p=buildUnion(*iter,id);
      if (last)
         last->next=p; else
         dpTable[0]=p;
      last=p;
   }

   // Construct the join info
   vector<JoinDescription> joins;
   for (vector<QueryGraph::Edge>::const_iterator iter=query.edges.begin(),limit=query.edges.end();iter!=limit;++iter)
      joins.push_back(buildJoinInfo(query,*iter));

   // Build larger join trees
   vector<unsigned> joinOrderings;
   for (unsigned index=1;index<dpTable.size();index++) {
      map<BitSet,Problem*> lookup;
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
               for (vector<JoinDescription>::const_iterator iter3=joins.begin(),limit3=joins.end();iter3!=limit3;++iter3)
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
                        // selectivity*=(*iter3).selectivity;
                     }
                     break;
                  }
               if (!problem) continue;

               // Combine phyiscal plans
               for (Plan* leftPlan=iter->plans;leftPlan;leftPlan=leftPlan->next) {
                  for (Plan* rightPlan=iter2->plans;rightPlan;rightPlan=rightPlan->next) {
                     // Try a merge joins
                     if (leftPlan->ordering==rightPlan->ordering) {
                        for (vector<unsigned>::const_iterator iter=joinOrderings.begin(),limit=joinOrderings.end();iter!=limit;++iter) {
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
                     if (selectivity>=0) {
                        Plan* p=plans.alloc();
                        p->op=Plan::HashJoin;
                        p->opArg=0;
                        p->left=leftPlan;
                        p->right=rightPlan;
                        p->cardinality=leftPlan->cardinality*rightPlan->cardinality*selectivity;
                        p->costs=leftPlan->costs+rightPlan->costs+Costs::hashJoin(leftPlan->cardinality,rightPlan->cardinality);
                        p->ordering=~0u;
                        addPlan(problem,p);
                        // Second order
                        p=plans.alloc();
                        p->op=Plan::HashJoin;
                        p->opArg=0;
                        p->left=rightPlan;
                        p->right=leftPlan;
                        p->cardinality=leftPlan->cardinality*rightPlan->cardinality*selectivity;
                        p->costs=leftPlan->costs+rightPlan->costs+Costs::hashJoin(rightPlan->cardinality,leftPlan->cardinality);
                        p->ordering=~0u;
                        addPlan(problem,p);
                     } else {
                        // Nested loop join
                        Plan* p=plans.alloc();
                        p->op=Plan::NestedLoopJoin;
                        p->opArg=0;
                        p->left=leftPlan;
                        p->right=rightPlan;
                        p->cardinality=leftPlan->cardinality*rightPlan->cardinality;
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
   // Extract the bestplan
   if (!dpTable.back())
      return 0;
   Plan* plan=dpTable.back()->plans;
   if (!plan)
      return 0;

   // Greedily add complex filter predicates
   if (!query.complexFilters.empty())
      plan=addComplexFilters(plan,query);

   // Return the complete plan
   return plan;
}
//---------------------------------------------------------------------------
Plan* PlanGen::translate(Database& db,const QueryGraph& query)
   // Translate a query into an operator tree
{
   // Reset the plan generator
   plans.clear();
   problems.freeAll();
   this->db=&db;
   fullQuery=&query;

   // Retrieve the base plan
   Plan* plan=translate(query.getQuery());
   if (!plan)
      return 0;
   Plan* best=0;
   for (Plan* iter=plan;iter;iter=iter->next)
      if ((!best)||(iter->costs<best->costs)||((iter->costs==best->costs)&&(iter->cardinality<best->cardinality)))
         best=iter;
   if (!best)
      return 0;

   // Aggregate, if required
   if ((query.getDuplicateHandling()==QueryGraph::CountDuplicates)||(query.getDuplicateHandling()==QueryGraph::NoDuplicates)||(query.getDuplicateHandling()==QueryGraph::ShowDuplicates)) {
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
