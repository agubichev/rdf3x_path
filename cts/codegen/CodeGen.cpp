#include "cts/codegen/CodeGen.hpp"
#include "cts/infra/QueryGraph.hpp"
#include "cts/plangen/Plan.hpp"
#include "rts/operator/AggregatedIndexScan.hpp"
#include "rts/operator/HashGroupify.hpp"
#include "rts/operator/HashJoin.hpp"
#include "rts/operator/IndexScan.hpp"
#include "rts/operator/MergeJoin.hpp"
#include "rts/operator/NestedLoopJoin.hpp"
#include "rts/operator/ResultsPrinter.hpp"
#include "rts/operator/Selection.hpp"
#include "rts/operator/SingletonScan.hpp"
#include "rts/runtime/Runtime.hpp"
#include <map>
#include <set>
#include <cassert>
//---------------------------------------------------------------------------
static Operator* translatePlan(Runtime& runtime,const std::set<unsigned>& projection,std::map<unsigned,Register*>& bindings,Plan* plan);
//---------------------------------------------------------------------------
static Operator* translateIndexScan(Runtime& runtime,const std::set<unsigned>& projection,std::map<unsigned,Register*>& bindings,Plan* plan)
   // Translate an index scan into an operator tree
{
   unsigned id=plan->left-static_cast<Plan*>(0);
   const QueryGraph::Node& node=*reinterpret_cast<QueryGraph::Node*>(plan->right);

   // Initialize the registers
   Register* subject=runtime.getRegister(3*id+0);
   if (node.constSubject)
      subject->value=node.subject; else
   if (projection.count(node.subject))
      bindings[node.subject]=subject;
   Register* predicate=runtime.getRegister(3*id+1);
   if (node.constPredicate)
      predicate->value=node.predicate; else
   if (projection.count(node.predicate))
      bindings[node.predicate]=predicate;
   Register* object=runtime.getRegister(3*id+2);
   if (node.constObject)
      object->value=node.object; else
   if (projection.count(node.object))
      bindings[node.object]=object;

   // And return the operator
   return new IndexScan(runtime.getDatabase(),static_cast<Database::DataOrder>(plan->opArg),
                        subject,node.constSubject,
                        predicate,node.constPredicate,
                        object,node.constObject);
}
//---------------------------------------------------------------------------
static Operator* translateAggregatedIndexScan(Runtime& runtime,const std::set<unsigned>& projection,std::map<unsigned,Register*>& bindings,Plan* plan)
   // Translate an aggregated index scan into an operator tree
{
   unsigned id=plan->left-static_cast<Plan*>(0);
   const QueryGraph::Node& node=*reinterpret_cast<QueryGraph::Node*>(plan->right);
   Database::DataOrder order=static_cast<Database::DataOrder>(plan->opArg);

   // Initialize the registers
   Register* subject=runtime.getRegister(3*id+0);
   if (node.constSubject)
      subject->value=node.subject; else
   if ((order==Database::Order_Object_Predicate_Subject)||(order==Database::Order_Predicate_Object_Subject))
      subject=0; else
   if (projection.count(node.subject))
      bindings[node.subject]=subject;
   Register* predicate=runtime.getRegister(3*id+1);
   if (node.constPredicate)
      predicate->value=node.predicate; else
   if ((order==Database::Order_Subject_Object_Predicate)||(order==Database::Order_Object_Subject_Predicate))
      predicate=0; else
   if (projection.count(node.predicate))
      bindings[node.predicate]=predicate;
   Register* object=runtime.getRegister(3*id+2);
   if (node.constObject)
      object->value=node.object; else
   if ((order==Database::Order_Subject_Predicate_Object)||(order==Database::Order_Predicate_Subject_Object))
      object=0; else
   if (projection.count(node.object))
      bindings[node.object]=object;

   // And return the operator
   return new AggregatedIndexScan(runtime.getDatabase(),order,
                                  subject,node.constSubject,
                                  predicate,node.constPredicate,
                                  object,node.constObject);
}
//---------------------------------------------------------------------------
static void collectVariables(std::set<unsigned>& variables,Plan* plan)
   // Collect all variables contained in a plan
{
   switch (plan->op) {
      case Plan::IndexScan:
      case Plan::AggregatedIndexScan: {
         const QueryGraph::Node& node=*reinterpret_cast<QueryGraph::Node*>(plan->right);
         if (!node.constSubject)
            variables.insert(node.subject);
         if (!node.constPredicate)
            variables.insert(node.predicate);
         if (!node.constObject)
            variables.insert(node.object);
         break;
      }
      case Plan::NestedLoopJoin:
      case Plan::MergeJoin:
      case Plan::HashJoin:
         collectVariables(variables,plan->left);
         collectVariables(variables,plan->right);
         break;
      case Plan::HashGroupify:
         collectVariables(variables,plan->left);
         break;
   }
}
//---------------------------------------------------------------------------
static void getJoinVariables(std::set<unsigned>& variables,Plan* left,Plan* right)
   // Get the join variables
{
   // Collect all variables
   std::set<unsigned> leftVariables,rightVariables;
   collectVariables(leftVariables,left);
   collectVariables(rightVariables,right);

   // Find common ones
   if (leftVariables.size()<rightVariables.size()) {
      for (std::set<unsigned>::const_iterator iter=leftVariables.begin(),limit=leftVariables.end();iter!=limit;++iter)
         if (rightVariables.count(*iter))
            variables.insert(*iter);
   } else {
      for (std::set<unsigned>::const_iterator iter=rightVariables.begin(),limit=rightVariables.end();iter!=limit;++iter)
         if (leftVariables.count(*iter))
            variables.insert(*iter);
   }
}
//---------------------------------------------------------------------------
static void mergeBindings(const std::set<unsigned>& projection,std::map<unsigned,Register*>& bindings,const std::map<unsigned,Register*>& leftBindings,const std::map<unsigned,Register*>& rightBindings)
   // Merge bindings after a join
{
   for (std::map<unsigned,Register*>::const_iterator iter=leftBindings.begin(),limit=leftBindings.end();iter!=limit;++iter)
      if (projection.count((*iter).first))
         bindings[(*iter).first]=(*iter).second;
   for (std::map<unsigned,Register*>::const_iterator iter=rightBindings.begin(),limit=rightBindings.end();iter!=limit;++iter)
      if (projection.count((*iter).first)&&(!bindings.count((*iter).first)))
         bindings[(*iter).first]=(*iter).second;
}
//---------------------------------------------------------------------------
static Operator* translateNestedLoopJoin(Runtime& runtime,const std::set<unsigned>& projection,std::map<unsigned,Register*>& bindings,Plan* plan)
   // Translate a nested loop join into an operator tree
{
   // Get the join variables (if any)
   std::set<unsigned> joinVariables,newProjection=projection;
   getJoinVariables(joinVariables,plan->left,plan->right);
   newProjection.insert(joinVariables.begin(),joinVariables.end());

   // Build the input trees
   std::map<unsigned,Register*> leftBindings,rightBindings;
   Operator* leftTree=translatePlan(runtime,newProjection,leftBindings,plan->left);
   Operator* rightTree=translatePlan(runtime,newProjection,rightBindings,plan->right);
   mergeBindings(projection,bindings,leftBindings,rightBindings);

   // Build the operator
   Operator* result=new NestedLoopJoin(leftTree,rightTree);

   // And apply additional selections if necessary
   if (!joinVariables.empty()) {
      std::vector<Register*> conditions;
      for (std::set<unsigned>::const_iterator iter=joinVariables.begin(),limit=joinVariables.end();iter!=limit;++iter) {
         conditions.push_back(leftBindings[*iter]);
         conditions.push_back(rightBindings[*iter]);
      }
      result=new Selection(result,conditions);
   }

   return result;
}
//---------------------------------------------------------------------------
static Operator* translateMergeJoin(Runtime& runtime,const std::set<unsigned>& projection,std::map<unsigned,Register*>& bindings,Plan* plan)
   // Translate a merge join into an operator tree
{
   // Get the join variables (if any)
   std::set<unsigned> joinVariables,newProjection=projection;
   getJoinVariables(joinVariables,plan->left,plan->right);
   newProjection.insert(joinVariables.begin(),joinVariables.end());
   assert(!joinVariables.empty());
   unsigned joinOn=plan->opArg;
   assert(joinVariables.count(joinOn));

   // Build the input trees
   std::map<unsigned,Register*> leftBindings,rightBindings;
   Operator* leftTree=translatePlan(runtime,newProjection,leftBindings,plan->left);
   Operator* rightTree=translatePlan(runtime,newProjection,rightBindings,plan->right);
   mergeBindings(projection,bindings,leftBindings,rightBindings);

   // Prepare the tails
   std::vector<Register*> leftTail,rightTail;
   for (std::map<unsigned,Register*>::const_iterator iter=leftBindings.begin(),limit=leftBindings.end();iter!=limit;++iter)
      if ((*iter).first!=joinOn)
         leftTail.push_back((*iter).second);
   for (std::map<unsigned,Register*>::const_iterator iter=rightBindings.begin(),limit=rightBindings.end();iter!=limit;++iter)
      if ((*iter).first!=joinOn)
         rightTail.push_back((*iter).second);

   // Build the operator
   Operator* result=new MergeJoin(leftTree,leftBindings[joinOn],leftTail,rightTree,rightBindings[joinOn],rightTail);

   // And apply additional selections if necessary
   if (joinVariables.size()>1) {
      std::vector<Register*> conditions;
      for (std::set<unsigned>::const_iterator iter=joinVariables.begin(),limit=joinVariables.end();iter!=limit;++iter) {
         if ((*iter)!=joinOn) {
            conditions.push_back(leftBindings[*iter]);
            conditions.push_back(rightBindings[*iter]);
         }
      }
      result=new Selection(result,conditions);
   }

   return result;
}
//---------------------------------------------------------------------------
static Operator* translateHashJoin(Runtime& runtime,const std::set<unsigned>& projection,std::map<unsigned,Register*>& bindings,Plan* plan)
   // Translate a hash join into an operator tree
{
   // Get the join variables (if any)
   std::set<unsigned> joinVariables,newProjection=projection;
   getJoinVariables(joinVariables,plan->left,plan->right);
   newProjection.insert(joinVariables.begin(),joinVariables.end());
   assert(!joinVariables.empty());
   unsigned joinOn=*(joinVariables.begin());

   // Build the input trees
   std::map<unsigned,Register*> leftBindings,rightBindings;
   Operator* leftTree=translatePlan(runtime,newProjection,leftBindings,plan->left);
   Operator* rightTree=translatePlan(runtime,newProjection,rightBindings,plan->right);
   mergeBindings(projection,bindings,leftBindings,rightBindings);

   // Prepare the tails
   std::vector<Register*> leftTail,rightTail;
   for (std::map<unsigned,Register*>::const_iterator iter=leftBindings.begin(),limit=leftBindings.end();iter!=limit;++iter)
      if ((*iter).first!=joinOn)
         leftTail.push_back((*iter).second);
   for (std::map<unsigned,Register*>::const_iterator iter=rightBindings.begin(),limit=rightBindings.end();iter!=limit;++iter)
      if ((*iter).first!=joinOn)
         rightTail.push_back((*iter).second);

   // Build the operator
   Operator* result=new HashJoin(leftTree,leftBindings[joinOn],leftTail,rightTree,rightBindings[joinOn],rightTail);

   // And apply additional selections if necessary
   if (joinVariables.size()>1) {
      std::vector<Register*> conditions;
      for (std::set<unsigned>::const_iterator iter=joinVariables.begin(),limit=joinVariables.end();iter!=limit;++iter) {
         if ((*iter)!=joinOn) {
            conditions.push_back(leftBindings[*iter]);
            conditions.push_back(rightBindings[*iter]);
         }
      }
      result=new Selection(result,conditions);
   }

   return result;
}
//---------------------------------------------------------------------------
static Operator* translateHashGroupify(Runtime& runtime,const std::set<unsigned>& projection,std::map<unsigned,Register*>& bindings,Plan* plan)
   // Translate a hash groupify into an operator tree
{
   // Build the input trees
   Operator* tree=translatePlan(runtime,projection,bindings,plan->left);

   // Collect output registers
   std::vector<Register*> output;
   for (std::map<unsigned,Register*>::const_iterator iter=bindings.begin(),limit=bindings.end();iter!=limit;++iter)
      output.push_back((*iter).second);

   // Build the operator
   return new HashGroupify(tree,output);
}
//---------------------------------------------------------------------------
static Operator* translatePlan(Runtime& runtime,const std::set<unsigned>& projection,std::map<unsigned,Register*>& bindings,Plan* plan)
   // Translate a plan into an operator tree
{
   switch (plan->op) {
      case Plan::IndexScan: return translateIndexScan(runtime,projection,bindings,plan);
      case Plan::AggregatedIndexScan: return translateAggregatedIndexScan(runtime,projection,bindings,plan);
      case Plan::NestedLoopJoin: return translateNestedLoopJoin(runtime,projection,bindings,plan);
      case Plan::MergeJoin: return translateMergeJoin(runtime,projection,bindings,plan);
      case Plan::HashJoin: return translateHashJoin(runtime,projection,bindings,plan);
      case Plan::HashGroupify: return translateHashGroupify(runtime,projection,bindings,plan);
      default: return 0;
   }
}
//---------------------------------------------------------------------------
Operator* CodeGen::translate(Runtime& runtime,const QueryGraph& query,Plan* plan,bool silent)
   // Perform a naive translation of a query into an operator tree
{
   // Allocate registers for all relations
   runtime.allocateRegisters(query.getNodeCount()*3);

   // Build the operator tree
   Operator* tree;
   std::vector<Register*> output;
   if (query.nodesBegin()==query.nodesEnd()) {
      tree=new SingletonScan();
   } else {
      // Construct the projection
      std::set<unsigned> projection;
      for (QueryGraph::projection_iterator iter=query.projectionBegin(),limit=query.projectionEnd();iter!=limit;++iter)
         projection.insert(*iter);

      // And build the tree
      std::map<unsigned,Register*> bindings;
      tree=translatePlan(runtime,projection,bindings,plan);

      // Remember the output registers
      for (QueryGraph::projection_iterator iter=query.projectionBegin(),limit=query.projectionEnd();iter!=limit;++iter)
         output.push_back(bindings[*iter]);
   }

   // And add the output generation
   ResultsPrinter::DuplicateHandling duplicateHandling=ResultsPrinter::ExpandDuplicates;
   switch (query.getDuplicateHandling()) {
      case QueryGraph::AllDuplicates: duplicateHandling=ResultsPrinter::ExpandDuplicates; break;
      case QueryGraph::CountDuplicates: duplicateHandling=ResultsPrinter::CountDuplicates; break; // XXX additional group by required
      case QueryGraph::ReducedDuplicates: duplicateHandling=ResultsPrinter::ReduceDuplicates; break;
      case QueryGraph::NoDuplicates: duplicateHandling=ResultsPrinter::ReduceDuplicates; break; // XXX additional group by required
   }
   tree=new ResultsPrinter(runtime.getDatabase(),tree,output,duplicateHandling,silent);

   return tree;
}
//---------------------------------------------------------------------------
