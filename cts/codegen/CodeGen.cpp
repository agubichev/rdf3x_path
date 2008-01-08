#include "cts/codegen/CodeGen.hpp"
#include "cts/infra/QueryGraph.hpp"
#include "cts/plangen/Plan.hpp"
#include "rts/operator/AggregatedIndexScan.hpp"
#include "rts/operator/Filter.hpp"
#include "rts/operator/HashGroupify.hpp"
#include "rts/operator/HashJoin.hpp"
#include "rts/operator/IndexScan.hpp"
#include "rts/operator/MergeJoin.hpp"
#include "rts/operator/NestedLoopFilter.hpp"
#include "rts/operator/NestedLoopJoin.hpp"
#include "rts/operator/ResultsPrinter.hpp"
#include "rts/operator/Selection.hpp"
#include "rts/operator/SingletonScan.hpp"
#include "rts/runtime/Runtime.hpp"
#include <map>
#include <set>
#include <cassert>
//---------------------------------------------------------------------------
static Operator* translatePlan(Runtime& runtime,const std::map<unsigned,Register*>& context,const std::set<unsigned>& projection,std::map<unsigned,Register*>& bindings,Plan* plan);
//---------------------------------------------------------------------------
static void resolveScanVariable(Runtime& runtime,const std::map<unsigned,Register*>& context,const std::set<unsigned>& projection,std::map<unsigned,Register*>& bindings,unsigned id,unsigned slot,const QueryGraph::Node& node,Register*& reg,bool& bound,bool unused=false)
   // Resolve a variable used in a scan
{
   bool constant=(slot==0)?node.constSubject:((slot==1)?node.constPredicate:node.constObject);
   unsigned var=(slot==0)?node.subject:((slot==1)?node.predicate:node.object);
   reg=runtime.getRegister(3*id+slot);
   if (constant) {
      bound=true;
      reg->value=var;
   } else if (unused) {
      bound=false;
      reg=0;
   } else {
      if (context.count(var)) {
         bound=true;
         reg=(*(context.find(var))).second;
      } else {
         bound=false;
         if (projection.count(var))
            bindings[var]=reg;
      }
   }
}
//---------------------------------------------------------------------------
static Operator* translateIndexScan(Runtime& runtime,const std::map<unsigned,Register*>& context,const std::set<unsigned>& projection,std::map<unsigned,Register*>& bindings,Plan* plan)
   // Translate an index scan into an operator tree
{
   unsigned id=plan->left-static_cast<Plan*>(0);
   const QueryGraph::Node& node=*reinterpret_cast<QueryGraph::Node*>(plan->right);

   // Initialize the registers
   bool constSubject,constPredicate,constObject;
   Register* subject,*predicate,*object;
   resolveScanVariable(runtime,context,projection,bindings,id,0,node,subject,constSubject);
   resolveScanVariable(runtime,context,projection,bindings,id,1,node,predicate,constPredicate);
   resolveScanVariable(runtime,context,projection,bindings,id,2,node,object,constObject);

   // And return the operator
   return new IndexScan(runtime.getDatabase(),static_cast<Database::DataOrder>(plan->opArg),
                        subject,constSubject,
                        predicate,constPredicate,
                        object,constObject);
}
//---------------------------------------------------------------------------
static Operator* translateAggregatedIndexScan(Runtime& runtime,const std::map<unsigned,Register*>& context,const std::set<unsigned>& projection,std::map<unsigned,Register*>& bindings,Plan* plan)
   // Translate an aggregated index scan into an operator tree
{
   unsigned id=plan->left-static_cast<Plan*>(0);
   const QueryGraph::Node& node=*reinterpret_cast<QueryGraph::Node*>(plan->right);
   Database::DataOrder order=static_cast<Database::DataOrder>(plan->opArg);

   // Initialize the registers
   bool constSubject,constPredicate,constObject;
   Register* subject,*predicate,*object;
   resolveScanVariable(runtime,context,projection,bindings,id,0,node,subject,constSubject,(order==Database::Order_Object_Predicate_Subject)||(order==Database::Order_Predicate_Object_Subject));
   resolveScanVariable(runtime,context,projection,bindings,id,1,node,predicate,constPredicate,(order==Database::Order_Subject_Object_Predicate)||(order==Database::Order_Object_Subject_Predicate));
   resolveScanVariable(runtime,context,projection,bindings,id,2,node,object,constObject,(order==Database::Order_Subject_Predicate_Object)||(order==Database::Order_Predicate_Subject_Object));

   // And return the operator
   return new AggregatedIndexScan(runtime.getDatabase(),order,
                                  subject,constSubject,
                                  predicate,constPredicate,
                                  object,constObject);
}
//---------------------------------------------------------------------------
static void collectVariables(const std::map<unsigned,Register*>& context,std::set<unsigned>& variables,Plan* plan)
   // Collect all variables contained in a plan
{
   switch (plan->op) {
      case Plan::IndexScan:
      case Plan::AggregatedIndexScan: {
         const QueryGraph::Node& node=*reinterpret_cast<QueryGraph::Node*>(plan->right);
         if ((!node.constSubject)&&(!context.count(node.subject)))
            variables.insert(node.subject);
         if ((!node.constPredicate)&&(!context.count(node.predicate)))
            variables.insert(node.predicate);
         if ((!node.constObject)&&(!context.count(node.object)))
            variables.insert(node.object);
         break;
      }
      case Plan::NestedLoopJoin:
      case Plan::MergeJoin:
      case Plan::HashJoin:
         collectVariables(context,variables,plan->left);
         collectVariables(context,variables,plan->right);
         break;
      case Plan::HashGroupify:
      case Plan::Filter:
      case Plan::NestedLoopFilter:
         collectVariables(context,variables,plan->left);
         break;
   }
}
//---------------------------------------------------------------------------
static void getJoinVariables(const std::map<unsigned,Register*>& context,std::set<unsigned>& variables,Plan* left,Plan* right)
   // Get the join variables
{
   // Collect all variables
   std::set<unsigned> leftVariables,rightVariables;
   collectVariables(context,leftVariables,left);
   collectVariables(context,rightVariables,right);

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
static Operator* translateNestedLoopJoin(Runtime& runtime,const std::map<unsigned,Register*>& context,const std::set<unsigned>& projection,std::map<unsigned,Register*>& bindings,Plan* plan)
   // Translate a nested loop join into an operator tree
{
   // Get the join variables (if any)
   std::set<unsigned> joinVariables,newProjection=projection;
   getJoinVariables(context,joinVariables,plan->left,plan->right);
   newProjection.insert(joinVariables.begin(),joinVariables.end());

   // Build the input trees
   std::map<unsigned,Register*> leftBindings,rightBindings;
   Operator* leftTree=translatePlan(runtime,context,newProjection,leftBindings,plan->left);
   Operator* rightTree=translatePlan(runtime,context,newProjection,rightBindings,plan->right);
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
static Operator* translateMergeJoin(Runtime& runtime,const std::map<unsigned,Register*>& context,const std::set<unsigned>& projection,std::map<unsigned,Register*>& bindings,Plan* plan)
   // Translate a merge join into an operator tree
{
   // Get the join variables (if any)
   std::set<unsigned> joinVariables,newProjection=projection;
   getJoinVariables(context,joinVariables,plan->left,plan->right);
   newProjection.insert(joinVariables.begin(),joinVariables.end());
   assert(!joinVariables.empty());
   unsigned joinOn=plan->opArg;
   assert(joinVariables.count(joinOn));

   // Build the input trees
   std::map<unsigned,Register*> leftBindings,rightBindings;
   Operator* leftTree=translatePlan(runtime,context,newProjection,leftBindings,plan->left);
   Operator* rightTree=translatePlan(runtime,context,newProjection,rightBindings,plan->right);
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
static Operator* translateHashJoin(Runtime& runtime,const std::map<unsigned,Register*>& context,const std::set<unsigned>& projection,std::map<unsigned,Register*>& bindings,Plan* plan)
   // Translate a hash join into an operator tree
{
   // Get the join variables (if any)
   std::set<unsigned> joinVariables,newProjection=projection;
   getJoinVariables(context,joinVariables,plan->left,plan->right);
   newProjection.insert(joinVariables.begin(),joinVariables.end());
   assert(!joinVariables.empty());
   unsigned joinOn=*(joinVariables.begin());

   // Build the input trees
   std::map<unsigned,Register*> leftBindings,rightBindings;
   Operator* leftTree=translatePlan(runtime,context,newProjection,leftBindings,plan->left);
   Operator* rightTree=translatePlan(runtime,context,newProjection,rightBindings,plan->right);
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
static Operator* translateHashGroupify(Runtime& runtime,const std::map<unsigned,Register*>& context,const std::set<unsigned>& projection,std::map<unsigned,Register*>& bindings,Plan* plan)
   // Translate a hash groupify into an operator tree
{
   // Build the input trees
   Operator* tree=translatePlan(runtime,context,projection,bindings,plan->left);

   // Collect output registers
   std::vector<Register*> output;
   for (std::map<unsigned,Register*>::const_iterator iter=bindings.begin(),limit=bindings.end();iter!=limit;++iter)
      output.push_back((*iter).second);

   // Build the operator
   return new HashGroupify(tree,output);
}
//---------------------------------------------------------------------------
static Operator* translateFilter(Runtime& runtime,const std::map<unsigned,Register*>& context,const std::set<unsigned>& projection,std::map<unsigned,Register*>& bindings,Plan* plan)
   // Translate a filter into an operator tree
{
   const QueryGraph::Filter& filter=*reinterpret_cast<QueryGraph::Filter*>(plan->right);

   // Build the input trees
   std::set<unsigned> newProjection=projection;
   newProjection.insert(filter.id);
   Operator* tree=translatePlan(runtime,context,newProjection,bindings,plan->left);

   // Build the operator
   Operator* result=new Filter(tree,bindings[filter.id],filter.values);

   // Cleanup the binding
   if (!projection.count(filter.id))
      bindings.erase(filter.id);

   return result;
}
//---------------------------------------------------------------------------
static Operator* translateNestedLoopFilter(Runtime& runtime,const std::map<unsigned,Register*>& context,const std::set<unsigned>& projection,std::map<unsigned,Register*>& bindings,Plan* plan)
   // Translate a nested loop filter into an operator tree
{
   const QueryGraph::Filter& filter=*reinterpret_cast<QueryGraph::Filter*>(plan->right);

   // Compute the scan paramers
   assert((plan->left->op==Plan::IndexScan)||(plan->left->op==Plan::AggregatedIndexScan));
   unsigned id=plan->left->left-static_cast<Plan*>(0);
   const QueryGraph::Node& node=*reinterpret_cast<QueryGraph::Node*>(plan->left->right);

   // Lookup the filter register
   Register* filterRegister;
   if ((!node.constSubject)&&(filter.id==node.subject)) filterRegister=runtime.getRegister(3*id+0); else
   if ((!node.constPredicate)&&(filter.id==node.predicate)) filterRegister=runtime.getRegister(3*id+1); else
   if ((!node.constObject)&&(filter.id==node.object)) filterRegister=runtime.getRegister(3*id+2); else {
      assert(false);
   }

   // Build the input trees
   std::set<unsigned> newProjection=projection;
   std::map<unsigned,Register*> newContext=context;
   newProjection.insert(filter.id);
   newContext[filter.id]=filterRegister;
   Operator* tree=translatePlan(runtime,newContext,newProjection,bindings,plan->left);

   // Build the operator
   Operator* result=new NestedLoopFilter(tree,filterRegister,filter.values);

   // Setup the binding
   if (projection.count(filter.id))
      bindings[filter.id]=filterRegister;

   return result;
}
//---------------------------------------------------------------------------
static Operator* translatePlan(Runtime& runtime,const std::map<unsigned,Register*>& context,const std::set<unsigned>& projection,std::map<unsigned,Register*>& bindings,Plan* plan)
   // Translate a plan into an operator tree
{
   switch (plan->op) {
      case Plan::IndexScan: return translateIndexScan(runtime,context,projection,bindings,plan);
      case Plan::AggregatedIndexScan: return translateAggregatedIndexScan(runtime,context,projection,bindings,plan);
      case Plan::NestedLoopJoin: return translateNestedLoopJoin(runtime,context,projection,bindings,plan);
      case Plan::MergeJoin: return translateMergeJoin(runtime,context,projection,bindings,plan);
      case Plan::HashJoin: return translateHashJoin(runtime,context,projection,bindings,plan);
      case Plan::HashGroupify: return translateHashGroupify(runtime,context,projection,bindings,plan);
      case Plan::Filter: return translateFilter(runtime,context,projection,bindings,plan);
      case Plan::NestedLoopFilter: return translateNestedLoopFilter(runtime,context,projection,bindings,plan);
   }
   return 0;
}
//---------------------------------------------------------------------------
Operator* CodeGen::translate(Runtime& runtime,const QueryGraph& query,Plan* plan,bool silent)
   // Perform a naive translation of a query into an operator tree
{
   // Allocate registers for all relations
   unsigned unboundVariable=query.getNodeCount()*3;
   runtime.allocateRegisters(unboundVariable+1);

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
      std::map<unsigned,Register*> context,bindings;
      tree=translatePlan(runtime,context,projection,bindings,plan);

      // Remember the output registers
      for (QueryGraph::projection_iterator iter=query.projectionBegin(),limit=query.projectionEnd();iter!=limit;++iter)
         if (bindings.count(*iter))
            output.push_back(bindings[*iter]); else
            output.push_back(runtime.getRegister(unboundVariable));
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
