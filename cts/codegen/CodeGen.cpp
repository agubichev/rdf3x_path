#include "cts/codegen/CodeGen.hpp"
#include "cts/infra/QueryGraph.hpp"
#include "cts/plangen/Plan.hpp"
#include "rts/operator/AggregatedIndexScan.hpp"
#include "rts/operator/EmptyScan.hpp"
#include "rts/operator/Filter.hpp"
#include "rts/operator/FullyAggregatedIndexScan.hpp"
#include "rts/operator/HashGroupify.hpp"
#include "rts/operator/HashJoin.hpp"
#include "rts/operator/IndexScan.hpp"
#include "rts/operator/MergeJoin.hpp"
#include "rts/operator/MergeUnion.hpp"
#include "rts/operator/NestedLoopFilter.hpp"
#include "rts/operator/NestedLoopJoin.hpp"
#include "rts/operator/ResultsPrinter.hpp"
#include "rts/operator/Selection.hpp"
#include "rts/operator/SingletonScan.hpp"
#include "rts/operator/TupleCounter.hpp"
#include "rts/operator/Union.hpp"
#include "rts/runtime/Runtime.hpp"
#include <cstdlib>
#include <map>
#include <set>
#include <cassert>
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
static Operator* translatePlan(Runtime& runtime,const map<unsigned,Register*>& context,const set<unsigned>& projection,map<unsigned,Register*>& bindings,const map<const QueryGraph::Node*,unsigned>& registers,Plan* plan);
//---------------------------------------------------------------------------
static void resolveScanVariable(Runtime& runtime,const map<unsigned,Register*>& context,const set<unsigned>& projection,map<unsigned,Register*>& bindings,const map<const QueryGraph::Node*,unsigned>& registers,unsigned slot,const QueryGraph::Node& node,Register*& reg,bool& bound,bool unused=false)
   // Resolve a variable used in a scan
{
   bool constant=(slot==0)?node.constSubject:((slot==1)?node.constPredicate:node.constObject);
   unsigned var=(slot==0)?node.subject:((slot==1)?node.predicate:node.object);
   reg=runtime.getRegister((*registers.find(&node)).second+slot);
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
static Operator* translateIndexScan(Runtime& runtime,const map<unsigned,Register*>& context,const set<unsigned>& projection,map<unsigned,Register*>& bindings,const map<const QueryGraph::Node*,unsigned>& registers,Plan* plan)
   // Translate an index scan into an operator tree
{
   const QueryGraph::Node& node=*reinterpret_cast<QueryGraph::Node*>(plan->right);

   // Initialize the registers
   bool constSubject,constPredicate,constObject;
   Register* subject,*predicate,*object;
   resolveScanVariable(runtime,context,projection,bindings,registers,0,node,subject,constSubject);
   resolveScanVariable(runtime,context,projection,bindings,registers,1,node,predicate,constPredicate);
   resolveScanVariable(runtime,context,projection,bindings,registers,2,node,object,constObject);

   // And return the operator
   return IndexScan::create(runtime.getDatabase(),static_cast<Database::DataOrder>(plan->opArg),
                            subject,constSubject,
                            predicate,constPredicate,
                            object,constObject);
}
//---------------------------------------------------------------------------
static Operator* translateAggregatedIndexScan(Runtime& runtime,const map<unsigned,Register*>& context,const set<unsigned>& projection,map<unsigned,Register*>& bindings,const map<const QueryGraph::Node*,unsigned>& registers,Plan* plan)
   // Translate an aggregated index scan into an operator tree
{
   const QueryGraph::Node& node=*reinterpret_cast<QueryGraph::Node*>(plan->right);
   Database::DataOrder order=static_cast<Database::DataOrder>(plan->opArg);

   // Initialize the registers
   bool constSubject,constPredicate,constObject;
   Register* subject,*predicate,*object;
   resolveScanVariable(runtime,context,projection,bindings,registers,0,node,subject,constSubject,(order==Database::Order_Object_Predicate_Subject)||(order==Database::Order_Predicate_Object_Subject));
   resolveScanVariable(runtime,context,projection,bindings,registers,1,node,predicate,constPredicate,(order==Database::Order_Subject_Object_Predicate)||(order==Database::Order_Object_Subject_Predicate));
   resolveScanVariable(runtime,context,projection,bindings,registers,2,node,object,constObject,(order==Database::Order_Subject_Predicate_Object)||(order==Database::Order_Predicate_Subject_Object));

   // And return the operator
   return AggregatedIndexScan::create(runtime.getDatabase(),order,
                                      subject,constSubject,
                                      predicate,constPredicate,
                                      object,constObject);
}
//---------------------------------------------------------------------------
static Operator* translateFullyAggregatedIndexScan(Runtime& runtime,const map<unsigned,Register*>& context,const set<unsigned>& projection,map<unsigned,Register*>& bindings,const map<const QueryGraph::Node*,unsigned>& registers,Plan* plan)
   // Translate an fully aggregated index scan into an operator tree
{
   const QueryGraph::Node& node=*reinterpret_cast<QueryGraph::Node*>(plan->right);
   Database::DataOrder order=static_cast<Database::DataOrder>(plan->opArg);

   // Initialize the registers
   bool constSubject,constPredicate,constObject;
   Register* subject,*predicate,*object;
   resolveScanVariable(runtime,context,projection,bindings,registers,0,node,subject,constSubject,(order!=Database::Order_Subject_Predicate_Object)&&(order!=Database::Order_Subject_Object_Predicate));
   resolveScanVariable(runtime,context,projection,bindings,registers,1,node,predicate,constPredicate,(order!=Database::Order_Predicate_Subject_Object)&&(order==Database::Order_Predicate_Object_Subject));
   resolveScanVariable(runtime,context,projection,bindings,registers,2,node,object,constObject,(order!=Database::Order_Object_Subject_Predicate)&&(order!=Database::Order_Object_Predicate_Subject));

   // And return the operator
   return FullyAggregatedIndexScan::create(runtime.getDatabase(),order,
                                           subject,constSubject,
                                           predicate,constPredicate,
                                           object,constObject);
}
//---------------------------------------------------------------------------
static void collectVariables(const map<unsigned,Register*>& context,set<unsigned>& variables,Plan* plan)
   // Collect all variables contained in a plan
{
   switch (plan->op) {
      case Plan::IndexScan:
      case Plan::AggregatedIndexScan:
      case Plan::FullyAggregatedIndexScan: {
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
      case Plan::Union:
      case Plan::MergeUnion:
         collectVariables(context,variables,plan->left);
         collectVariables(context,variables,plan->right);
         break;
      case Plan::HashGroupify:
      case Plan::Filter:
      case Plan::NestedLoopFilter:
      case Plan::ComplexFilter:
         collectVariables(context,variables,plan->left);
         break;

   }
}
//---------------------------------------------------------------------------
static void getJoinVariables(const map<unsigned,Register*>& context,set<unsigned>& variables,Plan* left,Plan* right)
   // Get the join variables
{
   // Collect all variables
   set<unsigned> leftVariables,rightVariables;
   collectVariables(context,leftVariables,left);
   collectVariables(context,rightVariables,right);

   // Find common ones
   if (leftVariables.size()<rightVariables.size()) {
      for (set<unsigned>::const_iterator iter=leftVariables.begin(),limit=leftVariables.end();iter!=limit;++iter)
         if (rightVariables.count(*iter))
            variables.insert(*iter);
   } else {
      for (set<unsigned>::const_iterator iter=rightVariables.begin(),limit=rightVariables.end();iter!=limit;++iter)
         if (leftVariables.count(*iter))
            variables.insert(*iter);
   }
}
//---------------------------------------------------------------------------
static void mergeBindings(const set<unsigned>& projection,map<unsigned,Register*>& bindings,const map<unsigned,Register*>& leftBindings,const map<unsigned,Register*>& rightBindings)
   // Merge bindings after a join
{
   for (map<unsigned,Register*>::const_iterator iter=leftBindings.begin(),limit=leftBindings.end();iter!=limit;++iter)
      if (projection.count((*iter).first))
         bindings[(*iter).first]=(*iter).second;
   for (map<unsigned,Register*>::const_iterator iter=rightBindings.begin(),limit=rightBindings.end();iter!=limit;++iter)
      if (projection.count((*iter).first)&&(!bindings.count((*iter).first)))
         bindings[(*iter).first]=(*iter).second;
}
//---------------------------------------------------------------------------
static Operator* addAdditionalSelections(Operator* input,const set<unsigned>& joinVariables,map<unsigned,Register*>& leftBindings,map<unsigned,Register*>& rightBindings,unsigned joinedOn)
   // Convert additional join predicates into a selection
{
   // Examine join conditions
   vector<Register*> conditions;
   for (set<unsigned>::const_iterator iter=joinVariables.begin(),limit=joinVariables.end();iter!=limit;++iter) {
      if ((*iter)!=joinedOn) {
         conditions.push_back(leftBindings[*iter]);
         conditions.push_back(rightBindings[*iter]);
      }
   }

   // Build the results
   if (!conditions.empty())
      return Selection::create(input,conditions,true); else
      return input;
}
//---------------------------------------------------------------------------
static Operator* translateNestedLoopJoin(Runtime& runtime,const map<unsigned,Register*>& context,const set<unsigned>& projection,map<unsigned,Register*>& bindings,const map<const QueryGraph::Node*,unsigned>& registers,Plan* plan)
   // Translate a nested loop join into an operator tree
{
   // Get the join variables (if any)
   set<unsigned> joinVariables,newProjection=projection;
   getJoinVariables(context,joinVariables,plan->left,plan->right);
   newProjection.insert(joinVariables.begin(),joinVariables.end());

   // Build the input trees
   map<unsigned,Register*> leftBindings,rightBindings;
   Operator* leftTree=translatePlan(runtime,context,newProjection,leftBindings,registers,plan->left);
   Operator* rightTree=translatePlan(runtime,context,newProjection,rightBindings,registers,plan->right);
   mergeBindings(projection,bindings,leftBindings,rightBindings);

   // Build the operator
   Operator* result=new NestedLoopJoin(leftTree,rightTree);

   // And apply additional selections if necessary
   result=addAdditionalSelections(result,joinVariables,leftBindings,rightBindings,~0u);

   return result;
}
//---------------------------------------------------------------------------
static Operator* translateMergeJoin(Runtime& runtime,const map<unsigned,Register*>& context,const set<unsigned>& projection,map<unsigned,Register*>& bindings,const map<const QueryGraph::Node*,unsigned>& registers,Plan* plan)
   // Translate a merge join into an operator tree
{
   // Get the join variables (if any)
   set<unsigned> joinVariables,newProjection=projection;
   getJoinVariables(context,joinVariables,plan->left,plan->right);
   newProjection.insert(joinVariables.begin(),joinVariables.end());
   assert(!joinVariables.empty());
   unsigned joinOn=plan->opArg;
   assert(joinVariables.count(joinOn));

   // Build the input trees
   map<unsigned,Register*> leftBindings,rightBindings;
   Operator* leftTree=translatePlan(runtime,context,newProjection,leftBindings,registers,plan->left);
   Operator* rightTree=translatePlan(runtime,context,newProjection,rightBindings,registers,plan->right);
   mergeBindings(projection,bindings,leftBindings,rightBindings);

   // Prepare the tails
   vector<Register*> leftTail,rightTail;
   for (map<unsigned,Register*>::const_iterator iter=leftBindings.begin(),limit=leftBindings.end();iter!=limit;++iter)
      if ((*iter).first!=joinOn)
         leftTail.push_back((*iter).second);
   for (map<unsigned,Register*>::const_iterator iter=rightBindings.begin(),limit=rightBindings.end();iter!=limit;++iter)
      if ((*iter).first!=joinOn)
         rightTail.push_back((*iter).second);

   // Build the operator
   Operator* result=new MergeJoin(leftTree,leftBindings[joinOn],leftTail,rightTree,rightBindings[joinOn],rightTail);

   // And apply additional selections if necessary
   result=addAdditionalSelections(result,joinVariables,leftBindings,rightBindings,joinOn);

   return result;
}
//---------------------------------------------------------------------------
static Operator* translateHashJoin(Runtime& runtime,const map<unsigned,Register*>& context,const set<unsigned>& projection,map<unsigned,Register*>& bindings,const map<const QueryGraph::Node*,unsigned>& registers,Plan* plan)
   // Translate a hash join into an operator tree
{
   // Get the join variables (if any)
   set<unsigned> joinVariables,newProjection=projection;
   getJoinVariables(context,joinVariables,plan->left,plan->right);
   newProjection.insert(joinVariables.begin(),joinVariables.end());
   assert(!joinVariables.empty());
   unsigned joinOn=*(joinVariables.begin());

   // Build the input trees
   map<unsigned,Register*> leftBindings,rightBindings;
   Operator* leftTree=translatePlan(runtime,context,newProjection,leftBindings,registers,plan->left);
   Operator* rightTree=translatePlan(runtime,context,newProjection,rightBindings,registers,plan->right);
   mergeBindings(projection,bindings,leftBindings,rightBindings);

   // Prepare the tails
   vector<Register*> leftTail,rightTail;
   for (map<unsigned,Register*>::const_iterator iter=leftBindings.begin(),limit=leftBindings.end();iter!=limit;++iter)
      if ((*iter).first!=joinOn)
         leftTail.push_back((*iter).second);
   for (map<unsigned,Register*>::const_iterator iter=rightBindings.begin(),limit=rightBindings.end();iter!=limit;++iter)
      if ((*iter).first!=joinOn)
         rightTail.push_back((*iter).second);

   // Build the operator
   Operator* result=new HashJoin(leftTree,leftBindings[joinOn],leftTail,rightTree,rightBindings[joinOn],rightTail,-plan->left->costs,plan->right->costs);

   // And apply additional selections if necessary
   result=addAdditionalSelections(result,joinVariables,leftBindings,rightBindings,joinOn);

   return result;
}
//---------------------------------------------------------------------------
static Operator* translateHashGroupify(Runtime& runtime,const map<unsigned,Register*>& context,const set<unsigned>& projection,map<unsigned,Register*>& bindings,const map<const QueryGraph::Node*,unsigned>& registers,Plan* plan)
   // Translate a hash groupify into an operator tree
{
   // Build the input trees
   Operator* tree=translatePlan(runtime,context,projection,bindings,registers,plan->left);

   // Collect output registers
   vector<Register*> output;
   for (map<unsigned,Register*>::const_iterator iter=bindings.begin(),limit=bindings.end();iter!=limit;++iter)
      output.push_back((*iter).second);

   // Build the operator
   return new HashGroupify(tree,output);
}
//---------------------------------------------------------------------------
static Operator* translateFilter(Runtime& runtime,const map<unsigned,Register*>& context,const set<unsigned>& projection,map<unsigned,Register*>& bindings,const map<const QueryGraph::Node*,unsigned>& registers,Plan* plan)
   // Translate a filter into an operator tree
{
   const QueryGraph::Filter& filter=*reinterpret_cast<QueryGraph::Filter*>(plan->right);

   // Build the input trees
   set<unsigned> newProjection=projection;
   newProjection.insert(filter.id);
   Operator* tree=translatePlan(runtime,context,newProjection,bindings,registers,plan->left);

   // Build the operator
   Operator* result=new Filter(tree,bindings[filter.id],filter.values,filter.exclude);

   // Cleanup the binding
   if (!projection.count(filter.id))
      bindings.erase(filter.id);

   return result;
}
//---------------------------------------------------------------------------
static Operator* translateNestedLoopFilter(Runtime& runtime,const map<unsigned,Register*>& context,const set<unsigned>& projection,map<unsigned,Register*>& bindings,const map<const QueryGraph::Node*,unsigned>& registers,Plan* plan)
   // Translate a nested loop filter into an operator tree
{
   const QueryGraph::Filter& filter=*reinterpret_cast<QueryGraph::Filter*>(plan->right);

   // Compute the scan paramers
   assert((plan->left->op==Plan::IndexScan)||(plan->left->op==Plan::AggregatedIndexScan));
   unsigned id=plan->left->left-static_cast<Plan*>(0);
   const QueryGraph::Node& node=*reinterpret_cast<QueryGraph::Node*>(plan->left->right);

   // Lookup the filter register
   Register* filterRegister=0;
   if ((!node.constSubject)&&(filter.id==node.subject)) filterRegister=runtime.getRegister(3*id+0); else
   if ((!node.constPredicate)&&(filter.id==node.predicate)) filterRegister=runtime.getRegister(3*id+1); else
   if ((!node.constObject)&&(filter.id==node.object)) filterRegister=runtime.getRegister(3*id+2); else {
      assert(false);
   }

   // Build the input trees
   set<unsigned> newProjection=projection;
   map<unsigned,Register*> newContext=context;
   newProjection.insert(filter.id);
   newContext[filter.id]=filterRegister;
   Operator* tree=translatePlan(runtime,newContext,newProjection,bindings,registers,plan->left);

   // Build the operator
   Operator* result=new NestedLoopFilter(tree,filterRegister,filter.values);

   // Setup the binding
   if (projection.count(filter.id))
      bindings[filter.id]=filterRegister;

   return result;
}
//---------------------------------------------------------------------------
static Operator* translateComplexFilter(Runtime& runtime,const map<unsigned,Register*>& context,const set<unsigned>& projection,map<unsigned,Register*>& bindings,const map<const QueryGraph::Node*,unsigned>& registers,Plan* plan)
   // Translate a complex filter into an operator tree
{
   const QueryGraph::ComplexFilter& filter=*reinterpret_cast<QueryGraph::ComplexFilter*>(plan->right);

   // Build the input trees
   set<unsigned> newProjection=projection;
   newProjection.insert(filter.id1);
   newProjection.insert(filter.id2);
   Operator* tree=translatePlan(runtime,context,newProjection,bindings,registers,plan->left);

   // Build the operator
   vector<Register*> condition;
   condition.push_back(bindings[filter.id1]);
   condition.push_back(bindings[filter.id2]);
   Operator* result=Selection::create(tree,condition,filter.equal);

   // Cleanup the binding
   if (!projection.count(filter.id1))
      bindings.erase(filter.id1);
   if (!projection.count(filter.id2))
      bindings.erase(filter.id2);

   return result;
}
//---------------------------------------------------------------------------
static Operator* translateUnion(Runtime& runtime,const map<unsigned,Register*>& context,const set<unsigned>& projection,map<unsigned,Register*>& bindings,const map<const QueryGraph::Node*,unsigned>& registers,Plan* plan)
   // Translate a union into an operator tree
{
   // Collect the parts
   vector<Plan*> parts;
   while (true) {
      parts.push_back(plan->left);
      if (plan->right->op!=Plan::Union) {
         parts.push_back(plan->right);
         break;
      } else plan=plan->right;
   }

   // Translate the parts of the union
   vector<map<unsigned,Register*> > subBindings;
   vector<Operator*> trees;
   subBindings.resize(parts.size());
   trees.resize(parts.size());
   for (unsigned index=0;index<parts.size();index++)
      trees[index]=translatePlan(runtime,context,projection,subBindings[index],registers,parts[index]);

   // Collect all bindings
   for (vector<map<unsigned,Register*> >::const_iterator iter=subBindings.begin(),limit=subBindings.end();iter!=limit;++iter)
      for (map<unsigned,Register*>::const_iterator iter2=(*iter).begin(),limit2=(*iter).end();iter2!=limit2;++iter2)
         if (!bindings.count((*iter2).first))
            bindings[(*iter2).first]=(*iter2).second;

   // Construct the mappings and initializations
   vector<vector<Register*> > mappings,initializations;
   mappings.resize(parts.size());
   initializations.resize(parts.size());
   for (unsigned index=0;index<subBindings.size();index++) {
      for (map<unsigned,Register*>::const_iterator iter=subBindings[index].begin(),limit=subBindings[index].end();iter!=limit;++iter)
         if (bindings[(*iter).first]!=(*iter).second) {
            mappings[index].push_back((*iter).second);
            mappings[index].push_back(bindings[(*iter).first]);
         }
      for (map<unsigned,Register*>::const_iterator iter=bindings.begin(),limit=bindings.end();iter!=limit;++iter)
         if (!subBindings[index].count((*iter).first))
            initializations[index].push_back((*iter).second);
   }

   // Build the operator
   Operator* result=new Union(trees,mappings,initializations);

   return result;
}
//---------------------------------------------------------------------------
static Operator* translateMergeUnion(Runtime& runtime,const map<unsigned,Register*>& context,const set<unsigned>& projection,map<unsigned,Register*>& bindings,const map<const QueryGraph::Node*,unsigned>& registers,Plan* plan)
   // Translate a merge union into an operator tree
{
   // Translate the input
   map<unsigned,Register*> leftBinding,rightBinding;
   Operator* left=translatePlan(runtime,context,projection,leftBinding,registers,plan->left);
   Operator* right=translatePlan(runtime,context,projection,rightBinding,registers,plan->right);

   // Collect the binding
   assert(leftBinding.size()==1);
   assert(rightBinding.size()==1);
   unsigned resultVar=(*(leftBinding.begin())).first;
   Register* leftReg=(*(leftBinding.begin())).second,*rightReg=(*(rightBinding.begin())).second;
   bindings[resultVar]=leftReg;

   // Build the operator
   Operator* result=new MergeUnion(leftReg,left,leftReg,right,rightReg);

   return result;
}
//---------------------------------------------------------------------------
static Operator* translatePlan(Runtime& runtime,const map<unsigned,Register*>& context,const set<unsigned>& projection,map<unsigned,Register*>& bindings,const map<const QueryGraph::Node*,unsigned>& registers,Plan* plan)
   // Translate a plan into an operator tree
{
   Operator* result=0;
   switch (plan->op) {
      case Plan::IndexScan: result=translateIndexScan(runtime,context,projection,bindings,registers,plan); break;
      case Plan::AggregatedIndexScan: result=translateAggregatedIndexScan(runtime,context,projection,bindings,registers,plan); break;
      case Plan::FullyAggregatedIndexScan: result=translateFullyAggregatedIndexScan(runtime,context,projection,bindings,registers,plan); break;
      case Plan::NestedLoopJoin: result=translateNestedLoopJoin(runtime,context,projection,bindings,registers,plan); break;
      case Plan::MergeJoin: result=translateMergeJoin(runtime,context,projection,bindings,registers,plan); break;
      case Plan::HashJoin: result=translateHashJoin(runtime,context,projection,bindings,registers,plan); break;
      case Plan::HashGroupify: result=translateHashGroupify(runtime,context,projection,bindings,registers,plan); break;
      case Plan::Filter: result=translateFilter(runtime,context,projection,bindings,registers,plan); break;
      case Plan::NestedLoopFilter: result=translateNestedLoopFilter(runtime,context,projection,bindings,registers,plan); break;
      case Plan::ComplexFilter: result=translateComplexFilter(runtime,context,projection,bindings,registers,plan); break;
      case Plan::Union: result=translateUnion(runtime,context,projection,bindings,registers,plan); break;
      case Plan::MergeUnion: result=translateMergeUnion(runtime,context,projection,bindings,registers,plan); break;
   }
   if (getenv("SHOWCARD"))
      result=new TupleCounter(result,plan->cardinality);
   return result;
}
//---------------------------------------------------------------------------
static unsigned allocateRegisters(map<const QueryGraph::Node*,unsigned>& registers,map<unsigned,set<unsigned> >& registerClasses,const QueryGraph::SubQuery& query,unsigned id)
   // Allocate registers
{
   for (vector<QueryGraph::Node>::const_iterator iter=query.nodes.begin(),limit=query.nodes.end();iter!=limit;++iter) {
      const QueryGraph::Node& node=*iter;
      registers[&node]=id;
      if (!node.constSubject)
         registerClasses[node.subject].insert(id+0);
      if (!node.constPredicate)
         registerClasses[node.predicate].insert(id+1);
      if (!node.constObject)
         registerClasses[node.object].insert(id+2);
      id+=3;
   }
   for (vector<QueryGraph::SubQuery>::const_iterator iter=query.optional.begin(),limit=query.optional.end();iter!=limit;++iter)
      id=allocateRegisters(registers,registerClasses,(*iter),id);
   for (vector<vector<QueryGraph::SubQuery> >::const_iterator iter=query.unions.begin(),limit=query.unions.end();iter!=limit;++iter)
      for (vector<QueryGraph::SubQuery>::const_iterator iter2=(*iter).begin(),limit2=(*iter).end();iter2!=limit2;++iter2)
         id=allocateRegisters(registers,registerClasses,(*iter2),id);
   return id;
}
//---------------------------------------------------------------------------
Operator* CodeGen::translate(Runtime& runtime,const QueryGraph& query,Plan* plan,bool silent)
   // Perform a naive translation of a query into an operator tree
{
   // Allocate registers for all relations
   map<const QueryGraph::Node*,unsigned> registers;
   map<unsigned,set<unsigned> > registerClasses;
   unsigned registerCount=allocateRegisters(registers,registerClasses,query.getQuery(),0);
   unsigned unboundVariable=registerCount;
   runtime.allocateRegisters(unboundVariable+1);

   // Prepare domain information for join attributes
   {
      // Count the required number of domains
      unsigned domainCount=0;
      for (map<unsigned,set<unsigned> >::const_iterator iter=registerClasses.begin(),limit=registerClasses.end();iter!=limit;++iter) {
         // No join attribute?
         if ((*iter).second.size()<2)
            continue;
         // We have a new domain
         domainCount++;
      }
      runtime.allocateDomainDescriptions(domainCount);

      // And assign registers to domains
      domainCount=0;
      for (map<unsigned,set<unsigned> >::const_iterator iter=registerClasses.begin(),limit=registerClasses.end();iter!=limit;++iter) {
         // No join attribute?
         if ((*iter).second.size()<2)
            continue;
         // Lookup the register addresses
         PotentialDomainDescription* domain=runtime.getDomainDescription(domainCount++);
         for (set<unsigned>::const_iterator iter2=(*iter).second.begin(),limit2=(*iter).second.end();iter2!=limit2;++iter2)
            runtime.getRegister(*iter2)->domain=domain;
      }
   }

   // Build the operator tree
   Operator* tree;
   vector<Register*> output;

   if (query.knownEmpty()) {
      tree=new EmptyScan();
   } else if (!plan) {
      tree=new SingletonScan();
   } else {
      // Construct the projection
      set<unsigned> projection;
      for (QueryGraph::projection_iterator iter=query.projectionBegin(),limit=query.projectionEnd();iter!=limit;++iter)
         projection.insert(*iter);

      // And build the tree
      map<unsigned,Register*> context,bindings;
      tree=translatePlan(runtime,context,projection,bindings,registers,plan);

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
      case QueryGraph::CountDuplicates: duplicateHandling=ResultsPrinter::CountDuplicates; break;
      case QueryGraph::ReducedDuplicates: duplicateHandling=ResultsPrinter::ReduceDuplicates; break;
      case QueryGraph::NoDuplicates: duplicateHandling=ResultsPrinter::ReduceDuplicates; break;
      case QueryGraph::ShowDuplicates: duplicateHandling=ResultsPrinter::ShowDuplicates; break;
   }
   tree=new ResultsPrinter(runtime.getDatabase(),tree,output,duplicateHandling,query.getLimit(),silent);

   return tree;
}
//---------------------------------------------------------------------------
void CodeGen::collectVariables(set<unsigned>& variables,Plan* plan)
   // Collect all variables contained in a plan
{
   ::collectVariables(map<unsigned,Register*>(),variables,plan);
}
//---------------------------------------------------------------------------
