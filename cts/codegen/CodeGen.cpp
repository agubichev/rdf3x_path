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
#include "rts/operator/Union.hpp"
#include "rts/runtime/Runtime.hpp"
#include "rts/runtime/DifferentialIndex.hpp"
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
   if (runtime.hasDifferentialIndex())
      return runtime.getDifferentialIndex().createScan(static_cast<Database::DataOrder>(plan->opArg),subject,constSubject,predicate,constPredicate,object,constObject,static_cast<unsigned>(plan->cardinality));
   return IndexScan::create(runtime.getDatabase(),static_cast<Database::DataOrder>(plan->opArg),
                            subject,constSubject,
                            predicate,constPredicate,
                            object,constObject,
			    static_cast<unsigned>(plan->cardinality));
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
   if (runtime.hasDifferentialIndex())
      return runtime.getDifferentialIndex().createAggregatedScan(static_cast<Database::DataOrder>(plan->opArg),subject,constSubject,predicate,constPredicate,object,constObject,static_cast<unsigned>(plan->cardinality));
   return AggregatedIndexScan::create(runtime.getDatabase(),order,
                                      subject,constSubject,
                                      predicate,constPredicate,
                                      object,constObject,
				      static_cast<unsigned>(plan->cardinality));
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
   resolveScanVariable(runtime,context,projection,bindings,registers,1,node,predicate,constPredicate,(order!=Database::Order_Predicate_Subject_Object)&&(order!=Database::Order_Predicate_Object_Subject));
   resolveScanVariable(runtime,context,projection,bindings,registers,2,node,object,constObject,(order!=Database::Order_Object_Subject_Predicate)&&(order!=Database::Order_Object_Predicate_Subject));

   // And return the operator
   if (runtime.hasDifferentialIndex())
      return runtime.getDifferentialIndex().createFullyAggregatedScan(static_cast<Database::DataOrder>(plan->opArg),subject,constSubject,predicate,constPredicate,object,constObject,static_cast<unsigned>(plan->cardinality));
   return FullyAggregatedIndexScan::create(runtime.getDatabase(),order,
                                           subject,constSubject,
                                           predicate,constPredicate,
                                           object,constObject,
					   static_cast<unsigned>(plan->cardinality));
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
static Operator* addAdditionalSelections(Runtime& runtime,Operator* input,const set<unsigned>& joinVariables,map<unsigned,Register*>& leftBindings,map<unsigned,Register*>& rightBindings,unsigned joinedOn)
   // Convert additional join predicates into a selection
{
   // Examine join conditions
   vector<Register*> left,right;
   for (set<unsigned>::const_iterator iter=joinVariables.begin(),limit=joinVariables.end();iter!=limit;++iter) {
      if ((*iter)!=joinedOn) {
         left.push_back(leftBindings[*iter]);
         right.push_back(rightBindings[*iter]);
      }
   }

   // Build the results
   if (!left.empty()) {
      Selection::Predicate* predicate=0;
      for (unsigned index=0;index<left.size();index++) {
         Selection::Predicate* p=new Selection::Equal(new Selection::Variable(left[index]),new Selection::Variable(right[index]));
         if (predicate)
            predicate=new Selection::And(predicate,p); else
            predicate=p;
      }
      return new Selection(input,runtime,predicate,input->getExpectedOutputCardinality());
   } else  {
      return input;
   }
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
   Operator* result=new NestedLoopJoin(leftTree,rightTree,static_cast<unsigned>(plan->cardinality));

   // And apply additional selections if necessary
   result=addAdditionalSelections(runtime,result,joinVariables,leftBindings,rightBindings,~0u);

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
   Operator* result=new MergeJoin(leftTree,leftBindings[joinOn],leftTail,rightTree,rightBindings[joinOn],rightTail,static_cast<unsigned>(plan->cardinality));

   // And apply additional selections if necessary
   result=addAdditionalSelections(runtime,result,joinVariables,leftBindings,rightBindings,joinOn);

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
   Operator* result=new HashJoin(leftTree,leftBindings[joinOn],leftTail,rightTree,rightBindings[joinOn],rightTail,-plan->left->costs,plan->right->costs,static_cast<unsigned>(plan->cardinality));

   // And apply additional selections if necessary
   result=addAdditionalSelections(runtime,result,joinVariables,leftBindings,rightBindings,joinOn);

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
   return new HashGroupify(tree,output,static_cast<unsigned>(plan->cardinality));
}
//---------------------------------------------------------------------------
static void collectVariables(set<unsigned>& filterVariables,const QueryGraph::Filter& filter)
   // Collect all query variables
{
   if (filter.type==QueryGraph::Filter::Variable)
      filterVariables.insert(filter.id);
   if (filter.arg1)
      collectVariables(filterVariables,*filter.arg1);
   if (filter.arg2)
      collectVariables(filterVariables,*filter.arg2);
   if (filter.arg3)
      collectVariables(filterVariables,*filter.arg3);
}
//---------------------------------------------------------------------------
static Selection::Predicate* buildSelection(const map<unsigned,Register*>& bindings,const QueryGraph::Filter& filter);
//---------------------------------------------------------------------------
static void collectSelectionArgs(const map<unsigned,Register*>& bindings,vector<Selection::Predicate*>& args,const QueryGraph::Filter* input)
   // Collect all function arguments
{
   for (const QueryGraph::Filter* iter=input;iter;iter=iter->arg2) {
      assert(iter->type==QueryGraph::Filter::ArgumentList);
      args.push_back(buildSelection(bindings,*(iter->arg1)));
   }
}
//---------------------------------------------------------------------------
static Selection::Predicate* buildSelection(const map<unsigned,Register*>& bindings,const QueryGraph::Filter& filter)
   // Construct a complex filter predicate
{
   switch (filter.type) {
      case QueryGraph::Filter::Or: return new Selection::Or(buildSelection(bindings,*filter.arg1),buildSelection(bindings,*filter.arg2));
      case QueryGraph::Filter::And: return new Selection::And(buildSelection(bindings,*filter.arg1),buildSelection(bindings,*filter.arg2));
      case QueryGraph::Filter::Equal: return new Selection::Equal(buildSelection(bindings,*filter.arg1),buildSelection(bindings,*filter.arg2));
      case QueryGraph::Filter::NotEqual: return new Selection::And(buildSelection(bindings,*filter.arg1),buildSelection(bindings,*filter.arg2));
      case QueryGraph::Filter::Less: return new Selection::Less(buildSelection(bindings,*filter.arg1),buildSelection(bindings,*filter.arg2));
      case QueryGraph::Filter::LessOrEqual: return new Selection::LessOrEqual(buildSelection(bindings,*filter.arg1),buildSelection(bindings,*filter.arg2));
      case QueryGraph::Filter::Greater: return new Selection::Less(buildSelection(bindings,*filter.arg2),buildSelection(bindings,*filter.arg1));
      case QueryGraph::Filter::GreaterOrEqual: return new Selection::LessOrEqual(buildSelection(bindings,*filter.arg2),buildSelection(bindings,*filter.arg1));
      case QueryGraph::Filter::Plus: return new Selection::Plus(buildSelection(bindings,*filter.arg1),buildSelection(bindings,*filter.arg2));
      case QueryGraph::Filter::Minus: return new Selection::Minus(buildSelection(bindings,*filter.arg1),buildSelection(bindings,*filter.arg2));
      case QueryGraph::Filter::Mul: return new Selection::Mul(buildSelection(bindings,*filter.arg1),buildSelection(bindings,*filter.arg2));
      case QueryGraph::Filter::Div: return new Selection::Div(buildSelection(bindings,*filter.arg1),buildSelection(bindings,*filter.arg2));
      case QueryGraph::Filter::Not: return new Selection::Not(buildSelection(bindings,*filter.arg1));
      case QueryGraph::Filter::UnaryPlus: return buildSelection(bindings,*filter.arg1);
      case QueryGraph::Filter::UnaryMinus: return new Selection::Neg(buildSelection(bindings,*filter.arg1));
      case QueryGraph::Filter::Literal:
         if (~filter.id)
            return new Selection::ConstantLiteral(filter.id); else
            return new Selection::TemporaryConstantLiteral(filter.value);
      case QueryGraph::Filter::Variable:
         if (bindings.count(filter.id))
            return new Selection::Variable((*bindings.find(filter.id)).second); else
            return new Selection::Null();
      case QueryGraph::Filter::IRI:
         if (~filter.id)
            return new Selection::ConstantIRI(filter.id); else
            return new Selection::TemporaryConstantIRI(filter.value);
      case QueryGraph::Filter::Null: return new Selection::Null();
      case QueryGraph::Filter::Function: {
         assert(filter.arg1->type==QueryGraph::Filter::IRI);
         vector<Selection::Predicate*> args;
         collectSelectionArgs(bindings,args,filter.arg2);
         return new Selection::FunctionCall(filter.arg1->value,args); }
      case QueryGraph::Filter::ArgumentList: assert(false); // cannot happen
      case QueryGraph::Filter::Builtin_str: return new Selection::BuiltinStr(buildSelection(bindings,*filter.arg1));
      case QueryGraph::Filter::Builtin_lang: return new Selection::BuiltinLang(buildSelection(bindings,*filter.arg1));
      case QueryGraph::Filter::Builtin_langmatches: return new Selection::BuiltinLangMatches(buildSelection(bindings,*filter.arg1),buildSelection(bindings,*filter.arg2));
      case QueryGraph::Filter::Builtin_datatype: return new Selection::BuiltinDatatype(buildSelection(bindings,*filter.arg1));
      case QueryGraph::Filter::Builtin_bound:
         if (~filter.id)
            return new Selection::BuiltinBound((*bindings.find(filter.id)).second); else
            return new Selection::False();
      case QueryGraph::Filter::Builtin_sameterm: return new Selection::BuiltinSameTerm(buildSelection(bindings,*filter.arg1),buildSelection(bindings,*filter.arg2));
      case QueryGraph::Filter::Builtin_isiri: return new Selection::BuiltinIsIRI(buildSelection(bindings,*filter.arg1));
      case QueryGraph::Filter::Builtin_isblank: return new Selection::BuiltinIsBlank(buildSelection(bindings,*filter.arg1));
      case QueryGraph::Filter::Builtin_isliteral: return new Selection::BuiltinIsLiteral(buildSelection(bindings,*filter.arg1));
      case QueryGraph::Filter::Builtin_regex: return new Selection::BuiltinRegEx(buildSelection(bindings,*filter.arg1),buildSelection(bindings,*filter.arg2),buildSelection(bindings,*filter.arg3));
      case QueryGraph::Filter::Builtin_in: {
         vector<Selection::Predicate*> args;
         collectSelectionArgs(bindings,args,filter.arg2);
         return new Selection::BuiltinIn(buildSelection(bindings,*filter.arg1),args); }
   }
   throw; // Cannot happen
}
//---------------------------------------------------------------------------
static Operator* translateFilter(Runtime& runtime,const map<unsigned,Register*>& context,const set<unsigned>& projection,map<unsigned,Register*>& bindings,const map<const QueryGraph::Node*,unsigned>& registers,Plan* plan)
   // Translate a filter into an operator tree
{
   const QueryGraph::Filter& filter=*reinterpret_cast<QueryGraph::Filter*>(plan->right);

   // Collect all variables
   set<unsigned> filterVariables;
   collectVariables(filterVariables,filter);

   // Build the input trees
   set<unsigned> newProjection=projection;
   for (set<unsigned>::const_iterator iter=filterVariables.begin(),limit=filterVariables.end();iter!=limit;++iter)
      newProjection.insert(*iter);
   Operator* tree=translatePlan(runtime,context,newProjection,bindings,registers,plan->left);

   // Build the operator, try special cases first
   Operator* result=0;
   if (((filter.type==QueryGraph::Filter::Equal)||(filter.type==QueryGraph::Filter::NotEqual))&&(filter.arg1->type==QueryGraph::Filter::Variable)) {
      if (((filter.arg2->type==QueryGraph::Filter::Literal)||(filter.arg2->type==QueryGraph::Filter::IRI))&&(bindings.count(filter.arg1->id))) {
         vector<unsigned> values;
         values.push_back(filter.arg2->id);
         result=new Filter(tree,bindings[filter.arg1->id],values,filter.type==QueryGraph::Filter::NotEqual,static_cast<unsigned>(plan->cardinality));
      }
   }
   if ((!result)&&((filter.type==QueryGraph::Filter::Equal)||(filter.type==QueryGraph::Filter::NotEqual))&&(filter.arg2->type==QueryGraph::Filter::Variable)) {
      if (((filter.arg1->type==QueryGraph::Filter::Literal)||(filter.arg1->type==QueryGraph::Filter::IRI))&&(bindings.count(filter.arg2->id))) {
         vector<unsigned> values;
         values.push_back(filter.arg1->id);
         result=new Filter(tree,bindings[filter.arg2->id],values,filter.type==QueryGraph::Filter::NotEqual,static_cast<unsigned>(plan->cardinality));
      }
   }
   if ((!result)&&(filter.type==QueryGraph::Filter::Builtin_in)&&(filter.arg1->type==QueryGraph::Filter::Variable)&&(bindings.count(filter.arg1->id))) {
      vector<unsigned> values;
      bool valid=true;
      if (filter.arg2) {
         const QueryGraph::Filter* iter=filter.arg2;
         for (;iter;iter=iter->arg2) {
            assert(iter->type==QueryGraph::Filter::ArgumentList);
            if ((iter->arg1->type!=QueryGraph::Filter::Literal)&&(iter->arg1->type!=QueryGraph::Filter::IRI)) {
               valid=false;
               break;
            }
            values.push_back(iter->arg1->id);
         }
      }
      if (valid) {
         result=new Filter(tree,bindings[filter.arg1->id],values,false,static_cast<unsigned>(plan->cardinality));
      }
   }
   if (!result) {
      result=new Selection(tree,runtime,buildSelection(bindings,filter),static_cast<unsigned>(plan->cardinality));
   }

   // Cleanup the binding
   for (set<unsigned>::const_iterator iter=filterVariables.begin(),limit=filterVariables.end();iter!=limit;++iter)
      if (!projection.count(*iter))
         bindings.erase(*iter);

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
   Operator* result=new Union(trees,mappings,initializations,static_cast<unsigned>(plan->cardinality));

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
   Operator* result=new MergeUnion(leftReg,left,leftReg,right,rightReg,static_cast<unsigned>(plan->cardinality));

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
      case Plan::Union: result=translateUnion(runtime,context,projection,bindings,registers,plan); break;
      case Plan::MergeUnion: result=translateMergeUnion(runtime,context,projection,bindings,registers,plan); break;
   }
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
