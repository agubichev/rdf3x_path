#include "cts/codegen/CodeGen.hpp"
#include "cts/infra/QueryGraph.hpp"
#include "rts/operator/AggregatedIndexScan.hpp"
#include "rts/operator/IndexScan.hpp"
#include "rts/operator/NestedLoopJoin.hpp"
#include "rts/operator/ResultsPrinter.hpp"
#include "rts/operator/SingletonScan.hpp"
#include "rts/runtime/Runtime.hpp"
#include <map>
#include <set>
//---------------------------------------------------------------------------
static Operator* buildIndexScan(Runtime& runtime,const QueryGraph::Node& node,std::set<unsigned>& boundVariables,std::map<unsigned,Register*>& constantRegisters,std::map<unsigned,Register*>& variableRegisters,std::map<unsigned,unsigned>& variableUses)
   // Construct an index scan
{
   // Examine which variables are bound
   bool boundSubject,boundPredicate,boundObject;
   boundSubject=(node.constSubject||boundVariables.count(node.subject));
   boundPredicate=(node.constPredicate||boundVariables.count(node.predicate));
   boundObject=(node.constObject||boundVariables.count(node.object));

   // Lookup the registers and mark them as bound
   Register* subject,*predicate,*object;
   if (node.constSubject) {
      subject=constantRegisters[node.subject];
   } else {
      subject=variableRegisters[node.subject];
      boundVariables.insert(node.subject);
   }
   if (node.constPredicate) {
      predicate=constantRegisters[node.predicate];
   } else {
      predicate=variableRegisters[node.predicate];
      boundVariables.insert(node.predicate);
   }
   if (node.constObject) {
      object=constantRegisters[node.object];
   } else {
      object=variableRegisters[node.object];
      boundVariables.insert(node.object);
   }

   // Pick a suitable data order
   Database::DataOrder order;
   bool aggregated=false;
   if (boundSubject) {
      if (boundPredicate) {
         order=Database::Order_Subject_Predicate_Object;
         if ((!node.constObject)&&(variableUses[node.object]==1)) {
            object=0; aggregated=true;
         }
      } else if (boundObject) {
         order=Database::Order_Subject_Object_Predicate;
         if ((!node.constPredicate)&&(variableUses[node.predicate]==1)) {
            predicate=0; aggregated=true;
         }
      } else {
         if ((!node.constPredicate)&&(variableUses[node.predicate]==1)) {
            order=Database::Order_Subject_Object_Predicate;
            predicate=0; aggregated=true;
         } else {
            order=Database::Order_Subject_Predicate_Object;
            if ((!node.constObject)&&(variableUses[node.object]==1)) {
               object=0; aggregated=true;
            }
         }
      }
   } else if (boundPredicate) {
      if (boundObject) {
         order=Database::Order_Predicate_Object_Subject;
         if ((!node.constSubject)&&(variableUses[node.subject]==1)) {
            subject=0; aggregated=true;
         }
      } else {
         if ((!node.constSubject)&&(variableUses[node.subject]==1)) {
            order=Database::Order_Predicate_Object_Subject;
            subject=0; aggregated=true;
         } else {
            order=Database::Order_Predicate_Subject_Object;
            if ((!node.constObject)&&(variableUses[node.object]==1)) {
               object=0; aggregated=true;
            }
         }
      }
   } else if (boundObject) {
      if ((!node.constSubject)&&(variableUses[node.subject]==1)) {
         order=Database::Order_Object_Predicate_Subject;
         subject=0; aggregated=true;
      } else {
         order=Database::Order_Object_Subject_Predicate;
         if ((!node.constPredicate)&&(variableUses[node.predicate]==1)) {
            predicate=0; aggregated=true;
         }
      }
   } else order=Database::Order_Subject_Predicate_Object;

   // And return the scan
   if (aggregated)
      return new AggregatedIndexScan(runtime.getDatabase(),order,subject,boundSubject,predicate,boundPredicate,object,boundObject);
   return new IndexScan(runtime.getDatabase(),order,subject,boundSubject,predicate,boundPredicate,object,boundObject);
}
//---------------------------------------------------------------------------
Operator* CodeGen::translate(Runtime& runtime,const QueryGraph& query,bool silent)
   // Perform a naive translation of a query into an operator tree
{
   // Collect the required number of registers
   std::map<unsigned,Register*> constantRegisters;
   std::map<unsigned,Register*> variableRegisters;
   std::map<unsigned,unsigned> variableUses;
   for (QueryGraph::projection_iterator iter=query.projectionBegin(),limit=query.projectionEnd();iter!=limit;++iter)
      variableRegisters[*iter];
   for (QueryGraph::node_iterator iter=query.nodesBegin(),limit=query.nodesEnd();iter!=limit;++iter) {
      const QueryGraph::Node& n=(*iter);
      if (n.constSubject) constantRegisters[n.subject]; else { variableRegisters[n.subject]; variableUses[n.subject]++; }
      if (n.constPredicate) constantRegisters[n.predicate]; else { variableRegisters[n.predicate]; variableUses[n.predicate]++; }
      if (n.constObject) constantRegisters[n.object]; else { variableRegisters[n.object]; variableUses[n.object]++; }
   }

   // Allocate the registers
   runtime.allocateRegisters(constantRegisters.size()+variableRegisters.size());
   { Register* nextReg=runtime.getRegister(0);
   for (std::map<unsigned,Register*>::iterator iter=constantRegisters.begin(),limit=constantRegisters.end();iter!=limit;++iter) {
      (*iter).second=nextReg++;
      (*iter).second->value=(*iter).first;
   }
   for (std::map<unsigned,Register*>::iterator iter=variableRegisters.begin(),limit=variableRegisters.end();iter!=limit;++iter) {
      (*iter).second=nextReg++;
   } }

   // Build the basic tree
   Operator* tree;
   if (query.nodesBegin()==query.nodesEnd()) {
      tree=new SingletonScan();
   } else {
      // Start with the first pattern
      std::set<unsigned> boundVariables;
      std::set<const QueryGraph::Node*> done,reachable;
      reachable.insert(&(*query.nodesBegin()));

      // Expand
      unsigned nodesCount=query.getNodeCount();
      tree=0;
      while (done.size()<nodesCount) {
         // Examine the next node
         const QueryGraph::Node* node;
         if (reachable.empty()) {
            // No reachable node found, take the next one
            node=0;
            for (QueryGraph::node_iterator iter=query.nodesBegin(),limit=query.nodesEnd();iter!=limit;++iter)
               if (!done.count(&(*iter))) {
                  node=&(*iter);
                  break;
               }
         } else {
            node=*reachable.begin();
            reachable.erase(node);
            if (done.count(node))
               continue;
         }
         done.insert(node);

         // Build the index scan
         Operator* scan=buildIndexScan(runtime,*node,boundVariables,constantRegisters,variableRegisters,variableUses);

         // And enlarge the tree
         if (!tree)
            tree=scan; else
            tree=new NestedLoopJoin(tree,scan);
      }
   }

   // And add the output generation
   std::vector<Register*> output;
   for (QueryGraph::projection_iterator iter=query.projectionBegin(),limit=query.projectionEnd();iter!=limit;++iter)
      output.push_back(variableRegisters[*iter]);
   tree=new ResultsPrinter(runtime.getDatabase(),tree,output,silent);

   return tree;
}
//---------------------------------------------------------------------------
