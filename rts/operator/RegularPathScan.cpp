#include "rts/operator/RegularPathScan.hpp"
#include "rts/operator/PlanPrinter.hpp"
#include "rts/runtime/Runtime.hpp"
#include "rts/segment/FactsSegment.hpp"
#include "cts/infra/QueryGraph.hpp"
#include "rts/segment/DictionarySegment.hpp"
#include "rts/ferrari/Graph.h"
#include <iostream>
#include <vector>
#include <map>
#include <set>
#include <algorithm>
#include <stdlib.h>
using namespace std;
//---------------------------------------------------------------------------
// RDF-3X
// (c) 2013 Andrey Gubichev
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
//---------------------------------------------------------------------------

void RegularPathScan::print(PlanPrinter& out)
   // Print the operator tree. Debugging only.
{
   out.beginOperator("RegularPathScan",expectedOutputCardinality,observedOutputCardinality);
   out.addScanAnnotation(value1,const1);
   out.addScanAnnotation(value3,const3);
   out.addScanAnnotation(firstSource,const1);
   out.addScanAnnotation(secondSource,const3);
   if (op1) op1->print(out);
   if (op2) op2->print(out);
   out.endOperator();
}
//---------------------------------------------------------------------------
RegularPathScan::RegularPathScan(Database& db,Database::DataOrder order,Register* value1,bool const1,Register* value3,bool const3,double expectedOutputCardinality,Modifier pathmode,unsigned predicate,bool inverse,Index* ferrari)
   : Operator(expectedOutputCardinality),value1(value1),value3(value3),const1(const1),const3(const3),pathmode(pathmode),predicate(predicate),order(order),dict(db.getDictionary()),ferrari(ferrari),op1(0),op2(0),
     firstSource(0),secondSource(0),inverse(inverse),entryPool(0),rightCount(0)
   // Constructor
{
	bound1=false;
	bound3=false;
}
//---------------------------------------------------------------------------
RegularPathScan::~RegularPathScan()
   // Destructor
{
	if (op1)
		delete op1;
	if (op2)
		delete op2;
	if (entryPool)
		delete entryPool;
}
//---------------------------------------------------------------------------
void RegularPathScan::buildStorage(){
	storage.reserve(1024);

   // Prepare relevant domain information
   vector<Register*> domainRegs;
   if (secondSource->domain){
      domainRegs.push_back(secondSource);
   }
   ObservedDomainDescription observedDomain;

   for (unsigned leftCount=op1->first();leftCount;leftCount=op1->next()) {
   	vector<unsigned> reachable;
   	ferrari->get_reachable(firstSource->value,reachable);
   	for (auto node: reachable)
   		observedDomain.add(node);

      Entry* e=entryPool->alloc();
      e->key=firstSource->value;
      for (unsigned i=0;i<firstBinding.size();i++){
      	e->values[i]=firstBinding[i]->value;
      }
      e->count=leftCount;
      storage.push_back(e);
   }
   // Update the domains
   secondSource->domain->restrictTo(observedDomain);
}
//---------------------------------------------------------------------------
unsigned RegularPathScan::first()
{
	buildStorage();

	if ((rightCount=op2->first())==0){
		return 0;
	}

	storageIterator=storage.begin();
	return next();
}
//---------------------------------------------------------------------------
unsigned RegularPathScan::next()
{
   /*	if (op1){
		while (op1->next()&&op2->next()){
			this->value3->value=firstSource->value;
			this->value1->value=secondSource->value;
			return 1;
		}
	}
	*/
	while (true){
		for (;storageIterator!=storage.end();++storageIterator){
			unsigned leftCount=(*storageIterator)->count;
			firstSource->value=(*storageIterator)->key;
			// IF ! reachable continue
			this->value1->value=firstSource->value;
			this->value3->value=secondSource->value;

			if (!inverse){
//				cerr<<value1->value<<" "<<value3->value<<endl;
				if (!ferrari->reachable(value1->value,value3->value))
					continue;
			} else {
				//cerr<<value3->value<<" "<<value1->value<<endl;
				if (!ferrari->reachable(value3->value,value1->value))
					continue;
			}

         for (unsigned index=0,limit=firstBinding.size();index<limit;++index)
         	firstBinding[index]->value=(*storageIterator)->values[index];
         ++storageIterator;
         return rightCount*leftCount;
		}

		if ((rightCount=op2->next())==0)
			return 0;
		storageIterator=storage.begin();
	}
	return 0;
}
//---------------------------------------------------------------------------
class RegularPathScan::RPConstant: public RegularPathScan {
private:
	/// is the second variable in the triple pattern bounded?
	/// (bounded = defined in other triple patterns)
	bool bounded;
public:
	RPConstant(Database& db,Database::DataOrder order,Register* value1,bool const1,Register* value3,bool const3,double expectedOutputCardinality,Modifier pathmod,unsigned predicate,bool inverse,Index* ferrari): RegularPathScan(db,order,value1,const1,value3,const3,expectedOutputCardinality,pathmod,predicate,inverse,ferrari){bounded=false;};
	unsigned first();
	unsigned next();

	void setBounded(bool b){bounded=b;}
};
//---------------------------------------------------------------------------
unsigned RegularPathScan::RPConstant::first(){
	// ASSUME (assert) value1 is constant
	if (op1){
		if (op1->first()){
			// bounded scan: check that value1 reaches nodes from value3
			this->value3->value=firstSource->value;
			if (!inverse&&ferrari->reachable(value1->value,value3->value)){
				return 1;
			}
			if (inverse&&ferrari->reachable(value3->value,value1->value)){
				//cerr<<value1->value<<" "<<value3->value<<endl;
				return 1;
			}
		}
	} else{
		// unbounded scan for all nodes reachable from value1
	}
	return next();
}
//---------------------------------------------------------------------------
unsigned RegularPathScan::RPConstant::next(){
	// ASSUME (assert?) value1 is constant
	if (op1){
		// bounded scan: check that value1 reaches nodes from value3
		while (op1->next()){
			this->value3->value=firstSource->value;
			//cerr<<value1->value<<" "<<value3->value<<endl;
			//assert(ferrari->get_graph()!=0);
			//Graph* g=ferrari->get_graph();
			//cerr<<"bound: "<<const1<<" "<<const3<<endl;

			//cerr<<g->getNodeId(value1->value)<<" "<<g->getNodeId(value3->value)<<endl;
			if (!inverse&&ferrari->reachable(value1->value,value3->value))
				return 1;
			if (inverse&&ferrari->reachable(value3->value,value1->value))
				return 1;
		}
	} else{
		// unbounded scan for all nodes reachable from value1
		// TODO: check reachability here
	}
	return 0;
}
//---------------------------------------------------------------------------
RegularPathScan* RegularPathScan::create(Database& db,Database::DataOrder order,Register* subject,bool subjectBound,Register* object,bool objectBound,double expectedOutputCardinality,Modifier pathmode,unsigned predicate,Index* ferrari)
   // Constructor
{
   // Setup the slot bindings
   Register* value1=0,*value3=0;
   bool const1=false,const3=false;
   bool reverse=false;
   switch (order) {
      case Database::Order_Subject_Predicate_Object:
         value1=subject; value3=object;
         const1=subjectBound; const3=objectBound;
         break;
      case Database::Order_Object_Predicate_Subject:
    	   // reachability on reversed edges
         value1=object;  value3=subject;
         const1=objectBound;  const3=subjectBound;
         reverse=true;
         break;
      case Database::Order_Subject_Object_Predicate:
      case Database::Order_Object_Subject_Predicate:
      case Database::Order_Predicate_Subject_Object:
      case Database::Order_Predicate_Object_Subject:
    	  return 0; //never happens
   }
   // Construct the appropriate operator
   RegularPathScan* result=0;

   if (const1||const3){
   	result=new RPConstant(db,order,value1,const1,value3,const3,expectedOutputCardinality,pathmode,predicate,reverse,ferrari);
   }
   else{
   	result = new RegularPathScan(db,order,value1,const1,value3,const3,expectedOutputCardinality,pathmode,predicate,reverse,ferrari);
   }
   return result;
}
//---------------------------------------------------------------------------
void RegularPathScan::setFirstInput(Operator* op){
	this->op1=op;
	if (const1) bound3=true;
	else bound1=true;
}
//---------------------------------------------------------------------------
void RegularPathScan::setSecondInput(Operator* op){
	this->op2=op;
	if (!bound1) bound1=true;
	else bound3=true;
//	cerr<<"expected card "<<op1->getExpectedOutputCardinality()<<" "<<op2->getExpectedOutputCardinality()<<endl;
}
//---------------------------------------------------------------------------
bool RegularPathScan::isFirstInputSet(){
	return this->op1!=0;
}
//---------------------------------------------------------------------------
bool RegularPathScan::isSecondInputSet(){
	return this->op2!=0;
}
//---------------------------------------------------------------------------
void RegularPathScan::setFirstBinding(std::vector<Register*>& firstBinding){
	this->firstBinding=firstBinding;
	this->entryPool=new VarPool<RegularPathScan::Entry>(firstBinding.size()*sizeof(unsigned));
}
//---------------------------------------------------------------------------
void RegularPathScan::setSecondBinding(std::vector<Register*>& secondBinding){
	this->secondBinding=secondBinding;
}
//---------------------------------------------------------------------------
void RegularPathScan::setFirstSource(Register* r){
	this->firstSource=r;
}
//---------------------------------------------------------------------------
void RegularPathScan::setSecondSource(Register* r){
	this->secondSource=r;
}
//---------------------------------------------------------------------------
void RegularPathScan::checkAndSwap(){
	if (op1->getExpectedOutputCardinality()>op2->getExpectedOutputCardinality()){
		swap(firstSource,secondSource);
		swap(firstBinding,secondBinding);
		swap(op1,op2);
		inverse=!inverse;
		if (entryPool){
			/// Do it only once
			delete entryPool;
			entryPool=new VarPool<RegularPathScan::Entry>(firstBinding.size()*sizeof(unsigned));
		}
	}
}
