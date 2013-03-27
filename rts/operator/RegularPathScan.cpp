#include "rts/operator/RegularPathScan.hpp"
#include "rts/operator/PlanPrinter.hpp"
#include "rts/runtime/Runtime.hpp"
#include "rts/segment/FactsSegment.hpp"
#include "cts/infra/QueryGraph.hpp"
#include "rts/segment/DictionarySegment.hpp"
#include <iostream>
#include <vector>
#include <map>
#include <set>
#include <algorithm>
#include <stdlib.h>
using namespace std;
//---------------------------------------------------------------------------
// RDF-3X
// (c) 2013 Andrey  Gubichev, Thomas Neumann.
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
   out.addScanAnnotation(value1,bound1);
   out.addScanAnnotation(value3,bound3);
   if (left) left->print(out);
   if (right) right->print(out);
   out.endOperator();
}
//---------------------------------------------------------------------------
RegularPathScan::RegularPathScan(Database& db,Database::DataOrder order,Register* value1,bool bound1,Register* value3,bool bound3,double expectedOutputCardinality,Modifier pathmode,unsigned predicate)
   : Operator(expectedOutputCardinality),value1(value1),value3(value3),bound1(bound1),bound3(bound3),pathmode(pathmode),predicate(predicate),order(order),dict(db.getDictionary()),left(0),right(0)
   // Constructor
{
}
//---------------------------------------------------------------------------
RegularPathScan::~RegularPathScan()
   // Destructor
{
	if (left)
		delete left;
	if (right)
		delete right;
}
//---------------------------------------------------------------------------
unsigned RegularPathScan::first()
{
	cerr<<"first"<<endl;
	if (left&&left->first()){
		cerr<<"bound1 bound3 "<<bound1<<" "<<bound3<<endl;
		cerr<<value1->value<<" "<<value3->value<<endl;
		for (auto t:leftBinding)
			cerr<<t.first<<" "<<t.second->value<<endl;
	}
	return next();
}
//---------------------------------------------------------------------------
unsigned RegularPathScan::next()
{
	if (left){
		while (left->next()){
			for (auto t:leftBinding)
				cerr<<t.first<<" "<<t.second->value<<endl;
			cerr<<value3->value<<endl;
			return 1;
		}
	}

	return 0;
}
//---------------------------------------------------------------------------
RegularPathScan* RegularPathScan::create(Database& db,Database::DataOrder order,Register* subject,bool subjectBound,Register* object,bool objectBound,double expectedOutputCardinality,Modifier pathmode,unsigned predicate)
   // Constructor
{
   // Setup the slot bindings
   Register* value1=0,*value3=0;
   bool bound1=false,bound3=false;
   switch (order) {
      case Database::Order_Subject_Predicate_Object:
         value1=subject; value3=object;
         bound1=subjectBound; bound3=objectBound;
         break;
      case Database::Order_Object_Predicate_Subject:
    	   // reachability on reversed edges
         value1=object;  value3=subject;
         bound1=objectBound;  bound3=subjectBound;
         break;
      case Database::Order_Subject_Object_Predicate:
      case Database::Order_Object_Subject_Predicate:
      case Database::Order_Predicate_Subject_Object:
      case Database::Order_Predicate_Object_Subject:
    	  return 0; //never happens
   }
   // Construct the appropriate operator
   RegularPathScan* result = new RegularPathScan(db,order,value1,bound1,value3,bound3,expectedOutputCardinality,pathmode,predicate);
   return result;
}

void RegularPathScan::setLeftInput(Operator* left){
	this->left=left;
}

void RegularPathScan::setRightInput(Operator* right){
	this->right=right;
}

void RegularPathScan::setLeftBinding(std::map<unsigned,Register*>& leftBinding){
	this->leftBinding=leftBinding;
}


