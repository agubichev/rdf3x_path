#include "rts/operator/DescribeScan.hpp"
#include "rts/operator/PlanPrinter.hpp"
#include "rts/runtime/Runtime.hpp"
#include "rts/segment/FactsSegment.hpp"
#include "cts/infra/QueryGraph.hpp"
#include "rts/segment/DictionarySegment.hpp"
#include "rts/database/Database.hpp"
#include <iostream>
#include <vector>
#include <map>
#include <set>
#include <algorithm>
#include <stdlib.h>
using namespace std;
//---------------------------------------------------------------------------
// RDF-3X
// (c) 2011 Thomas Neumann, Andrey Gubichev. Web site: http://www.mpi-inf.mpg.de/~neumann/rdf3x
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
//---------------------------------------------------------------------------
void DescribeScan::print(PlanPrinter& out)
   // Print the operator tree. Debugging only.
{
   out.beginOperator("DescribeScan",expectedOutputCardinality,observedOutputCardinality);
   input->print(out);
   out.endOperator();
}
//---------------------------------------------------------------------------
DescribeScan::DescribeScan(Database& db,Operator* input,const CodeGen::Output& output,Register* value1,Register* value2,Register* value3,double expectedOutputCardinality)
   : Operator(expectedOutputCardinality),input(input),output(output),value1(value1),value2(value2),value3(value3),factsSPO(db.getFacts(Database::Order_Subject_Predicate_Object)),factsOPS(db.getFacts(Database::Order_Object_Predicate_Subject))
   // Constructor
{
}
//---------------------------------------------------------------------------
DescribeScan::~DescribeScan()
   // Destructor
{
	delete input;
}
//---------------------------------------------------------------------------
void DescribeScan::addMergeHint(Register* /*reg1*/,Register* /*reg2*/)
   // Add a merge join hint
{
}
//---------------------------------------------------------------------------
void DescribeScan::getAsyncInputCandidates(Scheduler& /*scheduler*/)
   // Register parts of the tree that can be executed asynchronous
{
}
//---------------------------------------------------------------------------
unsigned DescribeScan::first()
{
	observedOutputCardinality=0;

//	if (input->first())  {
//		cerr<<output.valueoutput[0]->value<<endl;
//		if (!scanSPO.first(factsSPO,output.valueoutput[0]->value,0,0)||(scanSPO.getValue1() != output.valueoutput[0]->value)){
//			// try with another order
//			if (!scanOPS.first(factsOPS,output.valueoutput[0]->value,0,0)||(scanOPS.getValue1() != output.valueoutput[0]->value)){
//				return 0;
//			} else {
//				value1->value=scanSPO.getValue1();
//				value2->value=scanSPO.getValue2();
//				value3->value=scanSPO.getValue3();
//				return 1;
//			}
//		} else {
//			cerr<<scanSPO.getValue1()<<" "<<scanSPO.getValue2()<<" "<<scanSPO.getValue3()<<endl;
//			value1->value=scanSPO.getValue1();
//			value2->value=scanSPO.getValue2();
//			value3->value=scanSPO.getValue3();
//			return 1;
//		}
//	}
	if (!input->first())
		return 0;

//	switchScan=false;
//	nextInput=false;
//	if (!scanSPO.first(factsSPO,output.valueoutput[0]->value,0,0)||(scanSPO.getValue1() != output.valueoutput[0]->value))
//		switchScan=true;
	scanState=checkSPO;

	return next();
}
//---------------------------------------------------------------------------
unsigned DescribeScan::next(){
	cerr<<"next "<<endl;
//	   while (input->next())
//		   cerr<<output.valueoutput[0]->value<<endl;
//	   return 0;
	   while (true) {
	      switch (scanState) {
	      case checkSPO:
	    	  cerr<<"start new scan with "<<output.valueoutput[0]->value<<endl;
	    	  if (!scanSPO.first(factsSPO,output.valueoutput[0]->value,0,0)||(scanSPO.getValue1() != output.valueoutput[0]->value)){
	    		  scanState=checkOPS;
	    		  continue;
	    	  } else
	    		  scanState=scanningSPO;
	    	  continue;
	      case checkOPS:
	    	  cerr<<"starting OPS new scan with "<<output.valueoutput[0]->value<<endl;
	    	  if (!scanOPS.first(factsOPS,output.valueoutput[0]->value,0,0)||(scanOPS.getValue1() != output.valueoutput[0]->value)){
	    		  scanState=needNewTuple;
	    		  continue;
	    	  } else
	    		  scanState=scanningOPS;
	    	  continue;
	      case needNewTuple:
	    	  cerr<<"checking new tuple"<<endl;
	    	  if (!input->next()){
	    		  cerr<<"no new tuple"<<endl;
	    		  return 0;
	    	  }
	    	  scanState=checkSPO;
	    	  continue;
	      case scanningSPO:
	          if (!scanSPO.next()||(scanSPO.getValue1() != output.valueoutput[0]->value)){
	        	  cerr<<"switch to OPS"<<endl;
	        	  scanState=checkOPS;
	   			  continue;
	    	  }
	          value1->value=scanSPO.getValue1();
	          value2->value=scanSPO.getValue2();
	          value3->value=scanSPO.getValue3();
	          observedOutputCardinality++;
	          cerr<<"SPO: "<<scanSPO.getValue1()<<" "<<scanSPO.getValue2()<<" "<<scanSPO.getValue3()<<endl;

	    	  return 1;
	      case scanningOPS:
	          if (!scanOPS.next()||(scanOPS.getValue1() != output.valueoutput[0]->value)){
	        	  scanState=needNewTuple;
	        	  continue;
	          }
	          cerr<<"OPS: "<<scanOPS.getValue1()<<" "<<scanOPS.getValue2()<<" "<<scanOPS.getValue3()<<endl;

	    	  value1->value=scanOPS.getValue3();
	    	  value2->value=scanOPS.getValue2();
	    	  value3->value=scanOPS.getValue1();
	    	  observedOutputCardinality++;
	    	  return 1;
	   }
	   }
	   return 0;
//	if (nextInput){
//		cerr<<"nextInput"<<endl;
//		if (!input->next())
//			return 0;
//		nextInput=false;
//		if (!scanSPO.first(factsSPO,output.valueoutput[0]->value,0,0)||(scanSPO.getValue1() != output.valueoutput[0]->value))
//			switchScan=true;
//	}
//
//	if (switchScan) {
//		cerr<<"switchScan"<<endl;
//		if (!scanOPS.first(factsOPS,output.valueoutput[0]->value,0,0)||(scanOPS.getValue1() != output.valueoutput[0]->value)){
//			// try with the next tuple
//			nextInput=true;
//			switchScan=false;
//			return next();
//		}
//	}

//	if (!scanSPO.first(factsSPO,output.valueoutput[0]->value,0,0)||(scanSPO.getValue1() != output.valueoutput[0]->value)){
//		// try with another order
//		cerr<<"try OPS"<<endl;
//		if (!scanOPS.first(factsOPS,output.valueoutput[0]->value,0,0)||(scanOPS.getValue1() != output.valueoutput[0]->value)){
//			// try with the next tuple
//			cerr<<"try next tuple"<<endl;
//			nextInput=true;
//			return next();
//		} else {
//			cerr<<"OPS: "<<scanOPS.getValue1()<<" "<<scanOPS.getValue2()<<" "<<scanOPS.getValue3()<<endl;
//			value1->value=scanOPS.getValue1();
//			value2->value=scanOPS.getValue2();
//			value3->value=scanOPS.getValue3();
//			return 1;
//		}
//	} else {
//		cerr<<"SPO: "<<scanSPO.getValue1()<<" "<<scanSPO.getValue2()<<" "<<scanSPO.getValue3()<<endl;
//		value1->value=scanSPO.getValue1();
//		value2->value=scanSPO.getValue2();
//		value3->value=scanSPO.getValue3();
//		return 1;
//	}

//	if (scanSPO.next()){
//		if (scanSPO.getValue1() != output.valueoutput[0]->value){
//			switchScan=true;
//			return next();
//		}
//		cerr<<"SPO: "<<scanSPO.getValue1()<<" "<<scanSPO.getValue2()<<" "<<scanSPO.getValue3()<<endl;
//		value1->value=scanSPO.getValue1();
//		value2->value=scanSPO.getValue2();
//		value3->value=scanSPO.getValue3();
//		observedOutputCardinality++;
//		return 1;
//	} else if (scanOPS.next()){
//		cerr<<"trying OPS"<<endl;
//
//		if (scanOPS.getValue1() != output.valueoutput[0]->value){
//			cerr<<"OPS does not have tuples"<<endl;
//			nextInput=true;
//			return next();
//		}
//    	cerr<<"OPS: "<<scanOPS.getValue1()<<" "<<scanOPS.getValue2()<<" "<<scanOPS.getValue3()<<endl;
//		value1->value=scanOPS.getValue1();
//		value2->value=scanOPS.getValue2();
//		value3->value=scanOPS.getValue3();
//		observedOutputCardinality++;
//		return 1;
//	} else {
//		cerr<<"getting to the next tuple"<<endl;
//		nextInput=true;
//		return next();
//	}

	return 0;
}
//---------------------------------------------------------------------------
