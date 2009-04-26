#include "rts/operator/TupleCounter.hpp"
#include <iostream>
//---------------------------------------------------------------------------
// RDF-3X
// (c) 2009 Thomas Neumann. Web site: http://www.mpi-inf.mpg.de/~neumann/rdf3x
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
//---------------------------------------------------------------------------
unsigned TupleCounter::totalEstimated = 0;
unsigned TupleCounter::totalObserved = 0;
//---------------------------------------------------------------------------
TupleCounter::TupleCounter(Operator* input,unsigned estimated)
   : Operator(estimated),input(input),estimated(estimated),observed(0)
   // Constructor
{
}
//---------------------------------------------------------------------------
TupleCounter::~TupleCounter()
   // Destructor
{
   delete input;
}
//---------------------------------------------------------------------------
unsigned TupleCounter::first()
   // Produce the first tuple
{
   observedOutputCardinality=0;
   unsigned result=input->first();
   observed=result;
   observedOutputCardinality+=result;
   return result;
}
//---------------------------------------------------------------------------
unsigned TupleCounter::next()
   // Produce the next tuple
{
   unsigned result=input->next();
   observed+=result;
   observedOutputCardinality+=result;
   return result;
}
//---------------------------------------------------------------------------
void TupleCounter::print(PlanPrinter& out)
   // Print the operator tree. Debugging only.
{
   totalEstimated+=estimated;
   totalObserved+=observed;

   std::cout << "# estimated cardinality: " << estimated << " observed cardinality: " << observed << std::endl;
   input->print(out);
}
//---------------------------------------------------------------------------
void TupleCounter::addMergeHint(Register* reg1,Register* reg2)
   // Add a merge join hint
{
   input->addMergeHint(reg1,reg2);
}
//---------------------------------------------------------------------------
void TupleCounter::getAsyncInputCandidates(Scheduler& scheduler)
   // Register parts of the tree that can be executed asynchronous
{
   input->getAsyncInputCandidates(scheduler);
}
//---------------------------------------------------------------------------
