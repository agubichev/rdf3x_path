#include "rts/operator/NestedLoopFilter.hpp"
#include "rts/runtime/Runtime.hpp"
#include <algorithm>
#include <iostream>
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
NestedLoopFilter::NestedLoopFilter(Operator* input,Register* filter,const std::vector<unsigned>& values)
   : input(input),filter(filter),values(values)
   // Constructor
{
   std::sort(this->values.begin(),this->values.end());
}
//---------------------------------------------------------------------------
NestedLoopFilter::~NestedLoopFilter()
   // Destructor
{
}
//---------------------------------------------------------------------------
unsigned NestedLoopFilter::first()
   // Produce the first tuple
{
   for (pos=0;pos<values.size();++pos) {
      filter->value=values[pos];
      unsigned count;
      if ((count=input->first())!=0)
         return count;
   }
   return false;
}
//---------------------------------------------------------------------------
unsigned NestedLoopFilter::next()
   // Produce the next tuple
{
   // Done?
   if (pos>=values.size())
      return false;

   // More tuples?
   unsigned count;
   if ((count=input->next())!=0)
      return count;

   // No, go to the next value
   for (++pos;pos<values.size();++pos) {
      filter->value=values[pos];
      unsigned count;
      if ((count=input->first())!=0)
         return count;
   }
   return false;
}
//---------------------------------------------------------------------------
void NestedLoopFilter::print(DictionarySegment& dict,unsigned level)
   // Print the operator tree. Debugging only.
{
   indent(level); std::cout << "<NestedLoopFilter ";
   printRegister(dict,filter);
   std::cout << " [";
   for (std::vector<unsigned>::const_iterator iter=values.begin(),limit=values.end();iter!=limit;++iter) {
      std::cout << " " << (*iter);
   }
   std::cout << "]" << std::endl;
   input->print(dict,level+1);
   indent(level); std::cout << ">" << std::endl;
}
//---------------------------------------------------------------------------
void NestedLoopFilter::addMergeHint(Register* /*reg1*/,Register* /*reg2*/)
   // Add a merge join hint
{
   // Do not propagate as we break the pipeline
}
//---------------------------------------------------------------------------
void NestedLoopFilter::getAsyncInputCandidates(Scheduler& scheduler)
   // Register parts of the tree that can be executed asynchronous
{
   input->getAsyncInputCandidates(scheduler);
}
//---------------------------------------------------------------------------
