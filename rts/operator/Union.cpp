#include "rts/operator/Union.hpp"
#include "rts/runtime/Runtime.hpp"
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
Union::Union(const std::vector<Operator*>& parts,const std::vector<std::vector<Register*> >& mappings,const std::vector<std::vector<Register*> >& initializations,unsigned expectedOutputCardinality)
   : Operator(expectedOutputCardinality),parts(parts),mappings(mappings),initializations(initializations)
   // Constructor
{
}
//---------------------------------------------------------------------------
Union::~Union()
   // Destructor
{
   for (std::vector<Operator*>::iterator iter=parts.begin(),limit=parts.end();iter!=limit;++iter)
      delete *iter;
}
//---------------------------------------------------------------------------
unsigned Union::firstFromPart()
   // Get the first tuple from the current part
{
   while (true) {
      unsigned count;
      if ((count=parts[current]->first())==0) {
         if ((++current)>=parts.size())
            return false;
         continue;
      }
      for (std::vector<Register*>::const_iterator iter=initializations[current].begin(),limit=initializations[current].end();iter!=limit;++iter)
         (*iter)->value=~0u;
      for (std::vector<Register*>::const_iterator iter=mappings[current].begin(),limit=mappings[current].end();iter!=limit;) {
         Register* from=*iter; ++iter;
         Register* to=*iter; ++iter;
         to->value=from->value;
      }
      observedOutputCardinality+=count;
      return count;
   }
}
//---------------------------------------------------------------------------
unsigned Union::first()
   // Produce the first tuple
{
   observedOutputCardinality=0;
   current=0;
   if (parts.empty())
      return false;
   return firstFromPart();
}
//---------------------------------------------------------------------------
unsigned Union::next()
   // Produce the next tuple
{
   // End of input?
   if (current>=parts.size())
      return false;

   // Can we get a tuple?
   unsigned count;
   if ((count=parts[current]->next())==0) {
      if ((++current)>=parts.size())
         return false;
      return firstFromPart();
   }

   // Yes, perform mapping
   for (std::vector<Register*>::const_iterator iter=mappings[current].begin(),limit=mappings[current].end();iter!=limit;) {
      Register* from=*iter; ++iter;
      Register* to=*iter; ++iter;
      to->value=from->value;
   }
   observedOutputCardinality+=count;
   return count;
}
//---------------------------------------------------------------------------
void Union::print(DictionarySegment& dict,unsigned level)
   // Print the operator tree. Debugging only.
{
   indent(level); std::cout << "<Union" << std::endl;
   for (unsigned index=0;index<parts.size();index++) {
      indent(level);
      std::cout << "[";
      for (unsigned index2=0;index2<mappings[index].size();index2+=2) {
         if (index2) std::cout << " ";
         printRegister(dict,mappings[index][index2]);
         std::cout << "->";
         printRegister(dict,mappings[index][index2+1]);
      }
      std::cout << "] [";
      for (unsigned index2=0;index2<initializations[index].size();index2++) {
         if (index2) std::cout << " ";
         printRegister(dict,initializations[index][index2]);
      }
      std::cout << "]" << std::endl;
      parts[index]->print(dict,level+1);
   }
   indent(level); std::cout << ">" << std::endl;
}
//---------------------------------------------------------------------------
void Union::addMergeHint(Register* /*reg1*/,Register* /*reg2*/)
   // Add a merge join hint
{
   // Do not propagate as we break the pipeline
}
//---------------------------------------------------------------------------
void Union::getAsyncInputCandidates(Scheduler& scheduler)
   // Register parts of the tree that can be executed asynchronous
{
   for (unsigned index=0;index<parts.size();index++)
      parts[index]->getAsyncInputCandidates(scheduler);
}
//---------------------------------------------------------------------------
