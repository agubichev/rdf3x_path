#include "rts/operator/Selection.hpp"
#include "rts/runtime/Runtime.hpp"
#include <iostream>
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
/// Test for equal
class Selection::Equal : public Selection
{
   public:
   /// Constructor
   Equal(Operator* input,const std::vector<Register*>& predicates) : Selection(input,predicates,true) {}

   /// First tuple
   unsigned first();
   /// Next tuples
   unsigned next();
};
//---------------------------------------------------------------------------
/// Test for not equal
class Selection::NotEqual : public Selection
{
   public:
   /// Constructor
   NotEqual(Operator* input,const std::vector<Register*>& predicates) : Selection(input,predicates,false) {}

   /// First tuple
   unsigned first();
   /// Next tuples
   unsigned next();
};
//---------------------------------------------------------------------------
Selection::Selection(Operator* input,const std::vector<Register*>& predicates,bool equal)
   : predicates(predicates),input(input),equal(equal)
   // Constructor
{
   assert((predicates.size()%2)==0);
}
//---------------------------------------------------------------------------
Selection* Selection::create(Operator* input,const std::vector<Register*>& predicates,bool equal)
   // Constructor
{
   if (equal)
      return new Equal(input,predicates); else
      return new NotEqual(input,predicates);
}
//---------------------------------------------------------------------------
Selection::~Selection()
   // Destructor
{
   delete input;
}
//---------------------------------------------------------------------------
unsigned Selection::Equal::first()
   // Produce the first tuple
{
   // Empty input?
   unsigned count;
   if ((count=input->first())==0)
      return false;

   // Check the predicate
   bool match=true;
   for (std::vector<Register*>::const_iterator iter=predicates.begin(),limit=predicates.end();iter!=limit;++iter) {
      unsigned v1=(*iter)->value;
      if (v1!=(*(++iter))->value) {
         match=false;
         break;
      }
   }
   if (match)
      return count;

   return next();
}
//---------------------------------------------------------------------------
unsigned Selection::Equal::next()
   // Produce the next tuple
{
   while (true) {
      // Input exhausted?
      unsigned count;
      if ((count=input->next())==0)
         return false;

      // Check the predicate
      bool match=true;
      for (std::vector<Register*>::const_iterator iter=predicates.begin(),limit=predicates.end();iter!=limit;++iter) {
         unsigned v1=(*iter)->value;
         if (v1!=(*(++iter))->value) {
            match=false;
            break;
         }
      }
      if (match)
         return count;
   }
}
//---------------------------------------------------------------------------
unsigned Selection::NotEqual::first()
   // Produce the first tuple
{
   // Empty input?
   unsigned count;
   if ((count=input->first())==0)
      return false;

   // Check the predicate
   bool match=true;
   for (std::vector<Register*>::const_iterator iter=predicates.begin(),limit=predicates.end();iter!=limit;++iter) {
      unsigned v1=(*iter)->value;
      if (v1==(*(++iter))->value) {
         match=false;
         break;
      }
   }
   if (match)
      return count;

   return next();
}
//---------------------------------------------------------------------------
unsigned Selection::NotEqual::next()
   // Produce the next tuple
{
   while (true) {
      // Input exhausted?
      unsigned count;
      if ((count=input->next())==0)
         return false;

      // Check the predicate
      bool match=true;
      for (std::vector<Register*>::const_iterator iter=predicates.begin(),limit=predicates.end();iter!=limit;++iter) {
         unsigned v1=(*iter)->value;
         if (v1==(*(++iter))->value) {
            match=false;
            break;
         }
      }
      if (match)
         return count;
   }
}
//---------------------------------------------------------------------------
void Selection::print(DictionarySegment& dict,unsigned level)
   // Print the operator tree. Debugging only.
{
   indent(level); std::cout << "<Selection";
   for (std::vector<Register*>::const_iterator iter=predicates.begin(),limit=predicates.end();iter!=limit;++iter) {
      std::cout << " ";
      printRegister(dict,*iter);
      if (equal)
         std::cout << "="; else
         std::cout << "!=";
      ++iter;
      printRegister(dict,*iter);
   }
   std::cout << std::endl;
   input->print(dict,level+1);
   indent(level); std::cout << ">" << std::endl;
}
//---------------------------------------------------------------------------
void Selection::addMergeHint(Register* reg1,Register* reg2)
   // Add a merge join hint
{
   input->addMergeHint(reg1,reg2);
}
//---------------------------------------------------------------------------
void Selection::getAsyncInputCandidates(Scheduler& scheduler)
   // Register parts of the tree that can be executed asynchronous
{
   input->getAsyncInputCandidates(scheduler);
}
//---------------------------------------------------------------------------
