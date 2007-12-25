#include "rts/operator/Selection.hpp"
#include "rts/runtime/Runtime.hpp"
#include <iostream>
#include <cassert>
//---------------------------------------------------------------------------
Selection::Selection(Operator* input,const std::vector<Register*>& predicates)
   : predicates(predicates),input(input)
   // Constructor
{
   assert((predicates.size()%2)==0);
}
//---------------------------------------------------------------------------
Selection::~Selection()
   // Destructor
{
}
//---------------------------------------------------------------------------
unsigned Selection::first()
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
unsigned Selection::next()
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
void Selection::print(unsigned level)
   // Print the operator tree. Debugging only.
{
   indent(level); std::cout << "<Selection";
   for (std::vector<Register*>::const_iterator iter=predicates.begin(),limit=predicates.end();iter!=limit;++iter) {
      std::cout << " ";
      printRegister(*iter);
      std::cout << "=";
      ++iter;
      printRegister(*iter);
   }
   std::cout << std::endl;
   input->print(level+1);
   indent(level); std::cout << ">" << std::endl;
}
//---------------------------------------------------------------------------
