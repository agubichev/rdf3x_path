#include "rts/operator/Selection.hpp"
#include "rts/runtime/Runtime.hpp"
#include <iostream>
#include <cassert>
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
void Selection::print(unsigned level)
   // Print the operator tree. Debugging only.
{
   indent(level); std::cout << "<Selection";
   for (std::vector<Register*>::const_iterator iter=predicates.begin(),limit=predicates.end();iter!=limit;++iter) {
      std::cout << " ";
      printRegister(*iter);
      if (equal)
         std::cout << "="; else
         std::cout << "!=";
      ++iter;
      printRegister(*iter);
   }
   std::cout << std::endl;
   input->print(level+1);
   indent(level); std::cout << ">" << std::endl;
}
//---------------------------------------------------------------------------
