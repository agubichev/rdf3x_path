#include "rts/operator/NestedLoopJoin.hpp"
#include <iostream>
//---------------------------------------------------------------------------
NestedLoopJoin::NestedLoopJoin(Operator* left,Operator* right)
   : left(left),right(right)
   // Constructor
{
}
//---------------------------------------------------------------------------
NestedLoopJoin::~NestedLoopJoin()
   // Destructor
{
   delete left;
   delete right;
}
//---------------------------------------------------------------------------
bool NestedLoopJoin::first()
   // Produce the first tuple
{
   // Read the first tuple on the left side
   if (!left->first())
      return false;

   // Look for tuples on the right side
   while (!right->first()) {
      if (!left->next())
         return false;
   }

   return true;
}
//---------------------------------------------------------------------------
bool NestedLoopJoin::next()
   // Produce the next tuple
{
   // A simple match?
   if (right->next())
      return true;

   // No, do we have more tuples on the left hand side?
   if (!left->next())
      return false;

   // Yes, look for tuples on the right side
   while (!right->first()) {
      if (!left->next())
         return false;
   }

   return true;
}
//---------------------------------------------------------------------------
void NestedLoopJoin::print(unsigned level)
   // Print the operator tree. Debugging only.
{
   indent(level); std::cout << "<NestedLoopJoin" << std::endl;
   left->print(level+1);
   right->print(level+1);
   indent(level); std::cout << ">" << std::endl;
}
//---------------------------------------------------------------------------
