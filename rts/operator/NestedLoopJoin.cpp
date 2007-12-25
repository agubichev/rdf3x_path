#include "rts/operator/NestedLoopJoin.hpp"
#include "rts/runtime/Runtime.hpp"
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
unsigned NestedLoopJoin::first()
   // Produce the first tuple
{
   // Read the first tuple on the left side
   if ((leftCount=left->first())==0)
      return false;

   // Look for tuples on the right side
   unsigned rightCount;
   while ((rightCount=right->first())==0) {
      if ((leftCount=left->next())==0)
         return false;
   }

   return leftCount*rightCount;
}
//---------------------------------------------------------------------------
unsigned NestedLoopJoin::next()
   // Produce the next tuple
{
   // A simple match?
   unsigned rightCount;
   if ((rightCount=right->next())!=0)
      return leftCount*rightCount;

   // No, do we have more tuples on the left hand side?
   if ((leftCount=left->next())==0)
      return false;

   // Yes, look for tuples on the right side
   while ((rightCount=right->first())==0) {
      if ((leftCount=left->next())==0)
         return false;
   }

   return leftCount*rightCount;
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
