#include "rts/operator/EmptyScan.hpp"
#include <iostream>
//---------------------------------------------------------------------------
EmptyScan::EmptyScan()
   // Constructor
{
}
//---------------------------------------------------------------------------
EmptyScan::~EmptyScan()
   // Destructor
{
}
//---------------------------------------------------------------------------
unsigned EmptyScan::first()
   // Produce the first tuple
{
   return false;
}
//---------------------------------------------------------------------------
unsigned EmptyScan::next()
   // Produce the next tuple
{
   return false;
}
//---------------------------------------------------------------------------
void EmptyScan::print(unsigned level)
   // Print the operator tree. Debugging only.
{
   indent(level); std::cout << "<EmptyScan>" << std::endl;
}
//---------------------------------------------------------------------------
