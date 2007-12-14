#include "rts/operator/SingletonScan.hpp"
#include <iostream>
//---------------------------------------------------------------------------
SingletonScan::SingletonScan()
   // Constructor
{
}
//---------------------------------------------------------------------------
SingletonScan::~SingletonScan()
   // Destructor
{
}
//---------------------------------------------------------------------------
bool SingletonScan::first()
   // Produce the first tuple
{
   return true;
}
//---------------------------------------------------------------------------
bool SingletonScan::next()
   // Produce the next tuple
{
   return false;
}
//---------------------------------------------------------------------------
void SingletonScan::print(unsigned level)
   // Print the operator tree. Debugging only.
{
   indent(level); std::cout << "<SingletonScan>" << std::endl;
}
//---------------------------------------------------------------------------
