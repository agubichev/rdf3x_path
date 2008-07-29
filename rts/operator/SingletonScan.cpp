#include "rts/operator/SingletonScan.hpp"
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
unsigned SingletonScan::first()
   // Produce the first tuple
{
   return 1;
}
//---------------------------------------------------------------------------
unsigned SingletonScan::next()
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
