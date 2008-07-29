#include "rts/operator/Operator.hpp"
#include "rts/runtime/Runtime.hpp"
#include "rts/segment/DictionarySegment.hpp"
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
Operator::Operator()
   // Constructor
{
}
//---------------------------------------------------------------------------
Operator::~Operator()
   // Destructor
{
}
//---------------------------------------------------------------------------
void Operator::indent(unsigned level)
   // Helper for indenting debug output
{
   for (unsigned index=0;index<level;index++)
      std::cout << ' ';
}
//---------------------------------------------------------------------------
void Operator::printRegister(DictionarySegment& dict,const Register* reg)
   // Helper for debug output
{
   // A constant?
   if (~(reg->value)) {
      const char* start,*stop;
      if (dict.lookupById(reg->value,start,stop)) {
         std::cout << '\"';
         for (const char* iter=start;iter!=stop;++iter)
           std::cout << *iter;
         std::cout << '\"';
      } else std::cout << reg->value;
   } else {
      std::cout << "@" << static_cast<const void*>(reg);
   }
}
//---------------------------------------------------------------------------
