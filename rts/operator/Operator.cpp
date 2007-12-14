#include "rts/operator/Operator.hpp"
#include "rts/runtime/Runtime.hpp"
#include <iostream>
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
void Operator::printRegister(const Register* reg)
   // Helper for debug output
{
   // A constant?
   if (~(reg->value))
      std::cout << reg->value; else
      std::cout << "@" << static_cast<const void*>(reg);
}
//---------------------------------------------------------------------------
