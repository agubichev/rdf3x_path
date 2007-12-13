#include "rts/operator/Operator.hpp"
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
