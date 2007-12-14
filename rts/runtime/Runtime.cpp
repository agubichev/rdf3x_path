#include "rts/runtime/Runtime.hpp"
//---------------------------------------------------------------------------
Runtime::Runtime(Database& db)
   : db(db)
   // Constructor
{
}
//---------------------------------------------------------------------------
Runtime::~Runtime()
   // Destructor
{
}
//---------------------------------------------------------------------------
void Runtime::allocateRegisters(unsigned count)
   // Set the number of registers
{
   registers.clear();
   registers.resize(count);

   for (unsigned index=0;index<count;index++)
      registers[index].value=~0u;
}
//---------------------------------------------------------------------------
