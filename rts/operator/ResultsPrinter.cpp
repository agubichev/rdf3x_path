#include "rts/operator/ResultsPrinter.hpp"
#include "rts/database/Database.hpp"
#include "rts/runtime/Runtime.hpp"
#include "rts/segment/DictionarySegment.hpp"
#include <iostream>
#include <map>
//---------------------------------------------------------------------------
ResultsPrinter::ResultsPrinter(Database& db,Operator* input,const std::vector<Register*>& output,bool silent)
   : output(output),input(input),dictionary(db.getDictionary()),silent(silent)
   // Constructor
{
}
//---------------------------------------------------------------------------
ResultsPrinter::~ResultsPrinter()
   // Destructor
{
}
//---------------------------------------------------------------------------
unsigned ResultsPrinter::first()
   // Produce the first tuple
{
   // Empty input?
   unsigned count;
   if ((count=input->first())==0) {
      if (!silent)
         std::cout << "<empty result>" << std::endl;
      return true;
   }

   // Prepare the constants cache
   static const unsigned cacheSize = 65536;
   unsigned constantCache[cacheSize];
   const char* cacheStart[cacheSize],*cacheStop[cacheSize];
   for (unsigned index=0;index<cacheSize;index++)
      constantCache[index]=~0u;

   // Collect all tuples and constants
   do {
      bool first=true;
      for (std::vector<Register*>::const_iterator iter=output.begin(),limit=output.end();iter!=limit;++iter) {
         if (first) first=false; else if (!silent) std::cout << ' ';
         if (~((*iter)->value)) {
            unsigned value=(*iter)->value,slot=value%cacheSize;
            if (constantCache[slot]!=value) {
               constantCache[slot]=value;
               dictionary.lookupById(value,cacheStart[slot],cacheStop[slot]);
            }
            if (!silent)
               std::cout << std::string(cacheStart[slot],cacheStop[slot]);
         } else {
            if (!silent)
               std::cout << "NULL";
         }
      }
      if (!silent) {
         if (count!=1)
            std::cout << " x" << count;
         std::cout << std::endl;
      }
   } while ((count=input->next())!=0);

   return 1;
}
//---------------------------------------------------------------------------
unsigned ResultsPrinter::next()
   // Produce the next tuple
{
   return false;
}
//---------------------------------------------------------------------------
void ResultsPrinter::print(unsigned level)
   // Print the operator tree. Debugging only.
{
   indent(level); std::cout << "<ResultsPrinter";
   for (std::vector<Register*>::const_iterator iter=output.begin(),limit=output.end();iter!=limit;++iter) {
      std::cout << " "; printRegister(*iter);
   }
   std::cout << std::endl;
   input->print(level+1);
   indent(level); std::cout << ">" << std::endl;
}
//---------------------------------------------------------------------------
