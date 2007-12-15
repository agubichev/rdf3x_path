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
bool ResultsPrinter::first()
   // Produce the first tuple
{
   // Empty input?
   if (!input->first()) {
      if (!silent)
         std::cout << "<empty result>" << std::endl;
      return true;
   }

   // Collect all tuples and constants
   do {
      bool first=true;
      for (std::vector<Register*>::const_iterator iter=output.begin(),limit=output.end();iter!=limit;++iter) {
         if (first) first=false; else if (!silent) std::cout << ' ';
         if (~((*iter)->value)) {
            const char* start,*stop;
            dictionary.lookupById((*iter)->value,start,stop);
            if (!silent)
               std::cout << std::string(start,stop);
         } else {
            if (!silent)
               std::cout << "NULL";
         }
      }
      if (!silent)
         std::cout << std::endl;
   } while (input->next());

   return true;
}
//---------------------------------------------------------------------------
bool ResultsPrinter::next()
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
