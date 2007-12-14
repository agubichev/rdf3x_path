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
   std::map<unsigned,std::string> constants;
   std::vector<std::vector<unsigned> > tuples;
   do {
      std::vector<unsigned> tuple;
      for (std::vector<Register*>::const_iterator iter=output.begin(),limit=output.end();iter!=limit;++iter) {
         tuple.push_back((*iter)->value);
         if (~((*iter)->value))
            constants[(*iter)->value];
      }
      tuples.push_back(tuple);
   } while (input->next());

   // Resolve all constants
   for (std::map<unsigned,std::string>::iterator iter=constants.begin(),limit=constants.end();iter!=limit;++iter)
      dictionary.lookupById((*iter).first,(*iter).second);

   // And print the tuples
   if (!silent) {
      for (std::vector<std::vector<unsigned> >::const_iterator iter=tuples.begin(),limit=tuples.end();iter!=limit;++iter) {
         bool first=true;
         for (std::vector<unsigned>::const_iterator iter2=(*iter).begin(),limit2=(*iter).end();iter2!=limit2;++iter2) {
            if (first) first=false; else std::cout << ' ';
            if (~(*iter2))
               std::cout << constants[*iter2];
         }
         std::cout << std::endl;
      }
   }

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
