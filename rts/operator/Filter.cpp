#include "rts/operator/Filter.hpp"
#include "rts/runtime/Runtime.hpp"
#include <algorithm>
#include <iostream>
//---------------------------------------------------------------------------
Filter::Filter(Operator* input,Register* filter,const std::vector<unsigned>& values)
   : input(input),filter(filter),values(values)
   // Constructor
{
   std::sort(this->values.begin(),this->values.end());
}
//---------------------------------------------------------------------------
Filter::~Filter()
   // Destructor
{
}
//---------------------------------------------------------------------------
unsigned Filter::first()
   // Produce the first tuple
{
   // Empty input?
   unsigned count;
   if ((count=input->first())==0)
      return false;

   // Check if valid
   unsigned value;
   lastValue=value=filter->value;
   std::vector<unsigned>::const_iterator iter=values.begin(),limit=values.end();
   iter=lower_bound(iter,limit,value);
   lastResult=((iter!=limit)&&((*iter)==value));

   // Return if found, otherwise call next
   if (lastResult)
      return count; else
      return next();
}
//---------------------------------------------------------------------------
unsigned Filter::next()
   // Produce the next tuple
{
   while (true) {
      // Done?
      unsigned count;
      if ((count=input->next())==0)
         return false;

      // Check if valid
      unsigned value=filter->value;
      if (value!=lastValue) {
         lastValue=value;
         std::vector<unsigned>::const_iterator iter=values.begin(),limit=values.end();
         iter=lower_bound(iter,limit,value);
         lastResult=((iter!=limit)&&((*iter)==value));
      }
      if (lastResult)
         return count;
   }
}
//---------------------------------------------------------------------------
void Filter::print(unsigned level)
   // Print the operator tree. Debugging only.
{
   indent(level); std::cout << "<Filter ";
   printRegister(filter);
   std::cout << " [";
   for (std::vector<unsigned>::const_iterator iter=values.begin(),limit=values.end();iter!=limit;++iter) {
      std::cout << " " << (*iter);
   }
   std::cout << "]" << std::endl;
   input->print(level+1);
   indent(level); std::cout << ">" << std::endl;
}
//---------------------------------------------------------------------------
