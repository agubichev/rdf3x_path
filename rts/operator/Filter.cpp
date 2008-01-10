#include "rts/operator/Filter.hpp"
#include "rts/runtime/Runtime.hpp"
#include <iostream>
//---------------------------------------------------------------------------
Filter::Filter(Operator* input,Register* filter,const std::vector<unsigned>& values)
   : input(input),filter(filter)
   // Constructor
{
   if (values.empty()) {
      min=1;
      max=0;
   } else {
      min=max=values[0];
      for (std::vector<unsigned>::const_iterator iter=values.begin(),limit=values.end();iter!=limit;++iter) {
         if ((*iter)<min)
            min=*iter;
         if ((*iter)>max)
            max=*iter;
      }
      valid.resize(max-min+1);
      for (std::vector<unsigned>::const_iterator iter=values.begin(),limit=values.end();iter!=limit;++iter)
         valid[(*iter)-min]=true;
   }
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
   unsigned value=filter->value;
   if ((value>=min)&&(value<=max)&&(valid[value-min]))
      return count;

   // Call next to find other entries
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
      if ((value>=min)&&(value<=max)&&(valid[value-min]))
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
   unsigned id=min;
   for (std::vector<unsigned char>::const_iterator iter=valid.begin(),limit=valid.end();iter!=limit;++iter,++id) {
      if (*iter)
         std::cout << " " << id;
   }
   std::cout << "]" << std::endl;
   input->print(level+1);
   indent(level); std::cout << ">" << std::endl;
}
//---------------------------------------------------------------------------
