#include "rts/operator/Filter.hpp"
#include "rts/runtime/Runtime.hpp"
#include <iostream>
//---------------------------------------------------------------------------
Filter::Filter(Operator* input,Register* filter,const std::vector<unsigned>& values,bool exclude)
   : input(input),filter(filter),exclude(exclude)
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
   if (exclude) {
      if ((value>=min)&&(value<=max)&&(valid[value-min]))
         return next();
      return count;
   } else {
      if ((value>=min)&&(value<=max)&&(valid[value-min]))
         return count;
      return next();
   }
}
//---------------------------------------------------------------------------
unsigned Filter::next()
   // Produce the next tuple
{
   if (exclude) {
      while (true) {
         // Done?
         unsigned count;
         if ((count=input->next())==0)
            return false;

         // Check if valid
         unsigned value=filter->value;
         if ((value>=min)&&(value<=max)&&(valid[value-min]))
            continue;
         return count;
      }
   } else {
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
}
//---------------------------------------------------------------------------
void Filter::print(unsigned level)
   // Print the operator tree. Debugging only.
{
   indent(level); std::cout << "<Filter ";
   printRegister(filter);
   if (exclude) std::cout << " !";
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