#include "rts/operator/Union.hpp"
#include "rts/runtime/Runtime.hpp"
#include <iostream>
//---------------------------------------------------------------------------
Union::Union(const std::vector<Operator*>& parts,const std::vector<std::vector<Register*> >& mappings,const std::vector<std::vector<Register*> >& initializations)
   : parts(parts),mappings(mappings),initializations(initializations)
   // Constructor
{
}
//---------------------------------------------------------------------------
Union::~Union()
   // Destructor
{
}
//---------------------------------------------------------------------------
unsigned Union::firstFromPart()
   // Get the first tuple from the current part
{
   while (true) {
      unsigned count;
      if ((count=parts[current]->first())==0) {
         if ((++current)>=parts.size())
            return false;
         continue;
      }
      for (std::vector<Register*>::const_iterator iter=initializations[current].begin(),limit=initializations[current].end();iter!=limit;++iter)
         (*iter)->value=~0u;
      for (std::vector<Register*>::const_iterator iter=mappings[current].begin(),limit=mappings[current].end();iter!=limit;) {
         Register* from=*iter; ++iter;
         Register* to=*iter; ++iter;
         to->value=from->value;
      }
      return count;
   }
}
//---------------------------------------------------------------------------
unsigned Union::first()
   // Produce the first tuple
{
   current=0;
   if (parts.empty())
      return false;
   return firstFromPart();
}
//---------------------------------------------------------------------------
unsigned Union::next()
   // Produce the next tuple
{
   // End of input?
   if (current>=parts.size())
      return false;

   // Can we get a tuple?
   unsigned count;
   if ((count=parts[current]->next())==0) {
      if ((++current)>=parts.size())
         return false;
      return firstFromPart();
   }

   // Yes, perform mapping
   for (std::vector<Register*>::const_iterator iter=mappings[current].begin(),limit=mappings[current].end();iter!=limit;) {
      Register* from=*iter; ++iter;
      Register* to=*iter; ++iter;
      to->value=from->value;
   }
   return count;
}
//---------------------------------------------------------------------------
void Union::print(unsigned level)
   // Print the operator tree. Debugging only.
{
   indent(level); std::cout << "<Union" << std::endl;
   for (unsigned index=0;index<parts.size();index++) {
      indent(level);
      std::cout << "[";
      for (unsigned index2=0;index2<mappings[index].size();index2+=2) {
         if (index2) std::cout << " ";
         printRegister(mappings[index][index2]);
         std::cout << "->";
         printRegister(mappings[index][index2+1]);
      }
      std::cout << "] [";
      for (unsigned index2=0;index2<initializations[index].size();index2++) {
         if (index2) std::cout << " ";
         printRegister(initializations[index][index2]);
      }
      std::cout << "]" << std::endl;
      parts[index]->print(level+1);
   }
   indent(level); std::cout << ">" << std::endl;
}
//---------------------------------------------------------------------------
