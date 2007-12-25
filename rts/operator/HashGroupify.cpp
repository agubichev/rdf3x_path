#include "rts/operator/HashGroupify.hpp"
#include "rts/runtime/Runtime.hpp"
#include <iostream>
//---------------------------------------------------------------------------
/// A group
struct HashGroupify::Group {
   /// The next group
   Group* next;
   /// The values
   std::vector<unsigned> values;
   /// The count
   unsigned count;
};
//---------------------------------------------------------------------------
HashGroupify::HashGroupify(Operator* input,const std::vector<Register*>& values)
   : values(values),input(input),groups(0)
   // Constructor
{
}
//---------------------------------------------------------------------------
HashGroupify::~HashGroupify()
   // Destructor
{
   delete input;
   deleteGroups();
}
//---------------------------------------------------------------------------
void HashGroupify::deleteGroups()
   // Delete the groups
{
   while (groups) {
      Group* next=groups;
      delete groups;
      groups=next;
   }
}
//---------------------------------------------------------------------------
unsigned HashGroupify::first()
   // Produce the first tuple
{
   // Aggregate the input
   const unsigned hashTableSize=997;
   Group* hashTable[hashTableSize];
   for (unsigned index=0;index<hashTableSize;index++)
      hashTable[index]=0;
   for (unsigned count=input->first();count;count=input->next()) {
      // Hash the aggregation values
      unsigned hash=0;
      for (std::vector<Register*>::const_iterator iter=values.begin(),limit=values.end();iter!=limit;++iter)
         hash=((hash<<15)|(hash>>(8*sizeof(unsigned)-15)))^((*iter)->value);

      // Scan the hash table for existing values
      bool match=false;
      for (Group* iter=hashTable[hash%hashTableSize];iter;iter=iter->next) {
         match=true;
         for (unsigned index=0,limit=values.size();index<limit;index++)
            if (iter->values[index]!=values[index]->value)
               { match=false; break; }
         if (match) {
            iter->count+=count;
            break;
         }
      }
      if (match) continue;

      // Create a new group
      Group* g=new Group();
      g->next=hashTable[hash%hashTableSize];
      hashTable[hash%hashTableSize]=g;
      g->values.resize(values.size());
      for (unsigned index=0,limit=values.size();index<limit;index++)
         g->values[index]=values[index]->value;
      g->count=count;
   }

   // Form a chain out of the groups
   deleteGroups();
   Group* tail=0;
   for (unsigned index=0;index<hashTableSize;index++)
      if (hashTable[index]) {
         if (tail)
            tail->next=hashTable[index]; else
            groups=tail=hashTable[index];
         while (tail->next)
            tail=tail->next;
      }
   groupsIter=groups;

   return next();
}
//---------------------------------------------------------------------------
unsigned HashGroupify::next()
   // Produce the next tuple
{
   // End of input?
   if (!groupsIter)
      return 0;

   // Produce the next group
   for (unsigned index=0,limit=values.size();index<limit;index++)
      values[index]->value=groupsIter->values[index];
   unsigned count=groupsIter->count;
   groupsIter=groupsIter->next;

   return count;
}
//---------------------------------------------------------------------------
void HashGroupify::print(unsigned level)
   // Print the operator tree. Debugging only.
{
   indent(level); std::cout << "<HashGroupify" << std::endl;
   for (std::vector<Register*>::const_iterator iter=values.begin(),limit=values.end();iter!=limit;++iter) {
      std::cout << " "; printRegister(*iter);
   }
   std::cout << std::endl;
   input->print(level+1);
   indent(level); std::cout << ">" << std::endl;
}
//---------------------------------------------------------------------------
