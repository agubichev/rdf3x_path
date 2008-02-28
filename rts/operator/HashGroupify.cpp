#include "rts/operator/HashGroupify.hpp"
#include "rts/runtime/Runtime.hpp"
#include <iostream>
//---------------------------------------------------------------------------
/// Helper
class HashGroupify::Rehasher {
   private:
   /// The hash table
   std::vector<Group*>& hashTable;

   public:
   /// Constructor
   Rehasher(std::vector<Group*>& hashTable) : hashTable(hashTable) {}

   /// Rehash
   void operator()(Group* g) {
      Group*& slot=hashTable[g->hash&(hashTable.size()-1)];
      g->next=slot;
      slot=g;
   }
};
//---------------------------------------------------------------------------
/// Helper
class HashGroupify::Chainer {
   private:
   /// The chain
   Group* head,*tail;

   public:
   /// Constructor
   Chainer() : head(0),tail(0) {}

   /// Get the head
   Group* getHead() const { return head; }

   /// Rehash
   void operator()(Group* g) {
      if (tail)
         tail->next=g; else
         head=g;
      g->next=0;
      tail=g;
   }
};
//---------------------------------------------------------------------------
HashGroupify::HashGroupify(Operator* input,const std::vector<Register*>& values)
   : values(values),input(input),groups(0),groupsPool(values.size()*sizeof(unsigned))
   // Constructor
{
}
//---------------------------------------------------------------------------
HashGroupify::~HashGroupify()
   // Destructor
{
   delete input;
}
//---------------------------------------------------------------------------
unsigned HashGroupify::first()
   // Produce the first tuple
{
   // Aggregate the input
   std::vector<Group*> hashTable;
   unsigned hashTableSize=64,load=0,maxLoad=static_cast<unsigned>(0.8*hashTableSize);
   hashTable.resize(hashTableSize);
   groupsPool.freeAll();

   for (unsigned count=input->first();count;count=input->next()) {
      // Hash the aggregation values
      unsigned hash=0;
      for (std::vector<Register*>::const_iterator iter=values.begin(),limit=values.end();iter!=limit;++iter)
         hash=((hash<<15)|(hash>>(8*sizeof(unsigned)-15)))^((*iter)->value);

      // Scan the hash table for existing values
      Group*& slot=hashTable[hash&(hashTableSize-1)];
      bool match=false;
      for (Group* iter=slot;iter;iter=iter->next) {
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
      Group* g=groupsPool.alloc();
      g->next=slot;
      g->hash=hash;
      g->count=count;
      for (unsigned index=0,limit=values.size();index<limit;index++)
         g->values[index]=values[index]->value;
      slot=g;

      // Rehash if necessary
      if ((++load)>=maxLoad) {
         hashTable.clear();
         hashTableSize*=2;
         maxLoad=static_cast<unsigned>(0.8*hashTableSize);
         hashTable.resize(hashTableSize);
         Rehasher rehasher(hashTable);
         groupsPool.enumAll(rehasher);
      }
   }

   // Form a chain out of the groups
   Chainer chainer;
   groupsPool.enumAll(chainer);

   groups=chainer.getHead();
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
