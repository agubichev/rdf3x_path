#include "rts/operator/HashJoin.hpp"
#include "rts/runtime/Runtime.hpp"
#include <iostream>
//---------------------------------------------------------------------------
/// Helper
class HashJoin::Rehasher {
   private:
   /// The hash table
   std::vector<Entry*>& hashTable;

   public:
   /// Constructor
   Rehasher(std::vector<Entry*>& hashTable) : hashTable(hashTable) {}

   /// Rehash
   void operator()(Entry* g) {
      Entry*& slot=hashTable[g->key&(hashTable.size()-1)];
      g->next=slot;
      slot=g;
   }
};
//---------------------------------------------------------------------------
HashJoin::HashJoin(Operator* left,Register* leftValue,const std::vector<Register*>& leftTail,Operator* right,Register* rightValue,const std::vector<Register*>& rightTail)
   : left(left),right(right),leftValue(leftValue),rightValue(rightValue),leftTail(leftTail),rightTail(rightTail),entryPool(leftTail.size()*sizeof(unsigned))
   // Constructor
{
}
//---------------------------------------------------------------------------
HashJoin::~HashJoin()
   // Destructor
{
}
//---------------------------------------------------------------------------
unsigned HashJoin::first()
   // Produce the first tuple
{
   // Build the hash table from the left side
   unsigned hashTableSize = 512,load=0,maxLoad=static_cast<unsigned>(0.8*hashTableSize);
   unsigned tailLength=leftTail.size();
   hashTable.clear();
   hashTable.resize(hashTableSize);
   for (unsigned leftCount=left->first();leftCount;leftCount=left->next()) {
      // Compute the slot
      unsigned leftKey=leftValue->value;
      unsigned slot=leftKey&(hashTableSize-1);

      // Scan if the entry already exists
      bool match=false;
      for (Entry* iter=hashTable[slot];iter;iter=iter->next)
         if (leftKey==iter->key) {
            // Tuple already in the table?
            match=true;
            for (unsigned index2=0;index2<tailLength;index2++)
               if (leftTail[index2]->value!=iter->values[index2]) {
                  match=false;
                  break;
               }
            // Then aggregate
            if (match) {
               iter->count+=leftCount;
               break;
            }
         }
      if (match)
         continue;

      // New tuple, append
      Entry* e=entryPool.alloc();
      e->next=hashTable[slot];
      hashTable[slot]=e;
      e->key=leftKey;
      e->count=leftCount;
      for (unsigned index2=0;index2<tailLength;index2++)
         e->values[index2]=leftTail[index2]->value;

      // Rehash?
      if ((++load)>=maxLoad) {
         hashTable.clear();
         hashTableSize*=2;
         maxLoad=static_cast<unsigned>(0.8*hashTableSize);
         hashTable.resize(hashTableSize);
         Rehasher rehasher(hashTable);
         entryPool.enumAll(rehasher);
      }
   }

   // Read the first tuple from the right side
   if ((rightCount=right->first())==0)
      return false;

   // Setup the lookup
   hashTableIter=hashTable[rightValue->value&(hashTableSize-1)];

   return next();
}
//---------------------------------------------------------------------------
unsigned HashJoin::next()
   // Produce the next tuple
{
   // Repeat until a match is found
   while (true) {
      // Still scanning the hash table?
      for (;hashTableIter;hashTableIter=hashTableIter->next) {
         if (hashTableIter->key==rightValue->value) {
            unsigned leftCount=hashTableIter->count;
            leftValue->value=hashTableIter->key;
            for (unsigned index=0,limit=leftTail.size();index<limit;++index)
               leftTail[index]->value=hashTableIter->values[index];
            hashTableIter=hashTableIter->next;
            return leftCount*rightCount;
         }
      }

      // Read the next tuple from the right
      if ((rightCount=right->next())==0)
         return false;
      hashTableIter=hashTable[rightValue->value&(hashTable.size()-1)];
   }
}
//---------------------------------------------------------------------------
void HashJoin::print(unsigned level)
   // Print the operator tree. Debugging only.
{
   indent(level); std::cout << "<HashJoin ";
   printRegister(leftValue); std::cout << "="; printRegister(rightValue);
   std::cout << " [";
   for (std::vector<Register*>::const_iterator iter=leftTail.begin(),limit=leftTail.end();iter!=limit;++iter) {
      std::cout << " "; printRegister(*iter);
   }
   std::cout << "] [";
   for (std::vector<Register*>::const_iterator iter=rightTail.begin(),limit=rightTail.end();iter!=limit;++iter) {
      std::cout << " "; printRegister(*iter);
   }
   std::cout << "]" << std::endl;
   left->print(level+1);
   right->print(level+1);
   indent(level); std::cout << ">" << std::endl;
}
//---------------------------------------------------------------------------
