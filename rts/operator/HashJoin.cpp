#include "rts/operator/HashJoin.hpp"
#include "rts/runtime/Runtime.hpp"
#include <iostream>
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
   delete left;
   delete right;
}
//---------------------------------------------------------------------------
static inline unsigned hash1(unsigned key,unsigned hashTableSize) { return key&(hashTableSize-1); }
static inline unsigned hash2(unsigned key,unsigned hashTableSize) { return hashTableSize+((key^(key>>3))&(hashTableSize-1)); }
//---------------------------------------------------------------------------
void HashJoin::insert(Entry* e)
   // Insert into the hash table
{
   unsigned hashTableSize=hashTable.size()/2;
   // Try to insert
   bool firstTable=true;
   for (unsigned index=0;index<hashTableSize;index++) {
      unsigned slot=firstTable?hash1(e->key,hashTableSize):hash2(e->key,hashTableSize);
      std::swap(e,hashTable[slot]);
      if (!e)
         return;
      firstTable=!firstTable;
   }

   // No place found, rehash
   std::vector<Entry*> oldTable;
   oldTable.resize(4*hashTableSize);
   swap(hashTable,oldTable);
   for (std::vector<Entry*>::const_iterator iter=oldTable.begin(),limit=oldTable.end();iter!=limit;++iter)
      if (*iter)
         insert(*iter);
   insert(e);
}
//---------------------------------------------------------------------------
HashJoin::Entry* HashJoin::lookup(unsigned key)
   // Search an entry in the hash table
{
   unsigned hashTableSize=hashTable.size()/2;
   Entry* e=hashTable[hash1(key,hashTableSize)];
   if (e&&(e->key==key))
      return e;
   e=hashTable[hash2(key,hashTableSize)];
   if (e&&(e->key==key))
      return e;
   return 0;
}
//---------------------------------------------------------------------------
unsigned HashJoin::first()
   // Produce the first tuple
{
   // Build the hash table from the left side
   unsigned hashTableSize = 1024;
   unsigned tailLength=leftTail.size();
   hashTable.clear();
   hashTable.resize(2*hashTableSize);
   for (unsigned leftCount=left->first();leftCount;leftCount=left->next()) {
      // Compute the slots
      unsigned leftKey=leftValue->value;
      unsigned slot1=hash1(leftKey,hashTableSize),slot2=hash2(leftKey,hashTableSize);

      // Scan if the entry already exists
      Entry* e=hashTable[slot1];
      if ((!e)||(e->key!=leftKey))
         e=hashTable[slot2];
      if (e&&(e->key==leftKey)) {
         unsigned ofs=(e==hashTable[slot1])?slot1:slot2;
         bool match=false;
         for (Entry* iter=e;iter;iter=iter->next)
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

         // Append to the current bucket
         e=entryPool.alloc();
         e->next=hashTable[ofs];
         hashTable[ofs]=e;
         e->key=leftKey;
         e->count=leftCount;
         for (unsigned index2=0;index2<tailLength;index2++)
            e->values[index2]=leftTail[index2]->value;
         continue;
      }

      // Create a new tuple
      e=entryPool.alloc();
      e->next=0;
      e->key=leftKey;
      e->count=leftCount;
      for (unsigned index2=0;index2<tailLength;index2++)
         e->values[index2]=leftTail[index2]->value;

      // And insert it
      insert(e);
      hashTableSize=hashTable.size()/2;
   }

   // Read the first tuple from the right side
   if ((rightCount=right->first())==0)
      return false;

   // Setup the lookup
   hashTableIter=lookup(rightValue->value);

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
         unsigned leftCount=hashTableIter->count;
         leftValue->value=hashTableIter->key;
         for (unsigned index=0,limit=leftTail.size();index<limit;++index)
            leftTail[index]->value=hashTableIter->values[index];
         hashTableIter=hashTableIter->next;
         return leftCount*rightCount;
      }

      // Read the next tuple from the right
      if ((rightCount=right->next())==0)
         return false;
      hashTableIter=lookup(rightValue->value);
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
