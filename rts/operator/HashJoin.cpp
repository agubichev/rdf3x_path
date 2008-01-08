#include "rts/operator/HashJoin.hpp"
#include "rts/runtime/Runtime.hpp"
#include <iostream>
//---------------------------------------------------------------------------
HashJoin::HashJoin(Operator* left,Register* leftValue,const std::vector<Register*>& leftTail,Operator* right,Register* rightValue,const std::vector<Register*>& rightTail)
   : left(left),right(right),leftValue(leftValue),rightValue(rightValue),leftTail(leftTail),rightTail(rightTail)
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
   const unsigned hashTableSize = 1503499;
   const unsigned stepSize = 2+leftTail.size();
   hashTable.clear();
   hashTable.resize(hashTableSize);
   for (unsigned leftCount=left->first();leftCount;leftCount=left->next()) {
      // Compute the slot
      unsigned l=leftValue->value;
      unsigned slot=l%hashTableSize;

      // Scan if the entry already exists
      std::vector<unsigned>& column=hashTable[slot];
      bool match=false;
      for (unsigned index=0,limit=column.size();index<limit;index+=stepSize)
         if (l==column[index+1]) {
            // Tuple already in the table?
            match=true;
            for (unsigned index2=0,limit2=stepSize-2;index2<limit2;index++)
               if (leftTail[index2]->value!=column[index+2+index2]) {
                  match=false;
                  break;
               }
            // Then aggregate
            if (match) {
               column[index]+=leftCount;
               break;
            }
         }
      if (match)
         continue;

      // New tuple, append
      column.push_back(leftCount);
      column.push_back(l);
      for (std::vector<Register*>::const_iterator iter=leftTail.begin(),limit=leftTail.end();iter!=limit;++iter)
         column.push_back((*iter)->value);
   }

   // Read the first tuple from the right side
   if ((rightCount=right->first())==0)
      return false;

   // Setup the lookup
   unsigned slot=rightValue->value%hashTableSize;
   hashTableIter=hashTable[slot].begin();
   hashTableLimit=hashTable[slot].end();

   return next();
}
//---------------------------------------------------------------------------
unsigned HashJoin::next()
   // Produce the next tuple
{
   const unsigned stepSize = 2+leftTail.size();

   // Repeat until a match is found
   while (true) {
      // Still scanning the hash table?
      while (hashTableIter!=hashTableLimit) {
         if ((*(hashTableIter+1))==rightValue->value) {
            unsigned leftCount=*hashTableIter; ++hashTableIter;
            leftValue->value=*hashTableIter; ++hashTableIter;
            for (std::vector<Register*>::const_iterator iter=leftTail.begin(),limit=leftTail.end();iter!=limit;++iter,++hashTableIter)
               (*iter)->value=*hashTableIter;
            return leftCount*rightCount;
         }
         hashTableIter+=stepSize;
      }

      // Read the next tuple from the right
      if ((rightCount=right->next())==0)
         return false;
      unsigned slot=rightValue->value%hashTable.size();
      hashTableIter=hashTable[slot].begin();
      hashTableLimit=hashTable[slot].end();
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
