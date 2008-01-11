#ifndef H_rts_operator_HashJoin
#define H_rts_operator_HashJoin
//---------------------------------------------------------------------------
#include "rts/operator/Operator.hpp"
#include "infra/util/VarPool.hpp"
#include <vector>
//---------------------------------------------------------------------------
class Register;
//---------------------------------------------------------------------------
/// A memory based hash join
class HashJoin : public Operator
{
   private:
   /// A hash table entry
   struct Entry {
      /// The next entry
      Entry* next;
      /// The key
      unsigned key;
      /// The count
      unsigned count;
      /// Further values
      unsigned values[];
   };
   /// Helper
   class Rehasher;

   /// The input
   Operator* left,*right;
   /// The join attributes
   Register* leftValue,*rightValue;
   /// The non-join attributes
   std::vector<Register*> leftTail,rightTail;
   /// The pool of hash entry
   VarPool<Entry> entryPool;
   /// The hash table
   std::vector<Entry*> hashTable;
   /// The current iter
   Entry* hashTableIter;
   /// The tuple count from the right side
   unsigned rightCount;

   public:
   /// Constructor
   HashJoin(Operator* left,Register* leftValue,const std::vector<Register*>& leftTail,Operator* right,Register* rightValue,const std::vector<Register*>& rightTail);
   /// Destructor
   ~HashJoin();

   /// Produce the first tuple
   unsigned first();
   /// Produce the next tuple
   unsigned next();

   /// Print the operator tree. Debugging only.
   void print(unsigned indent);
};
//---------------------------------------------------------------------------
#endif
