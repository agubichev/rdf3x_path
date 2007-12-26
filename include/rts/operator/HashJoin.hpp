#ifndef H_rts_operator_HashJoin
#define H_rts_operator_HashJoin
//---------------------------------------------------------------------------
#include "rts/operator/Operator.hpp"
#include <vector>
//---------------------------------------------------------------------------
class Register;
//---------------------------------------------------------------------------
/// A memory based hash join
class HashJoin : public Operator
{
   private:
   /// The input
   Operator* left,*right;
   /// The join attributes
   Register* leftValue,*rightValue;
   /// The non-join attributes
   std::vector<Register*> leftTail,rightTail;

   /// The hash table
   std::vector<std::vector<unsigned> > hashTable;
   /// The current iter
   std::vector<unsigned>::const_iterator hashTableIter,hashTableLimit;
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
