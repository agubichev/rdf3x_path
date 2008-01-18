#ifndef H_rts_operator_MergeUnion
#define H_rts_operator_MergeUnion
//---------------------------------------------------------------------------
#include "rts/operator/Operator.hpp"
//---------------------------------------------------------------------------
/// A union that merges two sorted streams
class MergeUnion : public Operator
{
   private:
   /// Possible states
   enum State { done, stepLeft, stepRight, stepBoth, leftEmpty, rightEmpty };

   /// The input
   Operator* left,*right;
   /// The input registers
   Register* leftReg,*rightReg;
   /// The result register
   Register* result;
   /// The values
   unsigned leftValue,rightValue;
   /// The counts
   unsigned leftCount,rightCount;
   /// The state
   State state;

   public:
   /// Constructor
   MergeUnion(Register* result,Operator* left,Register* leftReg,Operator* right,Register* rightReg);
   /// Destructor
   ~MergeUnion();

   /// Produce the first tuple
   unsigned first();
   /// Produce the next tuple
   unsigned next();

   /// Print the operator tree. Debugging only.
   void print(unsigned indent);
};
//---------------------------------------------------------------------------
#endif
