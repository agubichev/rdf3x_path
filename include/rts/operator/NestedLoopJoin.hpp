#ifndef H_rts_operator_NestedLoopJoin
#define H_rts_operator_NestedLoopJoin
//---------------------------------------------------------------------------
#include "rts/operator/Operator.hpp"
//---------------------------------------------------------------------------
/// A nested loop join
class NestedLoopJoin : public Operator
{
   private:
   /// The input
   Operator* left,*right;
   /// The count from the left side
   unsigned leftCount;

   public:
   /// Constructor
   NestedLoopJoin(Operator* left,Operator* right);
   /// Destructor
   ~NestedLoopJoin();

   /// Produce the first tuple
   unsigned first();
   /// Produce the next tuple
   unsigned next();

   /// Print the operator tree. Debugging only.
   void print(unsigned indent);
};
//---------------------------------------------------------------------------
#endif
