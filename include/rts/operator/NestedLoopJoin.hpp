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

   public:
   /// Constructor
   NextedLoopJoin(Operator* left,Operator* right);
   /// Destructor
   ~NestedLoopJoin();

   /// Produce the first tuple
   bool first();
   /// Produce the next tuple
   bool next();

   /// Print the operator tree. Debugging only.
   void print(unsigned indent);
};
//---------------------------------------------------------------------------
#endif
