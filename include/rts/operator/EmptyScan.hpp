#ifndef H_rts_operator_EmptyScan
#define H_rts_operator_EmptyScan
//---------------------------------------------------------------------------
#include "rts/operator/Operator.hpp"
//---------------------------------------------------------------------------
/// A scan over the empty set
class EmptyScan : public Operator
{
   public:
   /// Constructor
   EmptyScan();
   /// Destructor
   ~EmptyScan();

   /// Produce the first tuple
   unsigned first();
   /// Produce the next tuple
   unsigned next();

   /// Print the operator tree. Debugging only.
   void print(unsigned indent);
};
//---------------------------------------------------------------------------
#endif
