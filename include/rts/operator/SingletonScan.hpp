#ifndef H_rts_operator_SingletonScan
#define H_rts_operator_SingletonScan
//---------------------------------------------------------------------------
#include "rts/operator/Operator.hpp"
//---------------------------------------------------------------------------
/// A scan over a single empty tuple
class SingletonScan : public Operator
{
   public:
   /// Constructor
   SingletonScan();
   /// Destructor
   ~SingletonScan();

   /// Produce the first tuple
   unsigned first();
   /// Produce the next tuple
   unsigned next();

   /// Print the operator tree. Debugging only.
   void print(unsigned indent);
};
//---------------------------------------------------------------------------
#endif
