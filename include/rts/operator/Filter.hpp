#ifndef H_rts_operator_Filter
#define H_rts_operator_Filter
//---------------------------------------------------------------------------
#include "rts/operator/Operator.hpp"
#include <vector>
//---------------------------------------------------------------------------
class Register;
//---------------------------------------------------------------------------
/// A selection that checks if a register is within a set of valid values
class Filter : public Operator
{
   private:
   /// The input
   Operator* input;
   /// The filter register
   Register* filter;
   /// The valid values
   std::vector<unsigned> values;
   /// Cached lookup
   unsigned lastValue;
   /// Cached lookup
   bool lastResult;

   public:
   /// Constructor
   Filter(Operator* input,Register* filter,const std::vector<unsigned>& values);
   /// Destructor
   ~Filter();

   /// Produce the first tuple
   unsigned first();
   /// Produce the next tuple
   unsigned next();

   /// Print the operator tree. Debugging only.
   void print(unsigned indent);
};
//---------------------------------------------------------------------------
#endif
