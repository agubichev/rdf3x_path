#ifndef H_rts_operator_NestedLoopFilter
#define H_rts_operator_NestedLoopFilter
//---------------------------------------------------------------------------
#include "rts/operator/Operator.hpp"
#include <vector>
//---------------------------------------------------------------------------
class Register;
//---------------------------------------------------------------------------
/// A binding filter that calls its input multiple times with different
/// variable bindings
class NestedLoopFilter : public Operator
{
   private:
   /// The input
   Operator* input;
   /// The filter register
   Register* filter;
   /// The valid values
   std::vector<unsigned> values;
   /// The current position
   unsigned pos;

   public:
   /// Constructor
   NestedLoopFilter(Operator* input,Register* filter,const std::vector<unsigned>& values);
   /// Destructor
   ~NestedLoopFilter();

   /// Produce the first tuple
   unsigned first();
   /// Produce the next tuple
   unsigned next();

   /// Print the operator tree. Debugging only.
   void print(unsigned indent);
};
//---------------------------------------------------------------------------
#endif
