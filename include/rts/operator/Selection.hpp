#ifndef H_rts_operator_Selection
#define H_rts_operator_Selection
//---------------------------------------------------------------------------
#include "rts/operator/Operator.hpp"
#include <vector>
//---------------------------------------------------------------------------
class Register;
//---------------------------------------------------------------------------
/// Applies a number of selctions. Checks that pairs of registers have the same values
class Selection : public Operator
{
   private:
   /// The predicates
   std::vector<Register*> predicates;
   /// The input
   Operator* input;

   public:
   /// Constructor
   Selection(Operator* input,const std::vector<Register*>& predicates);
   /// Destructor
   ~Selection();

   /// Produce the first tuple
   unsigned first();
   /// Produce the next tuple
   unsigned next();

   /// Print the operator tree. Debugging only.
   void print(unsigned indent);
};
//---------------------------------------------------------------------------
#endif
