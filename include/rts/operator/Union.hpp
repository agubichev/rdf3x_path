#ifndef H_rts_operator_Union
#define H_rts_operator_Union
//---------------------------------------------------------------------------
#include "rts/operator/Operator.hpp"
#include <vector>
//---------------------------------------------------------------------------
/// A union of multiple input operators. The operators are concatenated, not
/// duplicates are eliminated.
class Union : public Operator
{
   private:
   /// The parts
   std::vector<Operator*> parts;
   /// The register mappings
   std::vector<std::vector<Register*> > mappings;
   /// The initialization lists
   std::vector<std::vector<Register*> > initializations;
   /// The current slot
   unsigned current;

   // Get the first tuple from the current part
   unsigned firstFromPart();

   public:
   /// Constructor
   Union(const std::vector<Operator*>& parts,const std::vector<std::vector<Register*> >& mappings,const std::vector<std::vector<Register*> >& initializations);
   /// Destructor
   ~Union();

   /// Produce the first tuple
   unsigned first();
   /// Produce the next tuple
   unsigned next();

   /// Print the operator tree. Debugging only.
   void print(unsigned indent);
};
//---------------------------------------------------------------------------
#endif