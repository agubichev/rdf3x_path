#ifndef H_rts_operator_HashGroupify
#define H_rts_operator_HashGroupify
//---------------------------------------------------------------------------
#include "rts/operator/Operator.hpp"
#include "infra/util/VarPool.hpp"
#include <vector>
//---------------------------------------------------------------------------
/// A hash based aggregation
class HashGroupify : public Operator
{
   private:
   /// A group
   struct Group {
      /// The next group
      Group* next;
      /// The hash value
      unsigned hash;
      /// The cound
      unsigned count;
      /// The values
      unsigned values[];
   };
   /// Helper
   class Rehasher;
   /// Helper
   class Chainer;

   /// The input registers
   std::vector<Register*> values;
   /// The input
   Operator* input;
   /// The groups
   Group* groups,*groupsIter;
   /// The groups pool
   VarPool<Group> groupsPool;

   public:
   /// Constructor
   HashGroupify(Operator* input,const std::vector<Register*>& values);
   /// Destructor
   ~HashGroupify();

   /// Produce the first tuple
   unsigned first();
   /// Produce the next tuple
   unsigned next();

   /// Print the operator tree. Debugging only.
   void print(unsigned indent);
};
//---------------------------------------------------------------------------
#endif
