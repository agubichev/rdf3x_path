#ifndef H_rts_operator_MergeJoin
#define H_rts_operator_MergeJoin
//---------------------------------------------------------------------------
#include "rts/operator/Operator.hpp"
#include <vector>
//---------------------------------------------------------------------------
class Register;
//---------------------------------------------------------------------------
/// A merge join. The input has to be sorted by the join attributes.
class MergeJoin : public Operator
{
   public:
   /// The input
   Operator* left,*right;
   /// The join attributes
   Register* leftValue,*rightValue;
   /// The non-join attributes
   std::vector<Register*> leftTail,rightTail;

   private:
   /// Possible states while scanning the input
   enum ScanState {
      empty,
      scanHasBothSwapped,scanStepLeft,scanStepBoth,scanStepRight,scanHasBoth,
      loopEmptyLeft,loopEmptyRight,loopEmptyRightHasData,
      loopEqualLeftHasData,loopEqualLeft,
      loopEqualRightHasData,loopEqualRight,
      loopSpooledRightEmpty,loopSpooledRightHasData
    };

   /// Tuple counts
   unsigned leftCount,rightCount;
   /// Shadow tuples
   std::vector<unsigned> leftShadow,rightShadow;
   /// The buffer
   std::vector<unsigned> buffer;
   /// The iterator over the buffer
   std::vector<unsigned>::const_iterator bufferIter;
   /// The current scan state
   ScanState scanState;
   /// Is a copy of the left hand side available? Only used for loopSpooled*
   bool leftInCopy;

   /// Swap the left tuple with its shadow
   void swapLeft();
   /// Swap the right tuple with its shadow
   void swapRight();

   /// Handle the n:m case
   void handleNM();

   public:
   /// Constructor
   MergeJoin(Operator* left,Register* leftValue,const std::vector<Register*>& leftTail,Operator* right,Register* rightValue,const std::vector<Register*>& rightTail);
   /// Destructor
   ~MergeJoin();

   /// Produce the first tuple
   unsigned first();
   /// Produce the next tuple
   unsigned next();

   /// Print the operator tree. Debugging only.
   void print(unsigned indent);
};
//---------------------------------------------------------------------------
#endif
