#ifndef H_rts_operator_NestedLoopJoin
#define H_rts_operator_NestedLoopJoin
//---------------------------------------------------------------------------
// RDF-3X
// (c) 2008 Thomas Neumann. Web site: http://www.mpi-inf.mpg.de/~neumann/rdf3x
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
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
