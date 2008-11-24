#ifndef H_rts_operator_Selection
#define H_rts_operator_Selection
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
   /// Check for equal?
   bool equal;

   /// Equal
   class Equal;
   /// Not eqaul
   class NotEqual;

   /// Constructor
   Selection(Operator* input,const std::vector<Register*>& predicates,bool equal);

   public:
   /// Destructor
   ~Selection();

   /// Produce the first tuple
   virtual unsigned first() = 0;
   /// Produce the next tuple
   virtual unsigned next() = 0;

   /// Create a selection
   static Selection* create(Operator* input,const std::vector<Register*>& predicates,bool equal);
   /// Print the operator tree. Debugging only.
   void print(DictionarySegment& dict,unsigned indent);
   /// Add a merge join hint
   void addMergeHint(Register* reg1,Register* reg2);
   /// Register parts of the tree that can be executed asynchronous
   void getAsyncInputCandidates(Scheduler& scheduler);
};
//---------------------------------------------------------------------------
#endif
