#ifndef H_rts_operator_ResultsPrinter
#define H_rts_operator_ResultsPrinter
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
class Database;
class DictionarySegment;
class Register;
//---------------------------------------------------------------------------
/// Consumes its input and prints it. Produces a single empty tuple.
class ResultsPrinter : public Operator
{
   public:
   /// Duplicate handling
   enum DuplicateHandling { ReduceDuplicates, ExpandDuplicates, CountDuplicates, ShowDuplicates };

   private:
   /// The output registers
   std::vector<Register*> output;
   /// The input
   Operator* input;
   /// The dictionary
   DictionarySegment& dictionary;
   /// The duplicate handling
   DuplicateHandling duplicateHandling;
   /// Maximum number of output tuples
   unsigned limit;
   /// Skip the printing, resolve only?
   bool silent;

   public:
   /// Constructor
   ResultsPrinter(Database& db,Operator* input,const std::vector<Register*>& output,DuplicateHandling duplicateHandling,unsigned limit=~0u,bool silent=false);
   /// Destructor
   ~ResultsPrinter();

   /// Produce the first tuple
   unsigned first();
   /// Produce the next tuple
   unsigned next();

   /// Print the operator tree. Debugging only.
   void print(DictionarySegment& dict,unsigned indent);
   /// Add a merge join hint
   void addMergeHint(Register* reg1,Register* reg2);
   /// Register parts of the tree that can be executed asynchronous
   void getAsyncInputCandidates(Scheduler& scheduler);
};
//---------------------------------------------------------------------------
#endif
