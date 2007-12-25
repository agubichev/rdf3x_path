#ifndef H_rts_operator_ResultsPrinter
#define H_rts_operator_ResultsPrinter
//---------------------------------------------------------------------------
#include "rts/operator/Operator.hpp"
#include <vector>
//---------------------------------------------------------------------------
class Database;
class DictionarySegment;
class Register;
//---------------------------------------------------------------------------
/// Consumes its input and prints it. Pruduces a single empty tuple.
class ResultsPrinter : public Operator
{
   public:
   /// Duplicate handling
   enum DuplicateHandling { ReduceDuplicates, ExpandDuplicates, CountDuplicates };

   private:
   /// The output registers
   std::vector<Register*> output;
   /// The input
   Operator* input;
   /// The dictionary
   DictionarySegment& dictionary;
   /// The duplicate handling
   DuplicateHandling duplicateHandling;
   /// Skip the printing, resolve only?
   bool silent;

   public:
   /// Constructor
   ResultsPrinter(Database& db,Operator* input,const std::vector<Register*>& output,DuplicateHandling duplicateHandling,bool silent=false);
   /// Destructor
   ~ResultsPrinter();

   /// Produce the first tuple
   unsigned first();
   /// Produce the next tuple
   unsigned next();

   /// Print the operator tree. Debugging only.
   void print(unsigned indent);
};
//---------------------------------------------------------------------------
#endif
