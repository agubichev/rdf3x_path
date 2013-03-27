#ifndef H_rts_operator_RegularPathScan
#define H_rts_operator_RegularPathScan
//---------------------------------------------------------------------------
// RDF-3X
// (c) 2013 Andrey Gubichev, Thomas Neumann.
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
//---------------------------------------------------------------------------
#include "rts/operator/Operator.hpp"
#include "rts/database/Database.hpp"
#include "rts/segment/FactsSegment.hpp"
#include "rts/runtime/Runtime.hpp"
#include "cts/infra/QueryGraph.hpp"
#include "rts/segment/DictionarySegment.hpp"
#include <vector>
//---------------------------------------------------------------------------
class Register;
/// An index scan over the facts table
class RegularPathScan : public Operator
{
	public:
   /// Regular expression modifier
   enum Modifier{Add, Mul};
   private:
   /// The registers for the different parts of the triple
   Register* value1,*value3;
   /// The different boundings
   bool bound1,bound3;
   /// Regular expression
   Modifier pathmode;
   /// Predicate from the regular expression
   unsigned predicate;

   /// The data order
   Database::DataOrder order;
   /// DB dictionary
   DictionarySegment dict;

   /// Constructor
   RegularPathScan(Database& db,Database::DataOrder order,Register* value1,bool bound1,Register* value3,bool bound3,double expectedOutputCardinality,Modifier pathmod,unsigned predicate);

   // Implementations
   //class RegularPathPrefix;
   //class RegularPathJoin;

   public:
   /// Destructor
   ~RegularPathScan();

   /// Produce the first tuple
   unsigned first();
   /// Produce the next tuple
   unsigned next();

   /// Print the operator tree. Debugging only.
   void print(PlanPrinter& out);

   /// Add a merge join hint
   void addMergeHint(Register* /*reg1*/,Register* /*reg2*/){};
   /// Register parts of the tree that can be executed asynchronous
   void getAsyncInputCandidates(Scheduler& /*scheduler*/){};

   /// Create a suitable operator
   static RegularPathScan* create(Database& db,Database::DataOrder order,Register* subjectRegister,bool subjectBound,Register* objectRegister,bool objectBound,double expectedOutputCardinality,Modifier pathmod,unsigned predicate);
};
//---------------------------------------------------------------------------


#endif
