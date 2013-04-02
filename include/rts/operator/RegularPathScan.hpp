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
#include "infra/util/VarPool.hpp"
#include "rts/ferrari/Index.h"
#include <vector>
#include <map>
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
   /// The bounds
   bool bound1,bound3;
   bool const1,const3;
   /// Regular expression
   Modifier pathmode;
   /// Predicate from the regular expression
   unsigned predicate;
   /// The data order
   Database::DataOrder order;
   /// DB dictionary
   DictionarySegment dict;
   Index* ferrari;
   /// Operator-input
   Operator* op1, *op2;
   Register* firstSource,*secondSource;
	/// matching along the inverse edges?
	bool inverse;

   std::vector<Register*> firstBinding,secondBinding;

   /// An entry to store the elements of one of the subtrees
   struct Entry {
      /// The key
      unsigned key;
      /// The count
      unsigned count;
      /// Further values
      unsigned values[];
   };
   /// The pool of  entries
   VarPool<Entry>* entryPool;
   // store the results of one of the subtrees
   std::vector<Entry*> storage;
   decltype(storage.begin()) storageIterator;
   // number of elements returned by the right subtree (probe)
   unsigned rightCount;
   //
   void buildStorage();

   //implementations
   class RPConstant;
   class RPBounded;
   /// Constructor
   RegularPathScan(Database& db,Database::DataOrder order,Register* value1,bool const1,Register* value3,bool const3,double expectedOutputCardinality,Modifier pathmod,unsigned predicate,bool inverse,Index* ferrari);


   public:
   /// Destructor
   ~RegularPathScan();

   /// Produce the first tuple
   virtual unsigned first();
   /// Produce the next tuple
   virtual unsigned next();

   /// Print the operator tree. Debugging only.
   void print(PlanPrinter& out);

   /// Add a merge join hint
   void addMergeHint(Register* /*reg1*/,Register* /*reg2*/){};
   /// Register parts of the tree that can be executed asynchronous
   void getAsyncInputCandidates(Scheduler& /*scheduler*/){};

   /// Register the input operator
   void setFirstInput(Operator* left);
   void setSecondInput(Operator* right);

   // set the source nodes defined by the subplans
   void setFirstSource(Register* left);
   void setSecondSource(Register* right);
   // set the "other" nodes, carried out from the subplans
   void setFirstBinding(std::vector<Register*>& firstBinding);
   void setSecondBinding(std::vector<Register*>& secondBinding);

   bool isFirstInputSet();
   /// Create a suitable operator
   static RegularPathScan* create(Database& db,Database::DataOrder order,Register* subjectRegister,bool subjectBound,Register* objectRegister,bool objectBound,double expectedOutputCardinality,Modifier pathmod,unsigned predicate,Index* ferrari);
};
//---------------------------------------------------------------------------


#endif
