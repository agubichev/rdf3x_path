#ifndef H_rts_operator_IndexScan
#define H_rts_operator_IndexScan
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
#include "rts/database/Database.hpp"
#include "rts/segment/FactsSegment.hpp"
//---------------------------------------------------------------------------
class Register;
//---------------------------------------------------------------------------
/// An index scan over the facts table
class IndexScan : public Operator
{
   private:
   /// The registers for the different parts of the triple
   Register* value1,*value2,*value3;
   /// The different boundings
   bool bound1,bound2,bound3;
   /// The facts segment
   FactsSegment& facts;
   /// The data order
   Database::DataOrder order;
   /// The scan
   FactsSegment::Scan scan;

   /// Constructor
   IndexScan(Database& db,Database::DataOrder order,Register* value1,bool bound1,Register* value2,bool bound2,Register* value3,bool bound3);

   // Implementations
   class Scan;
   class ScanFilter2;
   class ScanFilter3;
   class ScanFilter23;
   class ScanPrefix1;
   class ScanPrefix1Filter3;
   class ScanPrefix12;
   class ScanPrefix123;

   public:
   /// Destructor
   ~IndexScan();

   /// Produce the first tuple
   virtual unsigned first() = 0;
   /// Produce the next tuple
   virtual unsigned next() = 0;

   /// Print the operator tree. Debugging only.
   void print(DictionarySegment& dict,unsigned indent);

   /// Create a suitable operator
   static IndexScan* create(Database& db,Database::DataOrder order,Register* subjectRegister,bool subjectBound,Register* predicateRegister,bool predicateBound,Register* objectRegister,bool objectBound);
};
//---------------------------------------------------------------------------
#endif
