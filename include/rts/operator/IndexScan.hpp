#ifndef H_rts_operator_IndexScan
#define H_rts_operator_IndexScan
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
   /// The stop conditions
   unsigned stop1,stop2,stop3;
   /// Which colums form the prefix
   unsigned prefix;
   /// Which colums to need a filter?
   unsigned filter;
   /// The different boundings
   bool bound1,bound2,bound3;
   /// The facts segment
   FactsSegment& facts;
   /// The data order
   Database::DataOrder order;
   /// The scan
   FactsSegment::Scan scan;

   public:
   /// Constructor
   IndexScan(Database& db,Database::DataOrder order,Register* subjectRegister,bool subjectBound,Register* predicateRegister,bool predicateBound,Register* objectRegister,bool objectBound);
   /// Destructor
   ~IndexScan();

   /// Produce the first tuple
   bool first();
   /// Produce the next tuple
   bool next();

   /// Print the operator tree. Debugging only.
   void print(unsigned indent);
};
//---------------------------------------------------------------------------
#endif
