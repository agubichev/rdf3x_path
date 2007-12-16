#ifndef H_rts_operator_AggregatedIndexScan
#define H_rts_operator_AggregatedIndexScan
//---------------------------------------------------------------------------
#include "rts/operator/Operator.hpp"
#include "rts/database/Database.hpp"
#include "rts/segment/AggregatedFactsSegment.hpp"
//---------------------------------------------------------------------------
class Register;
//---------------------------------------------------------------------------
/// An index scan over the aggregated facts table
class AggregatedIndexScan : public Operator
{
   private:
   /// The registers for the different parts of the triple
   Register* value1,*value2;
   /// Register for the count
   Register* count;
   /// The stop conditions
   unsigned stop1,stop2;
   /// Which colums form the prefix
   unsigned prefix;
   /// Which colums to need a filter?
   unsigned filter;
   /// The different boundings
   bool bound1,bound2;
   /// The facts segment
   AggregatedFactsSegment& facts;
   /// The data order
   Database::DataOrder order;
   /// The scan
   AggregatedFactsSegment::Scan scan;

   public:
   /// Constructor
   AggregatedIndexScan(Database& db,Database::DataOrder order,Register* subjectRegister,bool subjectBound,Register* predicateRegister,bool predicateBound,Register* objectRegister,bool objectBound);
   /// Destructor
   ~AggregatedIndexScan();

   /// Produce the first tuple
   bool first();
   /// Produce the next tuple
   bool next();

   /// Print the operator tree. Debugging only.
   void print(unsigned indent);
};
//---------------------------------------------------------------------------
#endif
