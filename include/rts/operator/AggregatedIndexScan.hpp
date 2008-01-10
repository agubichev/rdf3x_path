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
   /// The different boundings
   bool bound1,bound2;
   /// The facts segment
   AggregatedFactsSegment& facts;
   /// The data order
   Database::DataOrder order;
   /// The scan
   AggregatedFactsSegment::Scan scan;

   /// Constructor
   AggregatedIndexScan(Database& db,Database::DataOrder order,Register* value1,bool bound1,Register* value2,bool bound2);

   // Implementations
   class Scan;
   class ScanFilter2;
   class ScanPrefix1;
   class ScanPrefix12;

   public:
   /// Destructor
   ~AggregatedIndexScan();

   /// Produce the first tuple
   virtual unsigned first() = 0;
   /// Produce the next tuple
   virtual unsigned next() = 0;

   /// Print the operator tree. Debugging only.
   void print(unsigned indent);

   /// Create a suitable operator
   static AggregatedIndexScan* create(Database& db,Database::DataOrder order,Register* subjectRegister,bool subjectBound,Register* predicateRegister,bool predicateBound,Register* objectRegister,bool objectBound);
};
//---------------------------------------------------------------------------
#endif
