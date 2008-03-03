#ifndef H_rts_operator_FullyAggregatedIndexScan
#define H_rts_operator_FullyAggregatedIndexScan
//---------------------------------------------------------------------------
#include "rts/operator/Operator.hpp"
#include "rts/database/Database.hpp"
#include "rts/segment/FullyAggregatedFactsSegment.hpp"
//---------------------------------------------------------------------------
class Register;
//---------------------------------------------------------------------------
/// An index scan over the fully aggregated facts table
class FullyAggregatedIndexScan : public Operator
{
   private:
   /// The registers for the different parts of the triple
   Register* value1;
   /// The different boundings
   bool bound1;
   /// The facts segment
   FullyAggregatedFactsSegment& facts;
   /// The data order
   Database::DataOrder order;
   /// The scan
   FullyAggregatedFactsSegment::Scan scan;

   /// Constructor
   FullyAggregatedIndexScan(Database& db,Database::DataOrder order,Register* value1,bool bound1);

   // Implementations
   class Scan;
   class ScanPrefix1;

   public:
   /// Destructor
   ~FullyAggregatedIndexScan();

   /// Produce the first tuple
   virtual unsigned first() = 0;
   /// Produce the next tuple
   virtual unsigned next() = 0;

   /// Print the operator tree. Debugging only.
   void print(unsigned indent);

   /// Create a suitable operator
   static FullyAggregatedIndexScan* create(Database& db,Database::DataOrder order,Register* subjectRegister,bool subjectBound,Register* predicateRegister,bool predicateBound,Register* objectRegister,bool objectBound);
};
//---------------------------------------------------------------------------
#endif
