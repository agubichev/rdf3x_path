#ifndef H_cts_plangen_Costs
#define H_cts_plangen_Costs
//---------------------------------------------------------------------------
/// A cost model used by the plan generator
class Costs
{
   public:
   /// Data type for costs
   typedef double cost_t;

   private:
   /// Costs for a seek in 1/10ms
   static const unsigned seekCosts = 95;
   /// Costs for a sequential page read in 1/10ms
   static const unsigned scanCosts = 17;

   public:
   /// Costs for traversing a btree
   static cost_t seekBtree() { return 3*seekCosts; }
   /// Costs for scanning a number of pages
   static cost_t scan(unsigned pages) { return pages*scanCosts; }
};
//---------------------------------------------------------------------------
#endif
