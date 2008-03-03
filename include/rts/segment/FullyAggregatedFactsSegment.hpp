#ifndef H_rts_segment_FullyAggregatedFactsSegment
#define H_rts_segment_FullyAggregatedFactsSegment
//---------------------------------------------------------------------------
#include "rts/segment/Segment.hpp"
#include "rts/buffer/BufferManager.hpp"
//---------------------------------------------------------------------------
/// Fully aggregated facts, i.e. counts for single values
class FullyAggregatedFactsSegment : public Segment
{
   private:
   /// The start of the raw facts table
   unsigned tableStart;
   /// The root of the index b-tree
   unsigned indexRoot;
   /// Statistics
   unsigned pages,groups1;

   /// Lookup the first page contains entries >= the start condition
   bool lookup(unsigned start1,BufferReference& ref);

   FullyAggregatedFactsSegment(const FullyAggregatedFactsSegment&);
   void operator=(const FullyAggregatedFactsSegment&);

   public:
   /// Constructor
   FullyAggregatedFactsSegment(BufferManager& bufferManager,unsigned tableStart,unsigned indexRoot,unsigned pages,unsigned groups1);

   /// Get the number of pages in the segment
   unsigned getPages() const { return pages; }
   /// Get the number of level 1 groups
   unsigned getLevel1Groups() const { return groups1; }

   /// A scan over the facts segment
   class Scan {
      private:
      /// The maximum number of entries per page
      static const unsigned maxCount = BufferManager::pageSize;
      /// A (aggregated) triple
      struct Triple {
         unsigned value1,count;
      };

      /// The current page
      BufferReference current;
      /// The segment
      FullyAggregatedFactsSegment* seg;
      /// The position on the current page
      const Triple* pos,*posLimit;
      /// The decompressed tripples
      Triple tripples[maxCount];

      Scan(const Scan&);
      void operator=(const Scan&);

      /// Read the next page
      bool readNextPage();

      public:
      /// Constructor
      Scan();
      /// Destructor
      ~Scan();

      /// Start a new scan over the whole segment and reads the first entry
      bool first(FullyAggregatedFactsSegment& segment);
      /// Start a new scan starting from the first entry >= the start condition and reads the first entry
      bool first(FullyAggregatedFactsSegment& segment,unsigned start1);

      /// Read the next entry
      bool next() { if ((++pos)>=posLimit) return readNextPage(); else return true; }
      /// Get the first value
      unsigned getValue1() const { return (*pos).value1; }
      /// Get the count
      unsigned getCount() const { return (*pos).count; }

      /// Close the scan
      void close();
   };
   friend class Scan;
};
//---------------------------------------------------------------------------
#endif
