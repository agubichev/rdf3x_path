#ifndef H_rts_segment_AggregatedFactsSegment
#define H_rts_segment_AggregatedFactsSegment
//---------------------------------------------------------------------------
#include "rts/segment/Segment.hpp"
#include "rts/buffer/BufferManager.hpp"
//---------------------------------------------------------------------------
/// A compressed and aggregated facts table stored in a clustered B-Tree
class AggregatedFactsSegment : public Segment
{
   private:
   /// The start of the raw facts table
   unsigned tableStart;
   /// The root of the index b-tree
   unsigned indexRoot;
   /// Statistics
   unsigned pages,groups1,groups2;

   /// Lookup the first page contains entries >= the start condition
   bool lookup(unsigned start1,unsigned start2,BufferReference& ref);

   AggregatedFactsSegment(const AggregatedFactsSegment&);
   void operator=(const AggregatedFactsSegment&);

   public:
   /// Constructor
   AggregatedFactsSegment(BufferManager& bufferManager,unsigned tableStart,unsigned indexRoot,unsigned pages,unsigned groups1,unsigned groups2);

   /// Get the number of pages in the segment
   unsigned getPages() const { return pages; }
   /// Get the number of level 1 groups
   unsigned getLevel1Groups() const { return groups1; }
   /// Get the number of level 2 groups
   unsigned getLevel2Groups() const { return groups2; }

   /// A scan over the facts segment
   class Scan {
      private:
      /// The maximum number of entries per page
      static const unsigned maxCount = BufferManager::pageSize;
      /// A tripple
      struct Tripple {
         unsigned value1,value2,count;
      };

      /// The current page
      BufferReference current;
      /// The segment
      AggregatedFactsSegment* seg;
      /// The position on the current page
      const Tripple* pos,*posLimit;
      /// The decompressed tripples
      Tripple tripples[maxCount];

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
      bool first(AggregatedFactsSegment& segment);
      /// Start a new scan starting from the first entry >= the start condition and reads the first entry
      bool first(AggregatedFactsSegment& segment,unsigned start1,unsigned start2);

      /// Read the next entry
      bool next() { if ((++pos)>=posLimit) return readNextPage(); else return true; }
      /// Get the first value
      unsigned getValue1() const { return (*pos).value1; }
      /// Get the second value
      unsigned getValue2() const { return (*pos).value2; }
      /// Get the count
      unsigned getCount() const { return (*pos).count; }

      /// Close the scan
      void close();
   };
   friend class Scan;
};
//---------------------------------------------------------------------------
#endif
