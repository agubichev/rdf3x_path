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

   /// Lookup the first page contains entries >= the start condition
   bool lookup(unsigned start1,unsigned start2,BufferReference& ref);

   AggregatedFactsSegment(const AggregatedFactsSegment&);
   void operator=(const AggregatedFactsSegment&);

   public:
   /// Constructor
   AggregatedFactsSegment(BufferManager& bufferManager,unsigned tableStart,unsigned indexRoot);

   /// A scan over the facts segment
   class Scan {
      private:
      /// The current page
      BufferReference current;
      /// The segment
      AggregatedFactsSegment* seg;
      /// The position on the current page
      unsigned pos;
      /// The last triple read
      unsigned value1,value2,count;

      Scan(const Scan&);
      void operator=(const Scan&);

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
      bool next();
      /// Get the first value
      unsigned getValue1() const { return value1; }
      /// Get the second value
      unsigned getValue2() const { return value2; }
      /// Get the count
      unsigned getCount() const { return count; }

      /// Close the scan
      void close();
   };
   friend class Scan;
};
//---------------------------------------------------------------------------
#endif
