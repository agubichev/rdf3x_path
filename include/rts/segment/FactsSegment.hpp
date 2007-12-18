#ifndef H_rts_segment_FactsSegment
#define H_rts_segment_FactsSegment
//---------------------------------------------------------------------------
#include "rts/segment/Segment.hpp"
#include "rts/buffer/BufferManager.hpp"
//---------------------------------------------------------------------------
/// A compressed facts table stored in a clustered B-Tree
class FactsSegment : public Segment
{
   private:
   /// The start of the raw facts table
   unsigned tableStart;
   /// The root of the index b-tree
   unsigned indexRoot;
   /// Statistics
   unsigned pages,groups1,groups2,cardinality;

   /// Lookup the first page contains entries >= the start condition
   bool lookup(unsigned start1,unsigned start2,unsigned start3,BufferReference& ref);

   FactsSegment(const FactsSegment&);
   void operator=(const FactsSegment&);

   public:
   /// Constructor
   FactsSegment(BufferManager& bufferManager,unsigned tableStart,unsigned indexRoot,unsigned pages,unsigned groups1,unsigned groups2,unsigned cardinality);

   /// Get the number of pages in the segment
   unsigned getPages() const { return pages; }
   /// Get the number of level 1 groups
   unsigned getLevel1Groups() const { return groups1; }
   /// Get the number of level 2 groups
   unsigned getLevel2Groups() const { return groups2; }
   /// Get the total cardinality
   unsigned getCardinality() const { return cardinality; }

   /// A scan over the facts segment
   class Scan {
      private:
      /// The current page
      BufferReference current;
      /// The segment
      FactsSegment* seg;
      /// The position on the current page
      unsigned pos;
      /// The last triple read
      unsigned value1,value2,value3;

      Scan(const Scan&);
      void operator=(const Scan&);

      public:
      /// Constructor
      Scan();
      /// Destructor
      ~Scan();

      /// Start a new scan over the whole segment and reads the first entry
      bool first(FactsSegment& segment);
      /// Start a new scan starting from the first entry >= the start condition and reads the first entry
      bool first(FactsSegment& segment,unsigned start1,unsigned start2,unsigned start3);

      /// Read the next entry
      bool next();
      /// Get the first value
      unsigned getValue1() const { return value1; }
      /// Get the second value
      unsigned getValue2() const { return value2; }
      /// Get the third value
      unsigned getValue3() const { return value3; }

      /// Close the scan
      void close();
   };
   friend class Scan;
};
//---------------------------------------------------------------------------
#endif
