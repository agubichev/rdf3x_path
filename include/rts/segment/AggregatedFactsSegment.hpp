#ifndef H_rts_segment_AggregatedFactsSegment
#define H_rts_segment_AggregatedFactsSegment
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

   /// Prefetch a range in the segment
   void prefetchRange(unsigned start1,unsigned start2,unsigned stop1,unsigned stop2);

   /// A scan over the facts segment
   class Scan {
      public:
      /// Hints for skipping through the scan
      class Hint {
         public:
         /// Constructor
         Hint();
         /// Destructor
         virtual ~Hint();

         /// The hint
         virtual void next(unsigned& value1,unsigned& value2) = 0;
      };

      private:
      /// The maximum number of entries per page
      static const unsigned maxCount = BufferManager::pageSize;
      /// A triple
      struct Triple {
         unsigned value1,value2,count;
      };

      /// The current page
      BufferReference current;
      /// The segment
      AggregatedFactsSegment* seg;
      /// The position on the current page
      const Triple* pos,*posLimit;
      /// The decompressed triples
      Triple triples[maxCount];
      /// The scan hint
      Hint* hint;

      Scan(const Scan&);
      void operator=(const Scan&);

      /// Perform a binary search
      bool find(unsigned value1,unsigned value2);
      /// Read the next page
      bool readNextPage();

      public:
      /// Constructor
      explicit Scan(Hint* hint=0);
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
