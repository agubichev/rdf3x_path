#ifndef H_rts_segment_StatisticsSegment
#define H_rts_segment_StatisticsSegment
//---------------------------------------------------------------------------
#include "rts/segment/Segment.hpp"
#include "rts/buffer/BufferManager.hpp"
//---------------------------------------------------------------------------
/// Statistics about a certain ordering
class StatisticsSegment : public Segment
{
   public:
   /// A bucket entry
   struct Bucket {
      /// Start value
      unsigned start1,start2,start3;
      /// Stop value
      unsigned stop1,stop2,stop3;
      /// Cardinalities
      unsigned prefix1Card,prefix2Card,card;
      /// Number of join partners
      unsigned val1S,val1P,val1O,val2S,val2P,val2O,val3S,val3P,val3O;
   };

   private:
   /// The position of the statistics
   unsigned statisticsPage;

   StatisticsSegment(const StatisticsSegment&);
   void operator=(const StatisticsSegment&);

   public:
   /// Constructor
   StatisticsSegment(BufferManager& bufferManager,unsigned statisticsPage);

   /// Derive a bucket
   void lookup(Bucket& result);
   /// Derive a bucket
   void lookup(unsigned value1,Bucket& result);
   /// Derive a bucket
   void lookup(unsigned value1,unsigned value2,Bucket& result);
   /// Derive a bucket
   void lookup(unsigned value1,unsigned value2,unsigned value3,Bucket& result);
};
//---------------------------------------------------------------------------
#endif
