#ifndef H_rts_segment_PathStatisticsSegment
#define H_rts_segment_PathStatisticsSegment
//---------------------------------------------------------------------------
#include "rts/segment/Segment.hpp"
#include <vector>
//---------------------------------------------------------------------------
class BufferManager;
//---------------------------------------------------------------------------
/// Statistics about frequent paths
class PathStatisticsSegment : public Segment
{
   private:
   /// The position of the statistics
   unsigned statisticsPage;

   PathStatisticsSegment(const PathStatisticsSegment&);
   void operator=(const PathStatisticsSegment&);

   public:
   /// Constructor
   PathStatisticsSegment(BufferManager& bufferManager,unsigned statisticsPage);

   /// Lookup frequency (if known)
   bool lookup(const std::vector<unsigned>& path,unsigned& count);
};
//---------------------------------------------------------------------------
#endif
