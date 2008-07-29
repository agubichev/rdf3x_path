#ifndef H_rts_segment_PathStatisticsSegment
#define H_rts_segment_PathStatisticsSegment
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
