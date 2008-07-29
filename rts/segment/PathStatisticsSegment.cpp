#include "rts/segment/PathStatisticsSegment.hpp"
#include "rts/buffer/BufferManager.hpp"
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
PathStatisticsSegment::PathStatisticsSegment(BufferManager& bufferManager,unsigned statisticsPage)
   : Segment(bufferManager),statisticsPage(statisticsPage)
   // Constructor
{
}
//---------------------------------------------------------------------------
bool PathStatisticsSegment::lookup(const std::vector<unsigned>& path,unsigned& frequency)
   // Lookup frequency (if known)
{
   BufferReference page(readShared(statisticsPage));
   const unsigned char* data=static_cast<const unsigned char*>(page.getPage());
   unsigned count=Segment::readUint32Aligned(data);
   data+=4;
   for (unsigned index=0;index<count;index++) {
      unsigned pathCount=Segment::readUint32Aligned(data);
      if (pathCount==path.size()) {
         bool match=true;
         for (unsigned index2=0;index2<pathCount;index2++)
            if (path[index2]!=Segment::readUint32Aligned(data+4+index2*4))
               { match=false; break; }
         if (match) {
            frequency=Segment::readUint32Aligned(data+4+pathCount*4);
            return true;
         }
      }
      data+=4+pathCount*4+4;
   }
   return false;
}
//---------------------------------------------------------------------------
