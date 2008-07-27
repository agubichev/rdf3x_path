#include "rts/segment/PathStatisticsSegment.hpp"
#include "rts/buffer/BufferManager.hpp"
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
