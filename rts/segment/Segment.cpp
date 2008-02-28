#include "rts/segment/Segment.hpp"
#include "rts/buffer/BufferManager.hpp"
//---------------------------------------------------------------------------
Segment::Segment(BufferManager& bufferManager)
   : bufferManager(bufferManager)
   // Constructor
{
}
//---------------------------------------------------------------------------
Segment::~Segment()
   // Destructor
{
}
//---------------------------------------------------------------------------
BufferRequest Segment::readShared(unsigned page)
   // Read a specific page
{
   return BufferRequest(bufferManager,page,true);
}
//---------------------------------------------------------------------------
BufferRequest Segment::readExclusive(unsigned page)
   // Read a specific page
{
   return BufferRequest(bufferManager,page,false);
}
//---------------------------------------------------------------------------
unsigned Segment::getPageId(const BufferReference& ref)
   /// Get the page ID
{
   return bufferManager.getPageId(ref);
}
//---------------------------------------------------------------------------
void Segment::prefetchPages(unsigned start,unsigned stop)
   // Prefetch a range of patches
{
   bufferManager.prefetchPages(start,stop);
}
//---------------------------------------------------------------------------
