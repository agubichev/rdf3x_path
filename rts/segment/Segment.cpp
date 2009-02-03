#include "rts/segment/Segment.hpp"
#include "rts/buffer/BufferManager.hpp"
#include "rts/buffer/BufferReference.hpp"
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
Segment::Segment(BufferManager& bufferManager,Partition& partition)
   : bufferManager(bufferManager),partition(partition)
   // Constructor
{
}
//---------------------------------------------------------------------------
Segment::~Segment()
   // Destructor
{
}
//---------------------------------------------------------------------------
BufferRequest Segment::readShared(unsigned page) const
   // Read a specific page
{
   return BufferRequest(bufferManager,partition,page);
}
//---------------------------------------------------------------------------
BufferRequestExclusive Segment::readExclusive(unsigned page)
   // Read a specific page
{
   return BufferRequestExclusive(bufferManager,partition,page);
}
//---------------------------------------------------------------------------
void Segment::prefetchPages(unsigned start,unsigned stop)
   // Prefetch a range of patches
{
   bufferManager.prefetchPages(partition,start,stop);
}
//---------------------------------------------------------------------------
