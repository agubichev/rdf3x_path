#include "rts/segment/Segment.hpp"
#include "rts/buffer/BufferManager.hpp"
#include "rts/buffer/BufferReference.hpp"
#include "rts/database/DatabasePartition.hpp"
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
Segment::Segment(DatabasePartition& partition)
   : partition(partition),id(~0u)
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
   return partition.readShared(page);
}
//---------------------------------------------------------------------------
BufferRequestExclusive Segment::readExclusive(unsigned page)
   // Read a specific page
{
   return partition.readExclusive(page);
}
//---------------------------------------------------------------------------
BufferRequestModified Segment::modifyExclusive(unsigned page)
   // Read a specific page
{
   return partition.modifyExclusive(page);
}
//---------------------------------------------------------------------------
void Segment::writeUint32(unsigned char* data,unsigned value)
   // Helper function. Write a 32bit big-endian value
{
   data[0]=static_cast<unsigned char>(value>>24);
   data[1]=static_cast<unsigned char>(value>>16);
   data[2]=static_cast<unsigned char>(value>>8);
   data[3]=static_cast<unsigned char>(value>>0);
}
//---------------------------------------------------------------------------
