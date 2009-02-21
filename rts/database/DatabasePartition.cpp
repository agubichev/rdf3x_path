#include "rts/database/DatabasePartition.hpp"
#include "rts/buffer/BufferReference.hpp"
#include "rts/partition/Partition.hpp"
#include "rts/segment/SegmentInventorySegment.hpp"
#include "rts/segment/SpaceInventorySegment.hpp"
#include <cassert>
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
using namespace std;
//---------------------------------------------------------------------------
DatabasePartition::DatabasePartition(BufferManager& bufferManager,Partition& partition)
   : bufferManager(bufferManager),partition(partition)
   // Constructor
{
}
//---------------------------------------------------------------------------
DatabasePartition::~DatabasePartition()
   // Destructor
{
   for (vector<Segment*>::iterator iter=segments.begin(),limit=segments.end();iter!=limit;++iter)
      delete (*iter);
}
//---------------------------------------------------------------------------
void DatabasePartition::create()
   // Initialize a new partition with a space inventory and a segment inventory
{
   assert(segments.empty());

   // Ensure a sufficient partition size
   if (partition.getSize()<(SegmentInventorySegment::root+1)) {
      unsigned start,len;
      if (!partition.grow((SegmentInventorySegment::root+1)-partition.getSize(),start,len))
	 assert(false&&"increasing the partition failed");
   }

   // Create a space inventory
   SpaceInventorySegment* spaceInv=new SpaceInventorySegment(*this);
   spaceInv->id=0;
   spaceInv->insertInterval(0,SpaceInventorySegment::root,SpaceInventorySegment::root);
   segments.push_back(spaceInv);

   // Create a segment inventory
   SegmentInventorySegment* segInv=new SegmentInventorySegment(*this);
   segInv->id=1;
   spaceInv->insertInterval(1,SegmentInventorySegment::root,SegmentInventorySegment::root);
   segInv->addSegment(Segment::SpaceInventorySegment,Tag_SpaceInventory);
   segInv->addSegment(Segment::SegmentInventorySegment,Tag_SegmentInventory);
   segments.push_back(segInv);
}
//---------------------------------------------------------------------------
void DatabasePartition::open()
   // Open an existing partition, reconstruct segments as required
{
   assert(segments.empty());

   // Retrieve all segment types
   vector<Segment::Type> segmentTypes;
   SegmentInventorySegment::openPartition(*this,segmentTypes);

   // Reconstruct segments
   unsigned id=0;
   for (vector<Segment::Type>::const_iterator iter=segmentTypes.begin(),limit=segmentTypes.end();iter!=limit;++iter) {
      Segment* seg=0;
      switch (*iter) {
	 case Segment::Unused: segments.push_back(0); continue;
	 case Segment::SpaceInventorySegment: seg=new SpaceInventorySegment(*this); break;
	 case Segment::SegmentInventorySegment: seg=new SegmentInventorySegment(*this); break;
      }
      assert(seg);
      seg->id=id++;
      segments.push_back(seg);
   }

   // Refresh the stored info
   for (vector<Segment*>::const_iterator iter=segments.begin(),limit=segments.end();iter!=limit;++iter)
      (*iter)->refreshInfo();
}
//---------------------------------------------------------------------------
BufferRequest DatabasePartition::readShared(unsigned page) const
   // Read a specific page
{
   return BufferRequest(bufferManager,partition,page);
}
//---------------------------------------------------------------------------
BufferRequestExclusive DatabasePartition::readExclusive(unsigned page)
   // Read a specific page
{
   return BufferRequestExclusive(bufferManager,partition,page);
}
//---------------------------------------------------------------------------
BufferRequestModified DatabasePartition::modifyExclusive(unsigned page)
   // Read a specific page
{
   return BufferRequestModified(bufferManager,partition,page);
}
//---------------------------------------------------------------------------
SpaceInventorySegment* DatabasePartition::getSpaceInventory()
   // Get the space inventory
{
   return static_cast<SpaceInventorySegment*>(segments[1]);
}
//---------------------------------------------------------------------------
SegmentInventorySegment* DatabasePartition::getSegmentInventory()
   // Get the segment inventory
{
   return static_cast<SegmentInventorySegment*>(segments[1]);
}
//---------------------------------------------------------------------------
