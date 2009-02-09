#ifndef H_rts_segment_Segment
#define H_rts_segment_Segment
//---------------------------------------------------------------------------
#include <stdint.h>
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
class BufferManager;
class BufferRequest;
class BufferRequestExclusive;
class BufferRequestModified;
class DatabasePartition;
class SpaceInventorySegment;
//---------------------------------------------------------------------------
/// Base class for all segments
class Segment
{
   public:
   /// Known segment types
   enum Type { Unused,SpaceInventorySegment, SegmentInventorySegment };

   private:
   /// The containing database partition
   DatabasePartition& partition;

   // Must access the underlying partition to grow it
   friend class SpaceInventorySegment;

   protected:
   /// Constructor
   explicit Segment(DatabasePartition& partition);

   /// Read a specific page
   BufferRequest readShared(unsigned page) const;
   /// Read a specific page
   BufferRequestExclusive readExclusive(unsigned page);
   /// Read a specific page
   BufferRequestModified modifyExclusive(unsigned page);

   /// Change the byte order
   static inline unsigned flipByteOrder(unsigned value) { return (value<<24)|((value&0xFF00)<<8)|((value&0xFF0000)>>8)|(value>>24); }

   public:
   /// Destructor
   virtual ~Segment();

   /// Convert to host order. This assumes little endinan-order! (needs a define for big endian)
   static inline unsigned toHost(unsigned value) { return flipByteOrder(value); }
   /// Convert to big endian. This assumes little endinan-order! (needs a define for big endian)
   static inline unsigned toBE(unsigned value) { return flipByteOrder(value); }
   /// Helper function. Reads a 32bit big-endian value
   static inline unsigned readUint32(const unsigned char* data) { return (data[0]<<24)|(data[1]<<16)|(data[2]<<8)|data[3]; }
   /// Helper function. Reads a 32bit big-endian value that is guaranteed to be aligned
   static inline unsigned readUint32Aligned(const unsigned char* data) { return toHost(*reinterpret_cast<const uint32_t*>(data)); }
   /// Helper function. Write a 32bit big-endian value
   static void writeUint32(unsigned char* data,unsigned value);
   /// Helper function. Writes a 32bit big-endian value that is guaranteed to be aligned
   static inline void writeUint32Aligned(unsigned char* data,unsigned value) { *reinterpret_cast<uint32_t*>(data)=toBE(value); }
};
//---------------------------------------------------------------------------
#endif
