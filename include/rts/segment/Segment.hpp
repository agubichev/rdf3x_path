#ifndef H_rts_segment_Segment
#define H_rts_segment_Segment
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
class BufferReference;
//---------------------------------------------------------------------------
/// Base class for all segments
class Segment
{
   private:
   /// The buffer manager
   BufferManager& bufferManager;

   protected:
   /// Constructor
   explicit Segment(BufferManager& bufferManager);

   /// Read a specific page
   BufferRequest readShared(unsigned page) const;
   /// Read a specific page
   BufferRequest readExclusive(unsigned page);
   /// Get the page ID
   unsigned getPageId(const BufferReference& ref);
   /// Prefetch a range of patches
   void prefetchPages(unsigned start,unsigned stop);

   /// Change the byte order
   static inline unsigned flipByteOrder(unsigned value) { return (value<<24)|((value&0xFF00)<<8)|((value&0xFF0000)>>8)|(value>>24); }

   public:
   /// Destructor
   virtual ~Segment();

   /// Helper function. Reads a 32bit big-endian value
   static inline unsigned readUint32(const unsigned char* data) { return (data[0]<<24)|(data[1]<<16)|(data[2]<<8)|data[3]; }
   /// Helper function. Reads a 32bit big-endian value that is guaranteed to be aligned. This assumes little-endian order! (needs a define for big-endian)
   static inline unsigned readUint32Aligned(const unsigned char* data) { return flipByteOrder(*reinterpret_cast<const unsigned*>(data)); }
};
//---------------------------------------------------------------------------
#endif
