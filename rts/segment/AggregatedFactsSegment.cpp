#include "rts/segment/AggregatedFactsSegment.hpp"
#include "rts/buffer/BufferManager.hpp"
//---------------------------------------------------------------------------
/// The size of the header on each fact page
static const unsigned headerSize = 4;
//---------------------------------------------------------------------------
/// Helper functions
static inline unsigned readInner1(const unsigned char* page,unsigned slot) { return Segment::readUint32Aligned(page+16+12*slot); }
static inline unsigned readInner2(const unsigned char* page,unsigned slot) { return Segment::readUint32Aligned(page+16+12*slot+4); }
static inline unsigned readInnerPage(const unsigned char* page,unsigned slot) { return Segment::readUint32Aligned(page+16+12*slot+8); }
//---------------------------------------------------------------------------
/// Compare
static inline bool greater(unsigned a1,unsigned a2,unsigned b1,unsigned b2) {
   return (a1>b1)||((a1==b1)&&(a2>b2));
}
//---------------------------------------------------------------------------
AggregatedFactsSegment::AggregatedFactsSegment(BufferManager& bufferManager,unsigned tableStart,unsigned indexRoot)
   : Segment(bufferManager),tableStart(tableStart),indexRoot(indexRoot)
   // Constructor
{
}
//---------------------------------------------------------------------------
bool AggregatedFactsSegment::lookup(unsigned start1,unsigned start2,BufferReference& ref)
   // Lookup the first page contains entries >= the start condition
{
   // Traverse the B-Tree
   ref=readShared(indexRoot);
   while (true) {
      const unsigned char* page=static_cast<const unsigned char*>(ref.getPage());
      // Inner node?
      if (readUint32Aligned(page)==0xFFFFFFFF) {
         // Perform a binary search. The test is more complex as we only have the upper bound for ranges
         unsigned left=0,right=readUint32Aligned(page+8);
         while (left!=right) {
            unsigned middle=(left+right)/2;
            unsigned middle1=readInner1(page,middle),middle2=readInner2(page,middle);
            if (greater(start1,start2,middle1,middle2)) {
               left=middle+1;
            } else if ((!middle)||(greater(start1,start2,readInner1(page,middle-1),readInner2(page,middle-1)))) {
               ref=readShared(readInnerPage(page,middle));
               break;
            } else {
               right=middle;
            }
         }
         // Unsuccessful search?
         if (left==right) {
            ref.reset();
            return false;
         }
      } else {
         // A leaf node. Stop here, the exact entry is found by the Scan
         return true;
      }
   }
}
//---------------------------------------------------------------------------
AggregatedFactsSegment::Scan::Scan()
   : seg(0)
   // Constructor
{
}
//---------------------------------------------------------------------------
AggregatedFactsSegment::Scan::~Scan()
   // Destructor
{
   close();
}
//---------------------------------------------------------------------------
bool AggregatedFactsSegment::Scan::first(AggregatedFactsSegment& segment)
   // Start a new scan over the whole segment
{
   current=segment.readShared(segment.tableStart);
   seg=&segment;
   pos=headerSize;

   return next();
}
//---------------------------------------------------------------------------
bool AggregatedFactsSegment::Scan::first(AggregatedFactsSegment& segment,unsigned start1,unsigned start2)
   // Start a new scan starting from the first entry >= the start condition
{
   // Lookup the right page
   if (!segment.lookup(start1,start2,current))
      return false;

   // Place the iterator
   seg=&segment;
   pos=headerSize;

   // Skip over leading entries that are too small
   while (true) {
      if (!next())
         return false;

      if ((value1>start1)||((value1==start1)&&(value2>=start2)))
         return true;
   }
}
//---------------------------------------------------------------------------
static unsigned readDelta(const unsigned char* pos,unsigned size)
   // Read an delta encoded value
{
   switch (size) {
      case 0: return 0;
      case 1: return pos[0];
      case 2: return (pos[0]<<8)|pos[1];
      case 3: return (pos[0]<<16)|(pos[1]<<8)|pos[2];
      case 4: return (pos[0]<<24)|(pos[1]<<16)|(pos[2]<<8)|pos[3];
      default: return 0;
   }
}
//---------------------------------------------------------------------------
bool AggregatedFactsSegment::Scan::next()
   // Read the next entry
{
   while (true) {
      const unsigned char* page=static_cast<const unsigned char*>(current.getPage());

      // End of page?
      if (pos>=BufferManager::pageSize) {
         nextPage:
         unsigned nextPage=readUint32Aligned(page);
         if (!nextPage)
            return false;
         current=seg->readShared(nextPage);
         pos=headerSize;
         continue;
      }
      // First entry on the page?
      if (pos==headerSize) {
         value1=readUint32Aligned(page+pos); pos+=4;
         value2=readUint32Aligned(page+pos); pos+=4;
         count=readUint32Aligned(page+pos); pos+=4;
         return true;
      }
      // No, decode it
      unsigned info=page[pos++];
      // Small gap only?
      if (info<0x80) {
         if (!info)
            goto nextPage;
         count=(info>>5)+1;
         value2+=(info&31);
         return true;
      }
      // Last value changed only?
      if (info<0x90) {
         unsigned size2=((info>>2)&3)+1;
         value2+=readDelta(page+pos,size2);
         pos+=size2;
         unsigned sizeCount=(info&3)+1;
         count=readDelta(page+pos,sizeCount);
         pos+=sizeCount;
         return true;
      }
      // Both values changed
      unsigned size1=(info>>4)&7;
      value1+=readDelta(page+pos,size1);
      pos+=size1;
      unsigned size2=((info>>2)&4)+1;
      value2=readDelta(page+pos,size2);
      pos+=size2;
      unsigned sizeCount=(info&3)+1;
      count=readDelta(page+pos,sizeCount);
      pos+=sizeCount;
      return true;
   }
}
//---------------------------------------------------------------------------
void AggregatedFactsSegment::Scan::close()
   // Close the scan
{
   seg=0;
   current.reset();
}
//---------------------------------------------------------------------------
