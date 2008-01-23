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
AggregatedFactsSegment::AggregatedFactsSegment(BufferManager& bufferManager,unsigned tableStart,unsigned indexRoot,unsigned pages,unsigned groups1,unsigned groups2)
   : Segment(bufferManager),tableStart(tableStart),indexRoot(indexRoot),
     pages(pages),groups1(groups1),groups2(groups2)
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
   pos=posLimit=0;

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
   pos=posLimit=0;

   // Skip over leading entries that are too small
   while (true) {
      if (!next())
         return false;

      if ((getValue1()>start1)||((getValue1()==start1)&&(getValue2()>=start2)))
         return true;
   }
}
//---------------------------------------------------------------------------
static inline unsigned readDelta1(const unsigned char* pos) { return pos[0]; }
static unsigned readDelta2(const unsigned char* pos) { return (pos[0]<<8)|pos[1]; }
static unsigned readDelta3(const unsigned char* pos) { return (pos[0]<<16)|(pos[1]<<8)|pos[2]; }
static unsigned readDelta4(const unsigned char* pos) { return (pos[0]<<24)|(pos[1]<<16)|(pos[2]<<8)|pos[3]; }
//---------------------------------------------------------------------------
bool AggregatedFactsSegment::Scan::readNextPage()
   // Read the next page
{
   // Alread read the first page? Then read the next one
   if (pos-1) {
      const unsigned char* page=static_cast<const unsigned char*>(current.getPage());
      unsigned nextPage=readUint32Aligned(page);
      if (!nextPage)
         return false;
      current=seg->readShared(nextPage);
   }

   // Decompress the first triple
   const unsigned char* page=static_cast<const unsigned char*>(current.getPage());
   const unsigned char* reader=page+headerSize,*limit=page+BufferManager::pageSize;
   unsigned value1=readUint32Aligned(reader); reader+=4;
   unsigned value2=readUint32Aligned(reader); reader+=4;
   unsigned count=readUint32Aligned(reader); reader+=4;
   Tripple* writer=tripples;
   (*writer).value1=value1;
   (*writer).value2=value2;
   (*writer).count=count;
   ++writer;

   // Decompress the remainder of the page
   while (reader<limit) {
      // Decode the header byte
      unsigned info=*(reader++);
      // Small gap only?
      if (info<0x80) {
         if (!info)
            break;
         count=(info>>5)+1;
         value2+=(info&31);
         (*writer).value1=value1;
         (*writer).value2=value2;
         (*writer).count=count;
         ++writer;
         continue;
      }
      // Decode the parts
      switch (info&127) {
         case 0: count=1; break;
         case 1: count=readDelta1(reader); reader+=1; break;
         case 2: count=readDelta2(reader); reader+=2; break;
         case 3: count=readDelta3(reader); reader+=3; break;
         case 4: count=readDelta4(reader); reader+=4; break;
         case 5: value2=readDelta1(reader); count=1; reader+=1; break;
         case 6: value2=readDelta1(reader); count=readDelta1(reader+1)+1; reader+=2; break;
         case 7: value2=readDelta1(reader); count=readDelta2(reader+1)+1; reader+=3; break;
         case 8: value2=readDelta1(reader); count=readDelta3(reader+1)+1; reader+=4; break;
         case 9: value2=readDelta1(reader); count=readDelta4(reader+1)+1; reader+=5; break;
         case 10: value2=readDelta2(reader); count=1; reader+=2; break;
         case 11: value2=readDelta2(reader); count=readDelta1(reader+2)+1; reader+=3; break;
         case 12: value2=readDelta2(reader); count=readDelta2(reader+2)+1; reader+=4; break;
         case 13: value2=readDelta2(reader); count=readDelta3(reader+2)+1; reader+=5; break;
         case 14: value2=readDelta2(reader); count=readDelta4(reader+2)+1; reader+=6; break;
         case 15: value2=readDelta3(reader); count=1; reader+=3; break;
         case 16: value2=readDelta3(reader); count=readDelta1(reader+3)+1; reader+=4; break;
         case 17: value2=readDelta3(reader); count=readDelta2(reader+3)+1; reader+=5; break;
         case 18: value2=readDelta3(reader); count=readDelta3(reader+3)+1; reader+=6; break;
         case 19: value2=readDelta3(reader); count=readDelta4(reader+3)+1; reader+=7; break;
         case 20: value2=readDelta4(reader); count=1; reader+=4; break;
         case 21: value2=readDelta4(reader); count=readDelta1(reader+4)+1; reader+=5; break;
         case 22: value2=readDelta4(reader); count=readDelta2(reader+4)+1; reader+=6; break;
         case 23: value2=readDelta4(reader); count=readDelta3(reader+4)+1; reader+=7; break;
         case 24: value2=readDelta4(reader); count=readDelta4(reader+4)+1; reader+=8; break;
         case 25: value1+=readDelta1(reader); value2=0; count=1; reader+=1; break;
         case 26: value1+=readDelta1(reader); value2=0; count=readDelta1(reader+1)+1; reader+=2; break;
         case 27: value1+=readDelta1(reader); value2=0; count=readDelta2(reader+1)+1; reader+=3; break;
         case 28: value1+=readDelta1(reader); value2=0; count=readDelta3(reader+1)+1; reader+=4; break;
         case 29: value1+=readDelta1(reader); value2=0; count=readDelta4(reader+1)+1; reader+=5; break;
         case 30: value1+=readDelta1(reader); value2=readDelta1(reader+1); count=1; reader+=2; break;
         case 31: value1+=readDelta1(reader); value2=readDelta1(reader+1); count=readDelta1(reader+2)+1; reader+=3; break;
         case 32: value1+=readDelta1(reader); value2=readDelta1(reader+1); count=readDelta2(reader+2)+1; reader+=4; break;
         case 33: value1+=readDelta1(reader); value2=readDelta1(reader+1); count=readDelta3(reader+2)+1; reader+=5; break;
         case 34: value1+=readDelta1(reader); value2=readDelta1(reader+1); count=readDelta4(reader+2)+1; reader+=6; break;
         case 35: value1+=readDelta1(reader); value2=readDelta2(reader+1); count=1; reader+=3; break;
         case 36: value1+=readDelta1(reader); value2=readDelta2(reader+1); count=readDelta1(reader+3)+1; reader+=4; break;
         case 37: value1+=readDelta1(reader); value2=readDelta2(reader+1); count=readDelta2(reader+3)+1; reader+=5; break;
         case 38: value1+=readDelta1(reader); value2=readDelta2(reader+1); count=readDelta3(reader+3)+1; reader+=6; break;
         case 39: value1+=readDelta1(reader); value2=readDelta2(reader+1); count=readDelta4(reader+3)+1; reader+=7; break;
         case 40: value1+=readDelta1(reader); value2=readDelta3(reader+1); count=1; reader+=4; break;
         case 41: value1+=readDelta1(reader); value2=readDelta3(reader+1); count=readDelta1(reader+4)+1; reader+=5; break;
         case 42: value1+=readDelta1(reader); value2=readDelta3(reader+1); count=readDelta2(reader+4)+1; reader+=6; break;
         case 43: value1+=readDelta1(reader); value2=readDelta3(reader+1); count=readDelta3(reader+4)+1; reader+=7; break;
         case 44: value1+=readDelta1(reader); value2=readDelta3(reader+1); count=readDelta4(reader+4)+1; reader+=8; break;
         case 45: value1+=readDelta1(reader); value2=readDelta4(reader+1); count=1; reader+=5; break;
         case 46: value1+=readDelta1(reader); value2=readDelta4(reader+1); count=readDelta1(reader+5)+1; reader+=6; break;
         case 47: value1+=readDelta1(reader); value2=readDelta4(reader+1); count=readDelta2(reader+5)+1; reader+=7; break;
         case 48: value1+=readDelta1(reader); value2=readDelta4(reader+1); count=readDelta3(reader+5)+1; reader+=8; break;
         case 49: value1+=readDelta1(reader); value2=readDelta4(reader+1); count=readDelta4(reader+5)+1; reader+=9; break;
         case 50: value1+=readDelta2(reader); value2=0; count=1; reader+=2; break;
         case 51: value1+=readDelta2(reader); value2=0; count=readDelta1(reader+2)+1; reader+=3; break;
         case 52: value1+=readDelta2(reader); value2=0; count=readDelta2(reader+2)+1; reader+=4; break;
         case 53: value1+=readDelta2(reader); value2=0; count=readDelta3(reader+2)+1; reader+=5; break;
         case 54: value1+=readDelta2(reader); value2=0; count=readDelta4(reader+2)+1; reader+=6; break;
         case 55: value1+=readDelta2(reader); value2=readDelta1(reader+2); count=1; reader+=3; break;
         case 56: value1+=readDelta2(reader); value2=readDelta1(reader+2); count=readDelta1(reader+3)+1; reader+=4; break;
         case 57: value1+=readDelta2(reader); value2=readDelta1(reader+2); count=readDelta2(reader+3)+1; reader+=5; break;
         case 58: value1+=readDelta2(reader); value2=readDelta1(reader+2); count=readDelta3(reader+3)+1; reader+=6; break;
         case 59: value1+=readDelta2(reader); value2=readDelta1(reader+2); count=readDelta4(reader+3)+1; reader+=7; break;
         case 60: value1+=readDelta2(reader); value2=readDelta2(reader+2); count=1; reader+=4; break;
         case 61: value1+=readDelta2(reader); value2=readDelta2(reader+2); count=readDelta1(reader+4)+1; reader+=5; break;
         case 62: value1+=readDelta2(reader); value2=readDelta2(reader+2); count=readDelta2(reader+4)+1; reader+=6; break;
         case 63: value1+=readDelta2(reader); value2=readDelta2(reader+2); count=readDelta3(reader+4)+1; reader+=7; break;
         case 64: value1+=readDelta2(reader); value2=readDelta2(reader+2); count=readDelta4(reader+4)+1; reader+=8; break;
         case 65: value1+=readDelta2(reader); value2=readDelta3(reader+2); count=1; reader+=5; break;
         case 66: value1+=readDelta2(reader); value2=readDelta3(reader+2); count=readDelta1(reader+5)+1; reader+=6; break;
         case 67: value1+=readDelta2(reader); value2=readDelta3(reader+2); count=readDelta2(reader+5)+1; reader+=7; break;
         case 68: value1+=readDelta2(reader); value2=readDelta3(reader+2); count=readDelta3(reader+5)+1; reader+=8; break;
         case 69: value1+=readDelta2(reader); value2=readDelta3(reader+2); count=readDelta4(reader+5)+1; reader+=9; break;
         case 70: value1+=readDelta2(reader); value2=readDelta4(reader+2); count=1; reader+=6; break;
         case 71: value1+=readDelta2(reader); value2=readDelta4(reader+2); count=readDelta1(reader+6)+1; reader+=7; break;
         case 72: value1+=readDelta2(reader); value2=readDelta4(reader+2); count=readDelta2(reader+6)+1; reader+=8; break;
         case 73: value1+=readDelta2(reader); value2=readDelta4(reader+2); count=readDelta3(reader+6)+1; reader+=9; break;
         case 74: value1+=readDelta2(reader); value2=readDelta4(reader+2); count=readDelta4(reader+6)+1; reader+=10; break;
         case 75: value1+=readDelta3(reader); value2=0; count=1; reader+=3; break;
         case 76: value1+=readDelta3(reader); value2=0; count=readDelta1(reader+3)+1; reader+=4; break;
         case 77: value1+=readDelta3(reader); value2=0; count=readDelta2(reader+3)+1; reader+=5; break;
         case 78: value1+=readDelta3(reader); value2=0; count=readDelta3(reader+3)+1; reader+=6; break;
         case 79: value1+=readDelta3(reader); value2=0; count=readDelta4(reader+3)+1; reader+=7; break;
         case 80: value1+=readDelta3(reader); value2=readDelta1(reader+3); count=1; reader+=4; break;
         case 81: value1+=readDelta3(reader); value2=readDelta1(reader+3); count=readDelta1(reader+4)+1; reader+=5; break;
         case 82: value1+=readDelta3(reader); value2=readDelta1(reader+3); count=readDelta2(reader+4)+1; reader+=6; break;
         case 83: value1+=readDelta3(reader); value2=readDelta1(reader+3); count=readDelta3(reader+4)+1; reader+=7; break;
         case 84: value1+=readDelta3(reader); value2=readDelta1(reader+3); count=readDelta4(reader+4)+1; reader+=8; break;
         case 85: value1+=readDelta3(reader); value2=readDelta2(reader+3); count=1; reader+=5; break;
         case 86: value1+=readDelta3(reader); value2=readDelta2(reader+3); count=readDelta1(reader+5)+1; reader+=6; break;
         case 87: value1+=readDelta3(reader); value2=readDelta2(reader+3); count=readDelta2(reader+5)+1; reader+=7; break;
         case 88: value1+=readDelta3(reader); value2=readDelta2(reader+3); count=readDelta3(reader+5)+1; reader+=8; break;
         case 89: value1+=readDelta3(reader); value2=readDelta2(reader+3); count=readDelta4(reader+5)+1; reader+=9; break;
         case 90: value1+=readDelta3(reader); value2=readDelta3(reader+3); count=1; reader+=6; break;
         case 91: value1+=readDelta3(reader); value2=readDelta3(reader+3); count=readDelta1(reader+6)+1; reader+=7; break;
         case 92: value1+=readDelta3(reader); value2=readDelta3(reader+3); count=readDelta2(reader+6)+1; reader+=8; break;
         case 93: value1+=readDelta3(reader); value2=readDelta3(reader+3); count=readDelta3(reader+6)+1; reader+=9; break;
         case 94: value1+=readDelta3(reader); value2=readDelta3(reader+3); count=readDelta4(reader+6)+1; reader+=10; break;
         case 95: value1+=readDelta3(reader); value2=readDelta4(reader+3); count=1; reader+=7; break;
         case 96: value1+=readDelta3(reader); value2=readDelta4(reader+3); count=readDelta1(reader+7)+1; reader+=8; break;
         case 97: value1+=readDelta3(reader); value2=readDelta4(reader+3); count=readDelta2(reader+7)+1; reader+=9; break;
         case 98: value1+=readDelta3(reader); value2=readDelta4(reader+3); count=readDelta3(reader+7)+1; reader+=10; break;
         case 99: value1+=readDelta3(reader); value2=readDelta4(reader+3); count=readDelta4(reader+7)+1; reader+=11; break;
         case 100: value1+=readDelta4(reader); value2=0; count=1; reader+=4; break;
         case 101: value1+=readDelta4(reader); value2=0; count=readDelta1(reader+4)+1; reader+=5; break;
         case 102: value1+=readDelta4(reader); value2=0; count=readDelta2(reader+4)+1; reader+=6; break;
         case 103: value1+=readDelta4(reader); value2=0; count=readDelta3(reader+4)+1; reader+=7; break;
         case 104: value1+=readDelta4(reader); value2=0; count=readDelta4(reader+4)+1; reader+=8; break;
         case 105: value1+=readDelta4(reader); value2=readDelta1(reader+4); count=1; reader+=5; break;
         case 106: value1+=readDelta4(reader); value2=readDelta1(reader+4); count=readDelta1(reader+5)+1; reader+=6; break;
         case 107: value1+=readDelta4(reader); value2=readDelta1(reader+4); count=readDelta2(reader+5)+1; reader+=7; break;
         case 108: value1+=readDelta4(reader); value2=readDelta1(reader+4); count=readDelta3(reader+5)+1; reader+=8; break;
         case 109: value1+=readDelta4(reader); value2=readDelta1(reader+4); count=readDelta4(reader+5)+1; reader+=9; break;
         case 110: value1+=readDelta4(reader); value2=readDelta2(reader+4); count=1; reader+=6; break;
         case 111: value1+=readDelta4(reader); value2=readDelta2(reader+4); count=readDelta1(reader+6)+1; reader+=7; break;
         case 112: value1+=readDelta4(reader); value2=readDelta2(reader+4); count=readDelta2(reader+6)+1; reader+=8; break;
         case 113: value1+=readDelta4(reader); value2=readDelta2(reader+4); count=readDelta3(reader+6)+1; reader+=9; break;
         case 114: value1+=readDelta4(reader); value2=readDelta2(reader+4); count=readDelta4(reader+6)+1; reader+=10; break;
         case 115: value1+=readDelta4(reader); value2=readDelta3(reader+4); count=1; reader+=7; break;
         case 116: value1+=readDelta4(reader); value2=readDelta3(reader+4); count=readDelta1(reader+7)+1; reader+=8; break;
         case 117: value1+=readDelta4(reader); value2=readDelta3(reader+4); count=readDelta2(reader+7)+1; reader+=9; break;
         case 118: value1+=readDelta4(reader); value2=readDelta3(reader+4); count=readDelta3(reader+7)+1; reader+=10; break;
         case 119: value1+=readDelta4(reader); value2=readDelta3(reader+4); count=readDelta4(reader+7)+1; reader+=11; break;
         case 120: value1+=readDelta4(reader); value2=readDelta4(reader+4); count=1; reader+=8; break;
         case 121: value1+=readDelta4(reader); value2=readDelta4(reader+4); count=readDelta1(reader+8)+1; reader+=9; break;
         case 122: value1+=readDelta4(reader); value2=readDelta4(reader+4); count=readDelta2(reader+8)+1; reader+=10; break;
         case 123: value1+=readDelta4(reader); value2=readDelta4(reader+4); count=readDelta3(reader+8)+1; reader+=11; break;
         case 124: value1+=readDelta4(reader); value2=readDelta4(reader+4); count=readDelta4(reader+8)+1; reader+=12; break;
      }
      (*writer).value1=value1;
      (*writer).value2=value2;
      (*writer).count=count;
      ++writer;
   }

   // Update the entries
   pos=tripples;
   posLimit=writer;

   return true;
}
//---------------------------------------------------------------------------
void AggregatedFactsSegment::Scan::close()
   // Close the scan
{
   seg=0;
   current.reset();
}
//---------------------------------------------------------------------------
