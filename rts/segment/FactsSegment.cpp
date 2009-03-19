#include "rts/segment/FactsSegment.hpp"
#include "rts/database/DatabaseBuilder.hpp"
#include "rts/transaction/LogAction.hpp"
#include <vector>
#include <cstring>
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
/// The size of the header on each fact page
static const unsigned headerSize = 12; // LSN + next pointer
/// The size of the header on an inner page
static const unsigned headerSizeInner = 24;
/// The size of an entry on a inner page
static const unsigned entrySizeInner = 16;
/// Maximum number of entries on a inner page
static const unsigned maxEntriesOnInner = (BufferReference::pageSize-headerSizeInner)/entrySizeInner;
//---------------------------------------------------------------------------
// Info slots
static const unsigned slotTableStart = 0;
static const unsigned slotIndexRoot = 1;
static const unsigned slotPages = 2;
static const unsigned slotGroups1 = 3;
static const unsigned slotGroups2 = 4;
static const unsigned slotCardinality = 5;
//---------------------------------------------------------------------------
/// Helper functions
static inline unsigned readInnerCount(const unsigned char* page) { return Segment::readUint32Aligned(page+16); }
static inline const unsigned char* readInnerPtr(const unsigned char* page,unsigned slot) { return page+headerSizeInner+entrySizeInner*slot; }
static inline unsigned readInner1(const unsigned char* page,unsigned slot) { return Segment::readUint32Aligned(page+headerSizeInner+entrySizeInner*slot); }
static inline unsigned readInner2(const unsigned char* page,unsigned slot) { return Segment::readUint32Aligned(page+headerSizeInner+entrySizeInner*slot+4); }
static inline unsigned readInner3(const unsigned char* page,unsigned slot) { return Segment::readUint32Aligned(page+headerSizeInner+entrySizeInner*slot+8); }
static inline unsigned readInnerPage(const unsigned char* page,unsigned slot) { return Segment::readUint32Aligned(page+headerSizeInner+entrySizeInner*slot+12); }
//---------------------------------------------------------------------------
/// Compare
static inline bool greater(unsigned a1,unsigned a2,unsigned a3,unsigned b1,unsigned b2,unsigned b3) {
   return (a1>b1)||
          ((a1==b1)&&((a2>b2)||
                      ((a2==b2)&&(a3>b3))));
}
//---------------------------------------------------------------------------
FactsSegment::Source::~Source()
   // Destructor
{
}
//---------------------------------------------------------------------------
FactsSegment::FactsSegment(DatabasePartition& partition)
   : Segment(partition),tableStart(0),indexRoot(0),pages(0),groups1(0),groups2(0),cardinality(0)
   // Constructor
{
}
//---------------------------------------------------------------------------
Segment::Type FactsSegment::getType() const
   // Get the type
{
   return Segment::Type_Facts;
}
//---------------------------------------------------------------------------
void FactsSegment::refreshInfo()
   // Refresh segment info stored in the partition
{
   tableStart=getSegmentData(slotTableStart);
   indexRoot=getSegmentData(slotIndexRoot);
   pages=getSegmentData(slotPages);
   groups1=getSegmentData(slotGroups1);
   groups2=getSegmentData(slotGroups2);
   cardinality=getSegmentData(slotCardinality);
}
//---------------------------------------------------------------------------
bool FactsSegment::lookup(unsigned start1,unsigned start2,unsigned start3,BufferReference& ref)
   // Lookup the first page contains entries >= the start condition
{
   // Traverse the B-Tree
   ref=readShared(indexRoot);
   while (true) {
      const unsigned char* page=static_cast<const unsigned char*>(ref.getPage());
      // Inner node?
      if (readUint32Aligned(page+8)==0xFFFFFFFF) {
         // Perform a binary search. The test is more complex as we only have the upper bound for ranges
         unsigned left=0,right=readInnerCount(page);
         while (left!=right) {
            unsigned middle=(left+right)/2;
            unsigned middle1=readInner1(page,middle),middle2=readInner2(page,middle),middle3=readInner3(page,middle);
            if (greater(start1,start2,start3,middle1,middle2,middle3)) {
               left=middle+1;
            } else if ((!middle)||(greater(start1,start2,start3,readInner1(page,middle-1),readInner2(page,middle-1),readInner3(page,middle-1)))) {
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
static inline unsigned readDelta1(const unsigned char* pos) { return pos[0]; }
static unsigned readDelta2(const unsigned char* pos) { return (pos[0]<<8)|pos[1]; }
static unsigned readDelta3(const unsigned char* pos) { return (pos[0]<<16)|(pos[1]<<8)|pos[2]; }
static unsigned readDelta4(const unsigned char* pos) { return (pos[0]<<24)|(pos[1]<<16)|(pos[2]<<8)|pos[3]; }
//---------------------------------------------------------------------------
static bool skipInTime(const unsigned char*& oreader,const unsigned char* limit,unsigned& v1,unsigned& v2,unsigned& v3,unsigned& oc,unsigned& od,unsigned& olc,unsigned& old,unsigned time)
   // Skip
{
   const unsigned char* reader=oreader;
   unsigned value1=v1,value2=v2,value3=v3;
   unsigned created=oc,deleted=od;
   unsigned lc=olc,ld=old;
   bool produce=false;

   // Decompress the remainder of the page
   while (reader<limit) {
      // Decode the header byte
      unsigned info=*(reader++);
      // Small gap only?
      if (info<0x80) {
         if (!info)
            break;
         value3+=info;
         continue;
      }
      // Decode it
      switch (info&127) {
         case 0: value3+=readDelta1(reader)+128; reader+=1; break;
         case 1: value3+=readDelta2(reader)+128; reader+=2; break;
         case 2: value3+=readDelta3(reader)+128; reader+=3; break;
         case 3: value3+=readDelta4(reader)+128; reader+=4; break;
         case 4: value2+=readDelta1(reader); value3=readDelta1(reader+1); reader+=2; break;
         case 5: value2+=readDelta1(reader); value3=readDelta2(reader+1); reader+=3; break;
         case 6: value2+=readDelta1(reader); value3=readDelta3(reader+1); reader+=4; break;
         case 7: value2+=readDelta1(reader); value3=readDelta4(reader+1); reader+=5; break;
         case 8: value2+=readDelta2(reader); value3=readDelta1(reader+2); reader+=3; break;
         case 9: value2+=readDelta2(reader); value3=readDelta2(reader+2); reader+=4; break;
         case 10: value2+=readDelta2(reader); value3=readDelta3(reader+2); reader+=5; break;
         case 11: value2+=readDelta2(reader); value3=readDelta4(reader+2); reader+=6; break;
         case 12: value2+=readDelta3(reader); value3=readDelta1(reader+3); reader+=4; break;
         case 13: value2+=readDelta3(reader); value3=readDelta2(reader+3); reader+=5; break;
         case 14: value2+=readDelta3(reader); value3=readDelta3(reader+3); reader+=6; break;
         case 15: value2+=readDelta3(reader); value3=readDelta4(reader+3); reader+=7; break;
         case 16: value2+=readDelta4(reader); value3=readDelta1(reader+4); reader+=5; break;
         case 17: value2+=readDelta4(reader); value3=readDelta2(reader+4); reader+=6; break;
         case 18: value2+=readDelta4(reader); value3=readDelta3(reader+4); reader+=7; break;
         case 19: value2+=readDelta4(reader); value3=readDelta4(reader+4); reader+=8; break;
         case 20: lc=created; created=Segment::readUint32(reader); reader+=4; if ((created>time)||(time>=deleted)) continue; else goto done;
         case 21: lc=created; created=Segment::readUint32(reader); reader+=4; if ((created>time)||(time>=deleted)) continue; else { produce=true; goto done; }
         case 22: swap(created,lc); if ((created>time)||(time>=deleted)) continue; else goto done;
         case 23: swap(created,lc); if ((created>time)||(time>=deleted)) continue; else { produce=true; goto done; }
         case 24: ld=deleted; deleted=Segment::readUint32(reader); reader+=4; if ((created>time)||(time>=deleted)) continue; else goto done;
         case 25: ld=deleted; deleted=Segment::readUint32(reader); reader+=4; if ((created>time)||(time>=deleted)) continue; else { produce=true; goto done; }
         case 26: swap(deleted,ld); if ((created>time)||(time>=deleted)) continue; else goto done;
         case 27: swap(deleted,ld); if ((created>time)||(time>=deleted)) continue; else { produce=true; goto done; }
         case 28: case 29: case 30: case 31: break;
         case 32: case 33: case 34: case 35: case 36: case 37: case 38: case 39: case 40: case 41: case 42: case 43:
         case 44: case 45: case 46: case 47: case 48: case 49: case 50: case 51: case 52: case 53: case 54: case 55:
         case 56: case 57: case 58: case 59: case 60: case 61: case 62: case 63:
         case 64: value1+=readDelta1(reader); value2=readDelta1(reader+1); value3=readDelta1(reader+2); reader+=3; break;
         case 65: value1+=readDelta1(reader); value2=readDelta1(reader+1); value3=readDelta2(reader+2); reader+=4; break;
         case 66: value1+=readDelta1(reader); value2=readDelta1(reader+1); value3=readDelta3(reader+2); reader+=5; break;
         case 67: value1+=readDelta1(reader); value2=readDelta1(reader+1); value3=readDelta4(reader+2); reader+=6; break;
         case 68: value1+=readDelta1(reader); value2=readDelta2(reader+1); value3=readDelta1(reader+3); reader+=4; break;
         case 69: value1+=readDelta1(reader); value2=readDelta2(reader+1); value3=readDelta2(reader+3); reader+=5; break;
         case 70: value1+=readDelta1(reader); value2=readDelta2(reader+1); value3=readDelta3(reader+3); reader+=6; break;
         case 71: value1+=readDelta1(reader); value2=readDelta2(reader+1); value3=readDelta4(reader+3); reader+=7; break;
         case 72: value1+=readDelta1(reader); value2=readDelta3(reader+1); value3=readDelta1(reader+4); reader+=5; break;
         case 73: value1+=readDelta1(reader); value2=readDelta3(reader+1); value3=readDelta2(reader+4); reader+=6; break;
         case 74: value1+=readDelta1(reader); value2=readDelta3(reader+1); value3=readDelta3(reader+4); reader+=7; break;
         case 75: value1+=readDelta1(reader); value2=readDelta3(reader+1); value3=readDelta4(reader+4); reader+=8; break;
         case 76: value1+=readDelta1(reader); value2=readDelta4(reader+1); value3=readDelta1(reader+5); reader+=6; break;
         case 77: value1+=readDelta1(reader); value2=readDelta4(reader+1); value3=readDelta2(reader+5); reader+=7; break;
         case 78: value1+=readDelta1(reader); value2=readDelta4(reader+1); value3=readDelta3(reader+5); reader+=8; break;
         case 79: value1+=readDelta1(reader); value2=readDelta4(reader+1); value3=readDelta4(reader+5); reader+=9; break;
         case 80: value1+=readDelta2(reader); value2=readDelta1(reader+2); value3=readDelta1(reader+3); reader+=4; break;
         case 81: value1+=readDelta2(reader); value2=readDelta1(reader+2); value3=readDelta2(reader+3); reader+=5; break;
         case 82: value1+=readDelta2(reader); value2=readDelta1(reader+2); value3=readDelta3(reader+3); reader+=6; break;
         case 83: value1+=readDelta2(reader); value2=readDelta1(reader+2); value3=readDelta4(reader+3); reader+=7; break;
         case 84: value1+=readDelta2(reader); value2=readDelta2(reader+2); value3=readDelta1(reader+4); reader+=5; break;
         case 85: value1+=readDelta2(reader); value2=readDelta2(reader+2); value3=readDelta2(reader+4); reader+=6; break;
         case 86: value1+=readDelta2(reader); value2=readDelta2(reader+2); value3=readDelta3(reader+4); reader+=7; break;
         case 87: value1+=readDelta2(reader); value2=readDelta2(reader+2); value3=readDelta4(reader+4); reader+=8; break;
         case 88: value1+=readDelta2(reader); value2=readDelta3(reader+2); value3=readDelta1(reader+5); reader+=6; break;
         case 89: value1+=readDelta2(reader); value2=readDelta3(reader+2); value3=readDelta2(reader+5); reader+=7; break;
         case 90: value1+=readDelta2(reader); value2=readDelta3(reader+2); value3=readDelta3(reader+5); reader+=8; break;
         case 91: value1+=readDelta2(reader); value2=readDelta3(reader+2); value3=readDelta4(reader+5); reader+=9; break;
         case 92: value1+=readDelta2(reader); value2=readDelta4(reader+2); value3=readDelta1(reader+6); reader+=7; break;
         case 93: value1+=readDelta2(reader); value2=readDelta4(reader+2); value3=readDelta2(reader+6); reader+=8; break;
         case 94: value1+=readDelta2(reader); value2=readDelta4(reader+2); value3=readDelta3(reader+6); reader+=9; break;
         case 95: value1+=readDelta2(reader); value2=readDelta4(reader+2); value3=readDelta4(reader+6); reader+=10; break;
         case 96: value1+=readDelta3(reader); value2=readDelta1(reader+3); value3=readDelta1(reader+4); reader+=5; break;
         case 97: value1+=readDelta3(reader); value2=readDelta1(reader+3); value3=readDelta2(reader+4); reader+=6; break;
         case 98: value1+=readDelta3(reader); value2=readDelta1(reader+3); value3=readDelta3(reader+4); reader+=7; break;
         case 99: value1+=readDelta3(reader); value2=readDelta1(reader+3); value3=readDelta4(reader+4); reader+=8; break;
         case 100: value1+=readDelta3(reader); value2=readDelta2(reader+3); value3=readDelta1(reader+5); reader+=6; break;
         case 101: value1+=readDelta3(reader); value2=readDelta2(reader+3); value3=readDelta2(reader+5); reader+=7; break;
         case 102: value1+=readDelta3(reader); value2=readDelta2(reader+3); value3=readDelta3(reader+5); reader+=8; break;
         case 103: value1+=readDelta3(reader); value2=readDelta2(reader+3); value3=readDelta4(reader+5); reader+=9; break;
         case 104: value1+=readDelta3(reader); value2=readDelta3(reader+3); value3=readDelta1(reader+6); reader+=7; break;
         case 105: value1+=readDelta3(reader); value2=readDelta3(reader+3); value3=readDelta2(reader+6); reader+=8; break;
         case 106: value1+=readDelta3(reader); value2=readDelta3(reader+3); value3=readDelta3(reader+6); reader+=9; break;
         case 107: value1+=readDelta3(reader); value2=readDelta3(reader+3); value3=readDelta4(reader+6); reader+=10; break;
         case 108: value1+=readDelta3(reader); value2=readDelta4(reader+3); value3=readDelta1(reader+7); reader+=8; break;
         case 109: value1+=readDelta3(reader); value2=readDelta4(reader+3); value3=readDelta2(reader+7); reader+=9; break;
         case 110: value1+=readDelta3(reader); value2=readDelta4(reader+3); value3=readDelta3(reader+7); reader+=10; break;
         case 111: value1+=readDelta3(reader); value2=readDelta4(reader+3); value3=readDelta4(reader+7); reader+=11; break;
         case 112: value1+=readDelta4(reader); value2=readDelta1(reader+4); value3=readDelta1(reader+5); reader+=6; break;
         case 113: value1+=readDelta4(reader); value2=readDelta1(reader+4); value3=readDelta2(reader+5); reader+=7; break;
         case 114: value1+=readDelta4(reader); value2=readDelta1(reader+4); value3=readDelta3(reader+5); reader+=8; break;
         case 115: value1+=readDelta4(reader); value2=readDelta1(reader+4); value3=readDelta4(reader+5); reader+=9; break;
         case 116: value1+=readDelta4(reader); value2=readDelta2(reader+4); value3=readDelta1(reader+6); reader+=7; break;
         case 117: value1+=readDelta4(reader); value2=readDelta2(reader+4); value3=readDelta2(reader+6); reader+=8; break;
         case 118: value1+=readDelta4(reader); value2=readDelta2(reader+4); value3=readDelta3(reader+6); reader+=9; break;
         case 119: value1+=readDelta4(reader); value2=readDelta2(reader+4); value3=readDelta4(reader+6); reader+=10; break;
         case 120: value1+=readDelta4(reader); value2=readDelta3(reader+4); value3=readDelta1(reader+7); reader+=8; break;
         case 121: value1+=readDelta4(reader); value2=readDelta3(reader+4); value3=readDelta2(reader+7); reader+=9; break;
         case 122: value1+=readDelta4(reader); value2=readDelta3(reader+4); value3=readDelta3(reader+7); reader+=10; break;
         case 123: value1+=readDelta4(reader); value2=readDelta3(reader+4); value3=readDelta4(reader+7); reader+=11; break;
         case 124: value1+=readDelta4(reader); value2=readDelta4(reader+4); value3=readDelta1(reader+8); reader+=9; break;
         case 125: value1+=readDelta4(reader); value2=readDelta4(reader+4); value3=readDelta2(reader+8); reader+=10; break;
         case 126: value1+=readDelta4(reader); value2=readDelta4(reader+4); value3=readDelta3(reader+8); reader+=11; break;
         case 127: value1+=readDelta4(reader); value2=readDelta4(reader+4); value3=readDelta4(reader+8); reader+=12; break;
      }
   }

   // Done
   done:
   oreader=reader;
   v1=value1; v2=value2; v3=value3;
   oc=created; od=deleted; olc=lc; old=ld;
   return produce;
}
//---------------------------------------------------------------------------
FactsSegment::Triple* FactsSegment::decompress(const unsigned char* reader,const unsigned char* limit,Triple* writer,unsigned time)
   /// Decompress triples
{
   // Decompress the first triple
   unsigned value1=readUint32Aligned(reader); reader+=4;
   unsigned value2=readUint32Aligned(reader); reader+=4;
   unsigned value3=readUint32Aligned(reader); reader+=4;
   unsigned created=0,deleted=~0u;
   unsigned lc=created,ld=deleted;
   (*writer).value1=value1;
   (*writer).value2=value2;
   (*writer).value3=value3;
   ++writer;

   // Decompress the remainder of the page
   while (reader<limit) {
      // Decode the header byte
      unsigned info=*(reader++);
      // Small gap only?
      if (info<0x80) {
         if (!info)
            break;
         value3+=info;
         (*writer).value1=value1;
         (*writer).value2=value2;
         (*writer).value3=value3;
         ++writer;
         continue;
      }
      // Decode it
      switch (info&127) {
         case 0: value3+=readDelta1(reader)+128; reader+=1; break;
         case 1: value3+=readDelta2(reader)+128; reader+=2; break;
         case 2: value3+=readDelta3(reader)+128; reader+=3; break;
         case 3: value3+=readDelta4(reader)+128; reader+=4; break;
         case 4: value2+=readDelta1(reader); value3=readDelta1(reader+1); reader+=2; break;
         case 5: value2+=readDelta1(reader); value3=readDelta2(reader+1); reader+=3; break;
         case 6: value2+=readDelta1(reader); value3=readDelta3(reader+1); reader+=4; break;
         case 7: value2+=readDelta1(reader); value3=readDelta4(reader+1); reader+=5; break;
         case 8: value2+=readDelta2(reader); value3=readDelta1(reader+2); reader+=3; break;
         case 9: value2+=readDelta2(reader); value3=readDelta2(reader+2); reader+=4; break;
         case 10: value2+=readDelta2(reader); value3=readDelta3(reader+2); reader+=5; break;
         case 11: value2+=readDelta2(reader); value3=readDelta4(reader+2); reader+=6; break;
         case 12: value2+=readDelta3(reader); value3=readDelta1(reader+3); reader+=4; break;
         case 13: value2+=readDelta3(reader); value3=readDelta2(reader+3); reader+=5; break;
         case 14: value2+=readDelta3(reader); value3=readDelta3(reader+3); reader+=6; break;
         case 15: value2+=readDelta3(reader); value3=readDelta4(reader+3); reader+=7; break;
         case 16: value2+=readDelta4(reader); value3=readDelta1(reader+4); reader+=5; break;
         case 17: value2+=readDelta4(reader); value3=readDelta2(reader+4); reader+=6; break;
         case 18: value2+=readDelta4(reader); value3=readDelta3(reader+4); reader+=7; break;
         case 19: value2+=readDelta4(reader); value3=readDelta4(reader+4); reader+=8; break;
         case 20: lc=created; created=readUint32(reader); reader+=4; if ((created>time)||(time>=deleted)) goto doSkipInTime; else continue;
         case 21: lc=created; created=readUint32(reader); reader+=4; if ((created>time)||(time>=deleted)) goto doSkipInTime; else break;
         case 22: swap(created,lc); if ((created>time)||(time>=deleted)) goto doSkipInTime; else continue;
         case 23: swap(created,lc); if ((created>time)||(time>=deleted)) goto doSkipInTime; else break;
         case 24: ld=deleted; deleted=readUint32(reader); reader+=4; if ((created>time)||(time>=deleted)) goto doSkipInTime; else continue;
         case 25: ld=deleted; deleted=readUint32(reader); reader+=4; if ((created>time)||(time>=deleted)) goto doSkipInTime; else break;
         case 26: swap(deleted,ld); if ((created>time)||(time>=deleted)) goto doSkipInTime; else continue;
         case 27: swap(deleted,ld); if ((created>time)||(time>=deleted)) goto doSkipInTime; else break;
            doSkipInTime: // Version outside the bounds, skip
            if (skipInTime(reader,limit,value1,value2,value3,created,deleted,lc,ld,time))
               break;
            continue;
         case 28: case 29: case 30: case 31: break;
         case 32: case 33: case 34: case 35: case 36: case 37: case 38: case 39: case 40: case 41: case 42: case 43:
         case 44: case 45: case 46: case 47: case 48: case 49: case 50: case 51: case 52: case 53: case 54: case 55:
         case 56: case 57: case 58: case 59: case 60: case 61: case 62: case 63:
         case 64: value1+=readDelta1(reader); value2=readDelta1(reader+1); value3=readDelta1(reader+2); reader+=3; break;
         case 65: value1+=readDelta1(reader); value2=readDelta1(reader+1); value3=readDelta2(reader+2); reader+=4; break;
         case 66: value1+=readDelta1(reader); value2=readDelta1(reader+1); value3=readDelta3(reader+2); reader+=5; break;
         case 67: value1+=readDelta1(reader); value2=readDelta1(reader+1); value3=readDelta4(reader+2); reader+=6; break;
         case 68: value1+=readDelta1(reader); value2=readDelta2(reader+1); value3=readDelta1(reader+3); reader+=4; break;
         case 69: value1+=readDelta1(reader); value2=readDelta2(reader+1); value3=readDelta2(reader+3); reader+=5; break;
         case 70: value1+=readDelta1(reader); value2=readDelta2(reader+1); value3=readDelta3(reader+3); reader+=6; break;
         case 71: value1+=readDelta1(reader); value2=readDelta2(reader+1); value3=readDelta4(reader+3); reader+=7; break;
         case 72: value1+=readDelta1(reader); value2=readDelta3(reader+1); value3=readDelta1(reader+4); reader+=5; break;
         case 73: value1+=readDelta1(reader); value2=readDelta3(reader+1); value3=readDelta2(reader+4); reader+=6; break;
         case 74: value1+=readDelta1(reader); value2=readDelta3(reader+1); value3=readDelta3(reader+4); reader+=7; break;
         case 75: value1+=readDelta1(reader); value2=readDelta3(reader+1); value3=readDelta4(reader+4); reader+=8; break;
         case 76: value1+=readDelta1(reader); value2=readDelta4(reader+1); value3=readDelta1(reader+5); reader+=6; break;
         case 77: value1+=readDelta1(reader); value2=readDelta4(reader+1); value3=readDelta2(reader+5); reader+=7; break;
         case 78: value1+=readDelta1(reader); value2=readDelta4(reader+1); value3=readDelta3(reader+5); reader+=8; break;
         case 79: value1+=readDelta1(reader); value2=readDelta4(reader+1); value3=readDelta4(reader+5); reader+=9; break;
         case 80: value1+=readDelta2(reader); value2=readDelta1(reader+2); value3=readDelta1(reader+3); reader+=4; break;
         case 81: value1+=readDelta2(reader); value2=readDelta1(reader+2); value3=readDelta2(reader+3); reader+=5; break;
         case 82: value1+=readDelta2(reader); value2=readDelta1(reader+2); value3=readDelta3(reader+3); reader+=6; break;
         case 83: value1+=readDelta2(reader); value2=readDelta1(reader+2); value3=readDelta4(reader+3); reader+=7; break;
         case 84: value1+=readDelta2(reader); value2=readDelta2(reader+2); value3=readDelta1(reader+4); reader+=5; break;
         case 85: value1+=readDelta2(reader); value2=readDelta2(reader+2); value3=readDelta2(reader+4); reader+=6; break;
         case 86: value1+=readDelta2(reader); value2=readDelta2(reader+2); value3=readDelta3(reader+4); reader+=7; break;
         case 87: value1+=readDelta2(reader); value2=readDelta2(reader+2); value3=readDelta4(reader+4); reader+=8; break;
         case 88: value1+=readDelta2(reader); value2=readDelta3(reader+2); value3=readDelta1(reader+5); reader+=6; break;
         case 89: value1+=readDelta2(reader); value2=readDelta3(reader+2); value3=readDelta2(reader+5); reader+=7; break;
         case 90: value1+=readDelta2(reader); value2=readDelta3(reader+2); value3=readDelta3(reader+5); reader+=8; break;
         case 91: value1+=readDelta2(reader); value2=readDelta3(reader+2); value3=readDelta4(reader+5); reader+=9; break;
         case 92: value1+=readDelta2(reader); value2=readDelta4(reader+2); value3=readDelta1(reader+6); reader+=7; break;
         case 93: value1+=readDelta2(reader); value2=readDelta4(reader+2); value3=readDelta2(reader+6); reader+=8; break;
         case 94: value1+=readDelta2(reader); value2=readDelta4(reader+2); value3=readDelta3(reader+6); reader+=9; break;
         case 95: value1+=readDelta2(reader); value2=readDelta4(reader+2); value3=readDelta4(reader+6); reader+=10; break;
         case 96: value1+=readDelta3(reader); value2=readDelta1(reader+3); value3=readDelta1(reader+4); reader+=5; break;
         case 97: value1+=readDelta3(reader); value2=readDelta1(reader+3); value3=readDelta2(reader+4); reader+=6; break;
         case 98: value1+=readDelta3(reader); value2=readDelta1(reader+3); value3=readDelta3(reader+4); reader+=7; break;
         case 99: value1+=readDelta3(reader); value2=readDelta1(reader+3); value3=readDelta4(reader+4); reader+=8; break;
         case 100: value1+=readDelta3(reader); value2=readDelta2(reader+3); value3=readDelta1(reader+5); reader+=6; break;
         case 101: value1+=readDelta3(reader); value2=readDelta2(reader+3); value3=readDelta2(reader+5); reader+=7; break;
         case 102: value1+=readDelta3(reader); value2=readDelta2(reader+3); value3=readDelta3(reader+5); reader+=8; break;
         case 103: value1+=readDelta3(reader); value2=readDelta2(reader+3); value3=readDelta4(reader+5); reader+=9; break;
         case 104: value1+=readDelta3(reader); value2=readDelta3(reader+3); value3=readDelta1(reader+6); reader+=7; break;
         case 105: value1+=readDelta3(reader); value2=readDelta3(reader+3); value3=readDelta2(reader+6); reader+=8; break;
         case 106: value1+=readDelta3(reader); value2=readDelta3(reader+3); value3=readDelta3(reader+6); reader+=9; break;
         case 107: value1+=readDelta3(reader); value2=readDelta3(reader+3); value3=readDelta4(reader+6); reader+=10; break;
         case 108: value1+=readDelta3(reader); value2=readDelta4(reader+3); value3=readDelta1(reader+7); reader+=8; break;
         case 109: value1+=readDelta3(reader); value2=readDelta4(reader+3); value3=readDelta2(reader+7); reader+=9; break;
         case 110: value1+=readDelta3(reader); value2=readDelta4(reader+3); value3=readDelta3(reader+7); reader+=10; break;
         case 111: value1+=readDelta3(reader); value2=readDelta4(reader+3); value3=readDelta4(reader+7); reader+=11; break;
         case 112: value1+=readDelta4(reader); value2=readDelta1(reader+4); value3=readDelta1(reader+5); reader+=6; break;
         case 113: value1+=readDelta4(reader); value2=readDelta1(reader+4); value3=readDelta2(reader+5); reader+=7; break;
         case 114: value1+=readDelta4(reader); value2=readDelta1(reader+4); value3=readDelta3(reader+5); reader+=8; break;
         case 115: value1+=readDelta4(reader); value2=readDelta1(reader+4); value3=readDelta4(reader+5); reader+=9; break;
         case 116: value1+=readDelta4(reader); value2=readDelta2(reader+4); value3=readDelta1(reader+6); reader+=7; break;
         case 117: value1+=readDelta4(reader); value2=readDelta2(reader+4); value3=readDelta2(reader+6); reader+=8; break;
         case 118: value1+=readDelta4(reader); value2=readDelta2(reader+4); value3=readDelta3(reader+6); reader+=9; break;
         case 119: value1+=readDelta4(reader); value2=readDelta2(reader+4); value3=readDelta4(reader+6); reader+=10; break;
         case 120: value1+=readDelta4(reader); value2=readDelta3(reader+4); value3=readDelta1(reader+7); reader+=8; break;
         case 121: value1+=readDelta4(reader); value2=readDelta3(reader+4); value3=readDelta2(reader+7); reader+=9; break;
         case 122: value1+=readDelta4(reader); value2=readDelta3(reader+4); value3=readDelta3(reader+7); reader+=10; break;
         case 123: value1+=readDelta4(reader); value2=readDelta3(reader+4); value3=readDelta4(reader+7); reader+=11; break;
         case 124: value1+=readDelta4(reader); value2=readDelta4(reader+4); value3=readDelta1(reader+8); reader+=9; break;
         case 125: value1+=readDelta4(reader); value2=readDelta4(reader+4); value3=readDelta2(reader+8); reader+=10; break;
         case 126: value1+=readDelta4(reader); value2=readDelta4(reader+4); value3=readDelta3(reader+8); reader+=11; break;
         case 127: value1+=readDelta4(reader); value2=readDelta4(reader+4); value3=readDelta4(reader+8); reader+=12; break;
      }
      (*writer).value1=value1;
      (*writer).value2=value2;
      (*writer).value3=value3;
      ++writer;
   }
   return writer;
}
//---------------------------------------------------------------------------
static unsigned bytes(unsigned v)
   // Compute the number of bytes required to encode a value
{
   if (v>=(1<<24))
      return 4; else
   if (v>=(1<<16))
      return 3; else
   if (v>=(1<<8)) return 2; else
      return 1;
}
//---------------------------------------------------------------------------
static unsigned writeDelta(unsigned char* buffer,unsigned ofs,unsigned value)
   // Write an integer with varying size
{
   if (value>=(1<<24)) {
      Segment::writeUint32(buffer+ofs,value);
      return ofs+4;
   } else if (value>=(1<<16)) {
      buffer[ofs]=value>>16;
      buffer[ofs+1]=(value>>8)&0xFF;
      buffer[ofs+2]=value&0xFF;
      return ofs+3;
   } else if (value>=(1<<8)) {
      buffer[ofs]=value>>8;
      buffer[ofs+1]=value&0xFF;
      return ofs+2;
   } else {
      buffer[ofs]=value;
      return ofs+1;
   }
}
//---------------------------------------------------------------------------
void FactsSegment::packLeaves(void* reader_,void* boundaries_)
   // Pack the facts into leaves using prefix compression
{
   DatabaseBuilder::FactsReader& reader=*static_cast<DatabaseBuilder::FactsReader*>(reader_);
   std::vector<std::pair<DatabaseBuilder::Triple,unsigned> >& boundaries=*static_cast<std::vector<std::pair<DatabaseBuilder::Triple,unsigned> >*>(boundaries_);

   DatabaseBuilder::PageChainer chainer(8);
   unsigned char buffer[BufferReference::pageSize];
   unsigned bufferPos=headerSize;
   unsigned lastSubject=0,lastPredicate=0,lastObject=0;

   unsigned subject,predicate,object;
   while (reader.next(subject,predicate,object)) {
      // Try to pack it on the current page
      unsigned len;
      if (subject==lastSubject) {
         if (predicate==lastPredicate) {
            if (object==lastObject) {
               // Skipping a duplicate
               continue;
            } else {
               if ((object-lastObject)<128)
                  len=1; else
                  len=1+bytes(object-lastObject-128);
            }
         } else {
            len=1+bytes(predicate-lastPredicate)+bytes(object);
         }
      } else {
         len=1+bytes(subject-lastSubject)+bytes(predicate)+bytes(object);
      }

      // Tuple too big or first element on the page?
      if ((bufferPos==headerSize)||(bufferPos+len>BufferReference::pageSize)) {
         // Write the partial page
         if (bufferPos>headerSize) {
            // Erase header and tail
            for (unsigned index=0;index<headerSize;index++)
               buffer[0]=0;
            for (unsigned index=bufferPos;index<BufferReference::pageSize;index++)
               buffer[index]=0;
            chainer.store(this,buffer);
            DatabaseBuilder::Triple t; t.subject=lastSubject; t.predicate=lastPredicate; t.object=lastObject;
            boundaries.push_back(pair<DatabaseBuilder::Triple,unsigned>(t,chainer.getPageNo()));
         }
         // Write the first element fully
         bufferPos=headerSize;
         writeUint32(buffer+bufferPos,subject); bufferPos+=4;
         writeUint32(buffer+bufferPos,predicate); bufferPos+=4;
         writeUint32(buffer+bufferPos,object); bufferPos+=4;
      } else {
         // No, pack them
         if (subject==lastSubject) {
            if (predicate==lastPredicate) {
               if (object==lastObject) {
                  // Skipping a duplicate
                  continue;
               } else {
                  if ((object-lastObject)<128) {
                     buffer[bufferPos++]=object-lastObject;
                  } else {
                     buffer[bufferPos++]=0x80|(bytes(object-lastObject-128)-1);
                     bufferPos=writeDelta(buffer,bufferPos,object-lastObject-128);
                  }
               }
            } else {
               buffer[bufferPos++]=0x80|(bytes(predicate-lastPredicate)<<2)|(bytes(object)-1);
               bufferPos=writeDelta(buffer,bufferPos,predicate-lastPredicate);
               bufferPos=writeDelta(buffer,bufferPos,object);
            }
         } else {
            buffer[bufferPos++]=0xC0|((bytes(subject-lastSubject)-1)<<4)|((bytes(predicate)-1)<<2)|(bytes(object)-1);
            bufferPos=writeDelta(buffer,bufferPos,subject-lastSubject);
            bufferPos=writeDelta(buffer,bufferPos,predicate);
            bufferPos=writeDelta(buffer,bufferPos,object);
         }
      }

      // Update the values
      lastSubject=subject; lastPredicate=predicate; lastObject=object;
   }
   // Flush the last page
   for (unsigned index=0;index<headerSize;index++)
      buffer[0]=0;
   for (unsigned index=bufferPos;index<BufferReference::pageSize;index++)
      buffer[index]=0;
   chainer.store(this,buffer);
   DatabaseBuilder::Triple t; t.subject=lastSubject; t.predicate=lastPredicate; t.object=lastObject;
   boundaries.push_back(pair<DatabaseBuilder::Triple,unsigned>(t,chainer.getPageNo()));

   chainer.finish();
   tableStart=chainer.getFirstPageNo();
   setSegmentData(slotTableStart,tableStart);

   pages=chainer.getPages();
   setSegmentData(slotPages,pages);
}
//---------------------------------------------------------------------------
void FactsSegment::packInner(const void* data_,void* boundaries_)
   // Create inner nodes
{
   const vector<pair<DatabaseBuilder::Triple,unsigned> >& data=*static_cast<const vector<pair<DatabaseBuilder::Triple,unsigned> >*>(data_);
   vector<pair<DatabaseBuilder::Triple,unsigned> >& boundaries=*static_cast<vector<pair<DatabaseBuilder::Triple,unsigned> >*>(boundaries_);
   boundaries.clear();

   const unsigned headerSize = 24; // LSN+marker+next+count+padding
   DatabaseBuilder::PageChainer chainer(8+4);
   unsigned char buffer[BufferReference::pageSize];
   unsigned bufferPos=headerSize,bufferCount=0;

   for (vector<pair<DatabaseBuilder::Triple,unsigned> >::const_iterator iter=data.begin(),limit=data.end();iter!=limit;++iter) {
      // Do we have to start a new page?
      if ((bufferPos+16)>BufferReference::pageSize) {
         for (unsigned index=0;index<8;index++)
            buffer[index]=0;
         writeUint32(buffer+8,0xFFFFFFFF);
         writeUint32(buffer+12,0);
         writeUint32(buffer+16,bufferCount);
         writeUint32(buffer+20,0);
         for (unsigned index=bufferPos;index<BufferReference::pageSize;index++)
            buffer[index]=0;
         chainer.store(this,buffer);
         boundaries.push_back(pair<DatabaseBuilder::Triple,unsigned>((*(iter-1)).first,chainer.getPageNo()));
         bufferPos=headerSize; bufferCount=0;
      }
      // Write the entry
      writeUint32(buffer+bufferPos,(*iter).first.subject); bufferPos+=4;
      writeUint32(buffer+bufferPos,(*iter).first.predicate); bufferPos+=4;
      writeUint32(buffer+bufferPos,(*iter).first.object); bufferPos+=4;
      writeUint32(buffer+bufferPos,(*iter).second); bufferPos+=4;
      bufferCount++;
   }
   // Write the least page
   for (unsigned index=0;index<8;index++)
      buffer[index]=0;
   writeUint32(buffer+8,0xFFFFFFFF);
   writeUint32(buffer+12,0);
   writeUint32(buffer+16,bufferCount);
   writeUint32(buffer+20,0);
   for (unsigned index=bufferPos;index<BufferReference::pageSize;index++)
      buffer[index]=0;
   chainer.store(this,buffer);
   boundaries.push_back(pair<DatabaseBuilder::Triple,unsigned>(data.back().first,chainer.getPageNo()));
   chainer.finish();
}
//---------------------------------------------------------------------------
void FactsSegment::loadFullFacts(void* reader_)
   // Load the triples into the database
{
   DatabaseBuilder::FactsReader& reader=*static_cast<DatabaseBuilder::FactsReader*>(reader_);

   // Write the leaf nodes
   vector<pair<DatabaseBuilder::Triple,unsigned> > boundaries;
   packLeaves(&reader,&boundaries);

   // Only one leaf node? Special case this
   if (boundaries.size()==1) {
      vector<pair<DatabaseBuilder::Triple,unsigned> > newBoundaries;
      packInner(&boundaries,&newBoundaries);
      swap(boundaries,newBoundaries);
   } else {
      // Write the inner nodes
      while (boundaries.size()>1) {
         vector<pair<DatabaseBuilder::Triple,unsigned> > newBoundaries;
         packInner(&boundaries,&newBoundaries);
         swap(boundaries,newBoundaries);
      }
   }

   // Remember the index root
   indexRoot=boundaries.back().second;
   setSegmentData(slotIndexRoot,indexRoot);
}
//---------------------------------------------------------------------------
void FactsSegment::loadCounts(unsigned groups1,unsigned groups2,unsigned cardinality)
   // Load count statistics
{
   this->groups1=groups1; setSegmentData(slotGroups1,groups1);
   this->groups2=groups2; setSegmentData(slotGroups2,groups2);
   this->cardinality=cardinality; setSegmentData(slotCardinality,cardinality);
}
//---------------------------------------------------------------------------
/// Look ahead buffer for fact sources
class FactsSegment::SourceCollector {
   private:
   /// The input
   FactsSegment::Source& source;
   /// The next triple
   unsigned subject,predicate,object;
   /// Flags
   bool tripleKnown,sourceEmpty;

   /// Look in the future if necessary
   void peek();

   public:
   /// Constructor
   SourceCollector(FactsSegment::Source& source) : source(source),tripleKnown(false),sourceEmpty(false) {}

   /// Empty input?
   bool empty() { peek(); return !tripleKnown; }
   /// Look at the head triple
   Triple head();
   /// Get the next triple
   bool next(Triple& result);
   /// Mark the last read triple as duplicate
   void markAsDuplicate() { source.markAsDuplicate(); }
};
//---------------------------------------------------------------------------
void FactsSegment::SourceCollector::peek()
   // Look in the future if necessary
{
   if (tripleKnown||sourceEmpty)
      return;
   sourceEmpty=source.next(subject,predicate,object);
   tripleKnown=!sourceEmpty;
}
//---------------------------------------------------------------------------
FactsSegment::Triple FactsSegment::SourceCollector::head()
   // Look at the head triple
{
   peek();
   Triple t;
   t.value1=subject;
   t.value2=predicate;
   t.value3=object;
   return t;
}
//---------------------------------------------------------------------------
bool FactsSegment::SourceCollector::next(Triple& result)
   // Get the next triple
{
   peek();
   if (!tripleKnown)
      return false;
   result.value1=subject;
   result.value2=predicate;
   result.value3=object;
   tripleKnown=false;
   return true;
}
//---------------------------------------------------------------------------
/// Helper for updates
class FactsSegment::Updater {
   private:
   /// Maximum tree depth
   static const unsigned maxDepth = 10;

   /// The segment
   FactsSegment* seg;
   /// The pages
   BufferReferenceExclusive pages[maxDepth];
   /// Parent positions
   unsigned positions[maxDepth];
   /// The depth
   unsigned depth;
   /// Do we manipulate the first page after a lookup?
   bool firstPage;

   /// Update a parent entry
   void updateKey(unsigned level,Triple maxKey);

   public:
   /// Constructor
   Updater(FactsSegment* seg);
   /// Destructor
   ~Updater();

   /// Lookup the matching page for a triple
   void lookup(const Triple& triple);
   /// Store a new or updated leaf page
   void storePage(unsigned char* data,Triple maxKey);
   /// Get the first triple of the next leaf page
   Triple nextLeafStart();
   /// Data on the current leaf page
   const unsigned char* currentLeafData();
   /// Data on the current leaf page
   const unsigned char* currentLeafLimit();
};
//---------------------------------------------------------------------------
FactsSegment::Updater::Updater(FactsSegment* seg)
   // Constructor
   : seg(seg),depth(0)
{
}
//---------------------------------------------------------------------------
FactsSegment::Updater::~Updater()
   /// Destructor
{
}
//---------------------------------------------------------------------------
void FactsSegment::Updater::lookup(const Triple& triple)
   // Lookup the matching page for a triple
{
   // Release existing pages
   while (depth)
      pages[--depth].reset();

   // Traverse the B-Tree
   pages[0]=seg->readExclusive(seg->indexRoot);
   positions[0]=0;
   depth=1;
   while (true) {
      const unsigned char* page=static_cast<const unsigned char*>(pages[depth-1].getPage());
      // Inner node?
      if (readUint32Aligned(page+8)==0xFFFFFFFF) {
         // Perform a binary search. The test is more complex as we only have the upper bound for ranges
         unsigned left=0,right=readInnerCount(page),total=right;
         while (left!=right) {
            unsigned middle=(left+right)/2;
            unsigned middle1=readInner1(page,middle),middle2=readInner2(page,middle),middle3=readInner3(page,middle);
            if (greater(triple.value1,triple.value2,triple.value3,middle1,middle2,middle3)) {
               left=middle+1;
            } else if ((!middle)||(greater(triple.value1,triple.value2,triple.value3,readInner1(page,middle-1),readInner2(page,middle-1),readInner3(page,middle-1)))) {
               left=middle;
               break;
            } else {
               right=middle;
            }
         }
         // Unsuccessful search? Then pick the right-most entry
         if (left==total)
            left=total-1;

         // Go down
         pages[depth]=seg->readExclusive(readInnerPage(page,left));
         positions[depth]=left;
         ++depth;
      } else {
         // We reached a leaf
         firstPage=true;
         return;
      }
   }
}
//---------------------------------------------------------------------------
namespace {
//---------------------------------------------------------------------------
LOGACTION2(FactsSegment,UpdateInnerPage,LogData,oldEntry,LogData,newEntry);
//---------------------------------------------------------------------------
void UpdateInnerPage::redo(void* page) const { memcpy(static_cast<unsigned char*>(page)+8,newEntry.ptr,newEntry.len); }
void UpdateInnerPage::undo(void* page) const { memcpy(static_cast<unsigned char*>(page)+8,oldEntry.ptr,oldEntry.len); }
//---------------------------------------------------------------------------
LOGACTION3(FactsSegment,UpdateInner,uint32_t,slot,LogData,oldEntry,LogData,newEntry);
//---------------------------------------------------------------------------
void UpdateInner::redo(void* page) const { memcpy(static_cast<unsigned char*>(page)+headerSizeInner+(entrySizeInner*slot),newEntry.ptr,newEntry.len); }
void UpdateInner::undo(void* page) const { memcpy(static_cast<unsigned char*>(page)+headerSizeInner+(entrySizeInner*slot),oldEntry.ptr,oldEntry.len); }
//---------------------------------------------------------------------------
LOGACTION2(FactsSegment,InsertInner,uint32_t,slot,LogData,newEntry);
//---------------------------------------------------------------------------
void InsertInner::redo(void* page) const {
   memmove(static_cast<unsigned char*>(page)+headerSizeInner+(entrySizeInner*(slot+1)),static_cast<unsigned char*>(page)+headerSizeInner+(entrySizeInner*slot),entrySizeInner*(readInnerCount(static_cast<unsigned char*>(page))-slot));
   memcpy(static_cast<unsigned char*>(page)+headerSizeInner+(entrySizeInner*slot),newEntry.ptr,newEntry.len);
   Segment::writeUint32Aligned(static_cast<unsigned char*>(page)+16,readInnerCount(static_cast<unsigned char*>(page))+1);
}
void InsertInner::undo(void* page) const {
   memmove(static_cast<unsigned char*>(page)+headerSizeInner+(entrySizeInner*slot),static_cast<unsigned char*>(page)+headerSizeInner+(entrySizeInner*(slot+1)),entrySizeInner*(readInnerCount(static_cast<unsigned char*>(page))-slot-1));
   Segment::writeUint32Aligned(static_cast<unsigned char*>(page)+16,readInnerCount(static_cast<unsigned char*>(page))-1);
}
//---------------------------------------------------------------------------
LOGACTION2(FactsSegment,UpdateLeaf,LogData,oldContent,LogData,newContent);
//---------------------------------------------------------------------------
void UpdateLeaf::redo(void* page) const { memcpy(static_cast<unsigned char*>(page)+8,newContent.ptr,newContent.len); }
void UpdateLeaf::undo(void* page) const { memcpy(static_cast<unsigned char*>(page)+8,oldContent.ptr,oldContent.len); }
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
void FactsSegment::Updater::updateKey(unsigned level,Triple maxKey)
   // Update a parent entry
{
   // Update the parent
   BufferReferenceModified parent;
   parent.modify(pages[level]);
   unsigned char* parentData=static_cast<unsigned char*>(parent.getPage());
   unsigned char newEntry[16];
   writeUint32Aligned(newEntry+0,maxKey.value1);
   writeUint32Aligned(newEntry+4,maxKey.value2);
   writeUint32Aligned(newEntry+8,maxKey.value3);
   writeUint32Aligned(newEntry+12,readInnerPage(parentData,positions[level+1]));
   UpdateInner(positions[level+1],LogData(readInnerPtr(parentData,positions[level+1]),entrySizeInner),LogData(newEntry,entrySizeInner)).applyButKeep(parent,pages[level]);

   // Update further up if necessary
   for (;level>0;--level) {
      // Not the maximum?
      if (positions[level]!=readInnerCount(static_cast<const unsigned char*>(pages[level-1].getPage())))
         break;
      // Modify the parent
      parent.modify(pages[level-1]);
      parentData=static_cast<unsigned char*>(parent.getPage());
      writeUint32Aligned(newEntry+12,readInnerPage(parentData,positions[level]));
      UpdateInner(positions[level],LogData(readInnerPtr(parentData,positions[level]),16),LogData(newEntry,16)).applyButKeep(parent,pages[level-1]);
   }
}
//---------------------------------------------------------------------------
void FactsSegment::Updater::storePage(unsigned char* data,Triple maxKey)
   // Store a new or updated leaf page
{
   if (firstPage) {
      // Update the page itself
      BufferReferenceModified leaf;
      leaf.modify(pages[depth-1]);
      unsigned char* leafData=static_cast<unsigned char*>(leaf.getPage());
      memcpy(data+8,leafData+8,4); // copy the next pointer
      UpdateLeaf(LogData(leafData+8,BufferReference::pageSize-8),LogData(data+8,BufferReference::pageSize-8)).applyButKeep(leaf,pages[depth-1]);

      // Update the parent
      updateKey(depth-2,maxKey);
   } else {
      // Allocate a new page
      BufferReferenceModified newLeaf;
      seg->allocPage(newLeaf);
      unsigned newLeafNo=newLeaf.getPageNo();
      unsigned char oldNext[4],newNext[4];
      writeUint32Aligned(newNext,newLeafNo);
      memcpy(oldNext,static_cast<const unsigned char*>(pages[depth-1].getPage())+8,4);
      memcpy(data+8,oldNext,4);

      // Update the old page next pointer
      BufferReferenceModified oldLeaf;
      oldLeaf.modify(pages[depth-1]);
      UpdateLeaf(LogData(oldNext,4),LogData(newNext,4)).apply(oldLeaf);

      // And write the new page
      UpdateLeaf(LogData(static_cast<unsigned char*>(newLeaf.getPage())+8,BufferReference::pageSize-8),LogData(data+8,BufferReference::pageSize-8)).applyButKeep(newLeaf,pages[depth-1]);

      // Insert in parent
      Triple insertKey=maxKey;
      unsigned insertPage=pages[depth-1].getPageNo();
      bool insertRight=true;
      for (unsigned level=depth-2;;--level) {
         // Fits?
         if (readInnerCount(static_cast<const unsigned char*>(pages[level].getPage()))<maxEntriesOnInner) {
            BufferReferenceModified inner;
            inner.modify(pages[level]);
            unsigned char newEntry[16];
            writeUint32Aligned(newEntry+0,insertKey.value1);
            writeUint32Aligned(newEntry+4,insertKey.value2);
            writeUint32Aligned(newEntry+8,insertKey.value3);
            writeUint32Aligned(newEntry+12,insertPage);
            InsertInner(positions[level+1]+1,LogData(newEntry,16));
            if ((positions[level+1]+1==readInnerCount(static_cast<const unsigned char*>(pages[level].getPage())))&&(level>0))
               updateKey(level-1,insertKey);
            if (insertRight)
               positions[level+1]++;
            break;
         }
         // No, we have to split
         BufferReferenceModified newInner;
         seg->allocPage(newInner);
         unsigned char leftPage[BufferReference::pageSize],rightPage[BufferReference::pageSize];
         memset(leftPage,0,BufferReference::pageSize);
         writeUint32Aligned(leftPage+8,~0u);
         writeUint32Aligned(leftPage+12,newInner.getPageNo());
         writeUint32Aligned(leftPage+16,maxEntriesOnInner/2);
         memcpy(leftPage+24,static_cast<const unsigned char*>(pages[level].getPage())+24,16*(maxEntriesOnInner/2));
         memset(rightPage,0,BufferReference::pageSize);
         writeUint32Aligned(rightPage+8,~0u);
         writeUint32Aligned(leftPage+12,readUint32Aligned(static_cast<const unsigned char*>(pages[level].getPage())+12));
         writeUint32Aligned(leftPage+16,maxEntriesOnInner-(maxEntriesOnInner/2));
         memcpy(rightPage+24,static_cast<const unsigned char*>(pages[level].getPage())+24+16*((maxEntriesOnInner/2)),16*(maxEntriesOnInner-(maxEntriesOnInner/2)));
         Triple leftMax;
         leftMax.value1=readInner1(leftPage,maxEntriesOnInner/2);
         leftMax.value2=readInner2(leftPage,maxEntriesOnInner/2);
         leftMax.value3=readInner3(leftPage,maxEntriesOnInner/2);
         Triple rightMax;
         rightMax.value1=readInner1(rightPage,maxEntriesOnInner-(maxEntriesOnInner/2));
         rightMax.value2=readInner2(rightPage,maxEntriesOnInner-(maxEntriesOnInner/2));
         rightMax.value3=readInner3(rightPage,maxEntriesOnInner-(maxEntriesOnInner/2));
         if (level>0)
            updateKey(level-1,leftMax);

         // Update the entries
         BufferReferenceModified inner;
         inner.modify(pages[level]);
         insertKey=rightMax; insertPage=newInner.getPageNo();
         unsigned leftPageNo=inner.getPageNo();
         if (cmpTriple(insertKey,leftMax)<=0) {
            UpdateInnerPage(LogData(static_cast<unsigned char*>(newInner.getPage())+8,BufferReference::pageSize-8),LogData(rightPage+8,BufferReference::pageSize-8)).apply(newInner);
            UpdateInnerPage(LogData(static_cast<unsigned char*>(inner.getPage())+8,BufferReference::pageSize-8),LogData(leftPage+8,BufferReference::pageSize-8)).applyButKeep(inner,pages[level]);
            insertRight=false;
         } else {
            UpdateInnerPage(LogData(static_cast<unsigned char*>(inner.getPage())+8,BufferReference::pageSize-8),LogData(leftPage+8,BufferReference::pageSize-8)).apply(inner);
            UpdateInnerPage(LogData(static_cast<unsigned char*>(newInner.getPage())+8,BufferReference::pageSize-8),LogData(rightPage+8,BufferReference::pageSize-8)).applyButKeep(newInner,pages[level]);
            positions[level+1]-=maxEntriesOnInner/2;
            insertRight=true;
         }

         // Do we need a new root?
         if (!level) {
            for (unsigned index=depth;index>0;index--) {
               pages[index].swap(pages[index-1]);
               positions[index]=positions[index-1];
            }
            ++depth;
            BufferReferenceModified newRoot;
            seg->allocPage(newRoot);
            unsigned char newPage[BufferReference::pageSize];
            writeUint32(newPage+8,~0u);
            writeUint32(newPage+12,0);
            writeUint32(newPage+16,2);
            writeUint32(newPage+20,0);
            writeUint32(newPage+24,leftMax.value1);
            writeUint32(newPage+28,leftMax.value2);
            writeUint32(newPage+32,leftMax.value3);
            writeUint32(newPage+36,leftPageNo);
            writeUint32(newPage+40,rightMax.value1);
            writeUint32(newPage+44,rightMax.value2);
            writeUint32(newPage+48,rightMax.value3);
            writeUint32(newPage+52,insertPage);
            UpdateInnerPage(LogData(static_cast<unsigned char*>(newRoot.getPage())+8,BufferReference::pageSize-8),LogData(newPage+8,BufferReference::pageSize-8)).applyButKeep(newRoot,pages[0]);
            seg->indexRoot=pages[0].getPageNo();
            seg->setSegmentData(slotIndexRoot,seg->indexRoot);
            break;
         }
      }
   }
}
//---------------------------------------------------------------------------
FactsSegment::Triple FactsSegment::Updater::nextLeafStart()
   // Get the first triple of the next leaf page
{
   unsigned nextPage=readUint32Aligned(static_cast<const unsigned char*>(pages[depth-1].getPage())+8);
   Triple t;
   if (!nextPage) {
      t.value1=~0u; t.value2=~0u; t.value3=~0u;
   } else {
      BufferReference nextLeaf(seg->readShared(nextPage));
      const unsigned char* nextLeafData=static_cast<const unsigned char*>(nextLeaf.getPage())+headerSize;
      t.value1=readUint32(nextLeafData);
      t.value2=readUint32(nextLeafData+4);
      t.value3=readUint32(nextLeafData+8);
   }
   return t;
}
//---------------------------------------------------------------------------
const unsigned char* FactsSegment::Updater::currentLeafData()
   // Data on the current leaf page
{
   return static_cast<const unsigned char*>(pages[depth-1].getPage())+headerSize;
}
//---------------------------------------------------------------------------
const unsigned char* FactsSegment::Updater::currentLeafLimit()
   // Data on the current leaf page
{
   return static_cast<const unsigned char*>(pages[depth-1].getPage())+BufferReference::pageSize;
}
//---------------------------------------------------------------------------
int FactsSegment::cmpTriple(const Triple& a,const Triple& b)
   // Compare two triples
{
   if (a.value1<b.value1) return -1;
   if (a.value1>b.value1) return  1;
   if (a.value2<b.value2) return -1;
   if (a.value2>b.value2) return  1;
   if (a.value3<b.value3) return -1;
   if (a.value3>b.value3) return  1;
   return 0;
}
//---------------------------------------------------------------------------
FactsSegment::Triple* FactsSegment::mergeTriples(Triple* mergedTriplesStart,Triple* mergedTriplesLimit,Triple*& currentTriplesStart,Triple* currentTriplesLimit,SourceCollector& input,Triple limit)
   // Merge triples
{
   while (mergedTriplesStart!=mergedTriplesLimit) {
      // Compare
      int cmp;
      if (currentTriplesStart==currentTriplesLimit) {
         if (input.empty()||(cmpTriple(input.head(),limit)>=0))
            break;
         cmp=1;
      } else if (input.empty()||(cmpTriple(input.head(),limit)>=0)) {
         cmp=-1;
      } else {
         cmp=cmpTriple(*currentTriplesStart,input.head());
      }
      // And store
      if (cmp<0) {
         *mergedTriplesStart=*currentTriplesStart;
         ++mergedTriplesStart; ++currentTriplesStart;
      } else if (cmp>0) {
         input.next(*mergedTriplesStart);
         ++mergedTriplesStart;
      } else {
         input.next(*mergedTriplesStart);
         input.markAsDuplicate();
         ++mergedTriplesStart; ++currentTriplesStart;
      }
   }
   return mergedTriplesStart;
}
//---------------------------------------------------------------------------
void FactsSegment::update(FactsSegment::Source& source)
   // Load new facts into the segment
{
   SourceCollector input(source);
   Updater updater(this);

   // Load the input
   while (!input.empty()) {
      updater.lookup(input.head());
      Triple limit=updater.nextLeafStart();

      // Decompress the current page
      static const unsigned time = 0; // XXX use special decompression the preserves versions!
      static const unsigned maxTriples = BufferReference::pageSize;
      Triple currentTriples[maxTriples];
      Triple* currentTriplesStart=currentTriples;
      Triple* currentTriplesStop=decompress(updater.currentLeafData(),updater.currentLeafLimit(),currentTriples,time);

      // Merge and store
      Triple mergedTriples[maxTriples];
      Triple* mergedTriplesStart=mergedTriples,*mergedTriplesStop=mergedTriples;
      unsigned char buffer[BufferReference::pageSize];
      unsigned bufferPos=headerSize;
      unsigned lastSubject=0,lastPredicate=0,lastObject=0;
      while (true) {
         // Merge more triples if necessary
         mergedTriplesStop=mergeTriples(mergedTriplesStop,mergedTriples+maxTriples,currentTriplesStart,currentTriplesStop,input,limit);
         if (mergedTriplesStart==mergedTriplesStop)
            break;

         while (mergedTriplesStart!=mergedTriplesStop) {
            if (mergedTriplesStart>mergedTriples) {
               unsigned count=mergedTriplesStop-mergedTriplesStart;
               memmove(mergedTriples,mergedTriplesStart,sizeof(Triple)*count);
               mergedTriplesStart=mergedTriples;
               mergedTriplesStop=mergedTriplesStart+count;
            }
            unsigned subject=mergedTriplesStart->value1,predicate=mergedTriplesStart->value2,object=mergedTriplesStart->value3;
            ++mergedTriplesStart;

            // Try to pack it on the current page
            unsigned len;
            if (subject==lastSubject) {
               if (predicate==lastPredicate) {
                  if (object==lastObject) {
                     // Skipping a duplicate
                     continue;
                  } else {
                     if ((object-lastObject)<128)
                        len=1; else
                        len=1+bytes(object-lastObject-128);
                  }
               } else {
                  len=1+bytes(predicate-lastPredicate)+bytes(object);
               }
            } else {
               len=1+bytes(subject-lastSubject)+bytes(predicate)+bytes(object);
            }

            // Tuple too big or first element on the page?
            if ((bufferPos==headerSize)||(bufferPos+len>BufferReference::pageSize)) {
               // Write the partial page
               if (bufferPos>headerSize) {
                  // Erase header and tail
                  for (unsigned index=0;index<headerSize;index++)
                     buffer[0]=0;
                  for (unsigned index=bufferPos;index<BufferReference::pageSize;index++)
                     buffer[index]=0;
                  updater.storePage(buffer,mergedTriplesStart[-1]);
               }
               // Write the first element fully
               bufferPos=headerSize;
               writeUint32(buffer+bufferPos,subject); bufferPos+=4;
               writeUint32(buffer+bufferPos,predicate); bufferPos+=4;
               writeUint32(buffer+bufferPos,object); bufferPos+=4;
            } else {
               // No, pack them
               if (subject==lastSubject) {
                  if (predicate==lastPredicate) {
                     if (object==lastObject) {
                        // Skipping a duplicate
                        continue;
                     } else {
                        if ((object-lastObject)<128) {
                           buffer[bufferPos++]=object-lastObject;
                        } else {
                           buffer[bufferPos++]=0x80|(bytes(object-lastObject-128)-1);
                           bufferPos=writeDelta(buffer,bufferPos,object-lastObject-128);
                        }
                     }
                  } else {
                     buffer[bufferPos++]=0x80|(bytes(predicate-lastPredicate)<<2)|(bytes(object)-1);
                     bufferPos=writeDelta(buffer,bufferPos,predicate-lastPredicate);
                     bufferPos=writeDelta(buffer,bufferPos,object);
                  }
               } else {
                  buffer[bufferPos++]=0xC0|((bytes(subject-lastSubject)-1)<<4)|((bytes(predicate)-1)<<2)|(bytes(object)-1);
                  bufferPos=writeDelta(buffer,bufferPos,subject-lastSubject);
                  bufferPos=writeDelta(buffer,bufferPos,predicate);
                  bufferPos=writeDelta(buffer,bufferPos,object);
               }
            }

            // Update the values
            lastSubject=subject; lastPredicate=predicate; lastObject=object;
         }
      }
      // Write the last page
      if (bufferPos!=headerSize)
         updater.storePage(buffer,mergedTriplesStart[-1]);
   }
}
//---------------------------------------------------------------------------
FactsSegment::Scan::Hint::Hint()
   // Constructor
{
}
//---------------------------------------------------------------------------
FactsSegment::Scan::Hint::~Hint()
   // Destructor
{
}
//---------------------------------------------------------------------------
FactsSegment::Scan::Scan(Hint* hint)
   : seg(0),hint(hint)
   // Constructor
{
}
//---------------------------------------------------------------------------
FactsSegment::Scan::~Scan()
   // Destructor
{
   close();
}
//---------------------------------------------------------------------------
bool FactsSegment::Scan::first(FactsSegment& segment)
   // Start a new scan over the whole segment
{
   current=segment.readShared(segment.tableStart);
   seg=&segment;
   pos=posLimit=0;

   return next();
}
//---------------------------------------------------------------------------
bool FactsSegment::Scan::first(FactsSegment& segment,unsigned start1,unsigned start2,unsigned start3)
   // Start a new scan starting from the first entry >= the start condition
{
   // Lookup the right page
   if (!segment.lookup(start1,start2,start3,current))
      return false;

   // Place the iterator
   seg=&segment;
   pos=posLimit=0;

   // Skip over leading entries that are too small
   while (true) {
      if (!next())
         return false;

      if ((getValue1()>start1)||
          ((getValue1()==start1)&&((getValue2()>start2)||
                              ((getValue2()==start2)&&(getValue3()>=start3)))))
         return true;
   }
}
//---------------------------------------------------------------------------
bool FactsSegment::Scan::find(unsigned value1,unsigned value2,unsigned value3)
    // Perform a binary search
{
   const Triple* l=pos,*r=posLimit;
   while (l<r) {
      const Triple* m=l+((r-l)/2);
      if (greater(m->value1,m->value2,m->value3,value1,value2,value3)) {
         r=m;
      } else if (greater(value1,value2,value3,m->value1,m->value2,m->value3)) {
         l=m+1;
      } else {
         pos=m;
         return true;
      }
   }
   pos=l;
   return pos<posLimit;
}
//---------------------------------------------------------------------------
bool FactsSegment::Scan::readNextPage()
   // Read the next entry
{
   // Alread read the first page? Then read the next one
   if (pos-1) {
      const unsigned char* page=static_cast<const unsigned char*>(current.getPage());
      unsigned nextPage=readUint32Aligned(page+8);
      if (!nextPage)
         return false;
      current=seg->readShared(nextPage);
   }

   // Decompress the triples
   static const unsigned time = 0; // XXX use transaction time
   const unsigned char* page=static_cast<const unsigned char*>(current.getPage());
   pos=triples;
   posLimit=decompress(page+headerSize,page+BufferReference::pageSize,triples,time);

   // Check if we should make a skip
   if (hint) {
      unsigned next1=triples[0].value1,next2=triples[0].value2,next3=triples[0].value3;
      while (true) {
         // Compute the next hint
         hint->next(next1,next2,next3);

         // No entry on this page?
         const Triple* oldPos=pos;
         if (!find(next1,next2,next3)) {
            if (!seg->lookup(next1,next2,next3,current))
               return false;
            pos=posLimit=0;
            ++pos;
            return readNextPage();
         }

         // Stop if we are at a suitable position
         if (oldPos==pos)
            break;
      }
   }

   return true;
}
//---------------------------------------------------------------------------
void FactsSegment::Scan::close()
   // Close the scan
{
   seg=0;
   current.reset();
}
//---------------------------------------------------------------------------
