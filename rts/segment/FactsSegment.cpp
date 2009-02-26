#include "rts/segment/FactsSegment.hpp"
#include "rts/database/DatabaseBuilder.hpp"
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
static inline unsigned readInner1(const unsigned char* page,unsigned slot) { return Segment::readUint32Aligned(page+24+16*slot); }
static inline unsigned readInner2(const unsigned char* page,unsigned slot) { return Segment::readUint32Aligned(page+24+16*slot+4); }
static inline unsigned readInner3(const unsigned char* page,unsigned slot) { return Segment::readUint32Aligned(page+24+16*slot+8); }
static inline unsigned readInnerPage(const unsigned char* page,unsigned slot) { return Segment::readUint32Aligned(page+24+16*slot+12); }
//---------------------------------------------------------------------------
/// Compare
static inline bool greater(unsigned a1,unsigned a2,unsigned a3,unsigned b1,unsigned b2,unsigned b3) {
   return (a1>b1)||
          ((a1==b1)&&((a2>b2)||
                      ((a2==b2)&&(a3>b3))));
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
         unsigned left=0,right=readUint32Aligned(page+16);
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
   vector<pair<DatabaseBuilder::Triple,unsigned> > newBoundaries;
   if (boundaries.size()==1) {
      packInner(&boundaries,&newBoundaries);
      swap(boundaries,newBoundaries);
   } else {
      // Write the inner nodes
      while (boundaries.size()>1) {
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
static inline unsigned readDelta1(const unsigned char* pos) { return pos[0]; }
static unsigned readDelta2(const unsigned char* pos) { return (pos[0]<<8)|pos[1]; }
static unsigned readDelta3(const unsigned char* pos) { return (pos[0]<<16)|(pos[1]<<8)|pos[2]; }
static unsigned readDelta4(const unsigned char* pos) { return (pos[0]<<24)|(pos[1]<<16)|(pos[2]<<8)|pos[3]; }
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

   // Decompress the first triple
   const unsigned char* page=static_cast<const unsigned char*>(current.getPage());
   const unsigned char* reader=page+headerSize,*limit=page+BufferReference::pageSize;
   unsigned value1=readUint32Aligned(reader); reader+=4;
   unsigned value2=readUint32Aligned(reader); reader+=4;
   unsigned value3=readUint32Aligned(reader); reader+=4;
   Triple* writer=triples;
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
         case 20: case 21: case 22: case 23: case 24: case 25: case 26: case 27: case 28: case 29: case 30: case 31: break;
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

   // Update the entries
   pos=triples;
   posLimit=writer;

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
