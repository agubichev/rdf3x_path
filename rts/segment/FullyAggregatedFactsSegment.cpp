#include "rts/segment/FullyAggregatedFactsSegment.hpp"
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
static const unsigned headerSize = 12; // LSN+next pointer
//---------------------------------------------------------------------------
// Info slots
static const unsigned slotTableStart = 0;
static const unsigned slotIndexRoot = 1;
static const unsigned slotPages = 2;
static const unsigned slotGroups1 = 3;
//---------------------------------------------------------------------------
/// Helper functions
static inline unsigned readInner1(const unsigned char* page,unsigned slot) { return Segment::readUint32Aligned(page+24+8*slot); }
static inline unsigned readInnerPage(const unsigned char* page,unsigned slot) { return Segment::readUint32Aligned(page+24+8*slot+4); }
//---------------------------------------------------------------------------
FullyAggregatedFactsSegment::FullyAggregatedFactsSegment(DatabasePartition& partition)
   : Segment(partition),tableStart(0),indexRoot(0),pages(0),groups1(0)
   // Constructor
{
}
//---------------------------------------------------------------------------
Segment::Type FullyAggregatedFactsSegment::getType() const
   // Get the type
{
   return Segment::Type_FullyAggregatedFacts;
}
//---------------------------------------------------------------------------
void FullyAggregatedFactsSegment::refreshInfo()
   // Refresh segment info stored in the partition
{
   tableStart=getSegmentData(slotTableStart);
   indexRoot=getSegmentData(slotIndexRoot);
   pages=getSegmentData(slotPages);
   groups1=getSegmentData(slotGroups1);
}
//---------------------------------------------------------------------------
bool FullyAggregatedFactsSegment::lookup(unsigned start1,BufferReference& ref)
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
            unsigned middle1=readInner1(page,middle);
            if (start1>middle1) {
               left=middle+1;
            } else if ((!middle)||(start1>readInner1(page,middle-1))) {
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
static unsigned bytes0(unsigned v)
   // Compute the number of bytes required to encode a value with 0 compression
{
   if (v>=(1<<24))
      return 4; else
   if (v>=(1<<16))
      return 3; else
   if (v>=(1<<8)) return 2;
   if (v>0)
      return 1; else
      return 0;
}
//---------------------------------------------------------------------------
static unsigned writeDelta0(unsigned char* buffer,unsigned ofs,unsigned value)
   // Write an integer with varying size with 0 compression
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
   } else if (value>0) {
      buffer[ofs]=value;
      return ofs+1;
   } else return ofs;
}
//---------------------------------------------------------------------------
void FullyAggregatedFactsSegment::packFullyAggregatedLeaves(void* reader_,void* boundaries_)
   // Pack the fully aggregated facts into leaves using prefix compression
{
   DatabaseBuilder::FactsReader& factsReader=*static_cast<DatabaseBuilder::FactsReader*>(reader_);
   std::vector<DatabaseBuilder::Triple>& boundaries=*static_cast<std::vector<DatabaseBuilder::Triple>*>(boundaries_);

   DatabaseBuilder::PageChainer chainer(8);
   unsigned char buffer[BufferReference::pageSize];
   unsigned bufferPos=headerSize;
   unsigned lastSubject=0;

   DatabaseBuilder::PutbackReader reader(factsReader);
   unsigned subject,predicate,object;
   while (reader.next(subject,predicate,object)) {
      // Count
      unsigned nextSubject,nextPredicate,nextObject,count=1;
      while (reader.next(nextSubject,nextPredicate,nextObject)) {
         if (nextSubject!=subject) {
            reader.putBack(nextSubject,nextPredicate,nextObject);
            break;
         }
         if ((nextPredicate==predicate)&&(nextObject==object))
            continue;
         predicate=nextPredicate;
         object=nextObject;
         count++;
      }

      // Try to pack it on the current page
      unsigned len;
      if (((subject-lastSubject)<16)&&(count<=8))
         len=1; else
         len=1+bytes0(subject-lastSubject-1)+bytes0(count-1);

      // Tuple too big or first element on the page?
      if ((bufferPos==headerSize)||(bufferPos+len>BufferReference::pageSize)) {
         // Write the partial page
         if (bufferPos>headerSize) {
            for (unsigned index=0;index<headerSize;index++)
               buffer[index]=0;
            for (unsigned index=bufferPos;index<BufferReference::pageSize;index++)
               buffer[index]=0;
            chainer.store(this,buffer);
            DatabaseBuilder::Triple t; t.subject=lastSubject; t.predicate=0; t.object=chainer.getPageNo();
            boundaries.push_back(t);
         }
         // Write the first element fully
         bufferPos=headerSize;
         writeUint32(buffer+bufferPos,subject); bufferPos+=4;
         writeUint32(buffer+bufferPos,count); bufferPos+=4;
      } else {
         // No, pack them
         if (((subject-lastSubject)<16)&&(count<=8)) {
            buffer[bufferPos++]=((count-1)<<4)|(subject-lastSubject);
         } else {
            buffer[bufferPos++]=0x80|((bytes0(subject-lastSubject-1)*5)+bytes0(count-1));
            bufferPos=writeDelta0(buffer,bufferPos,subject-lastSubject-1);
            bufferPos=writeDelta0(buffer,bufferPos,count-1);
         }
      }

      // Update the values
      lastSubject=subject;
   }
   // Flush the last page
   for (unsigned index=0;index<headerSize;index++)
      buffer[index]=0;
   for (unsigned index=bufferPos;index<BufferReference::pageSize;index++)
      buffer[index]=0;
   chainer.store(this,buffer);
   DatabaseBuilder::Triple t; t.subject=lastSubject; t.predicate=0; t.object=chainer.getPageNo();
   boundaries.push_back(t);
   chainer.finish();
   tableStart=chainer.getFirstPageNo();
   setSegmentData(slotTableStart,tableStart);

   pages=chainer.getPages();
   setSegmentData(slotPages,pages);
}
//---------------------------------------------------------------------------
void FullyAggregatedFactsSegment::packFullyAggregatedInner(const void* data_,void* boundaries_)
   // Create inner nodes
{
   const vector<DatabaseBuilder::Triple>& data=*static_cast<const vector<DatabaseBuilder::Triple>*>(data_);
   vector<DatabaseBuilder::Triple>& boundaries=*static_cast<vector<DatabaseBuilder::Triple>*>(boundaries_);
   boundaries.clear();

   const unsigned headerSize = 24; // LSN+marker+next+count+padding
   DatabaseBuilder::PageChainer chainer(8+4);
   unsigned char buffer[BufferReference::pageSize];
   unsigned bufferPos=headerSize,bufferCount=0;

   for (vector<DatabaseBuilder::Triple>::const_iterator iter=data.begin(),limit=data.end();iter!=limit;++iter) {
      // Do we have to start a new page?
      if ((bufferPos+8)>BufferReference::pageSize) {
         for (unsigned index=0;index<8;index++)
            buffer[index]=0;
         writeUint32(buffer+8,0xFFFFFFFF);
         writeUint32(buffer+12,0);
         writeUint32(buffer+16,bufferCount);
         writeUint32(buffer+20,0);
         for (unsigned index=bufferPos;index<BufferReference::pageSize;index++)
            buffer[index]=0;
         chainer.store(this,buffer);
         DatabaseBuilder::Triple t=*(iter-1); t.object=chainer.getPageNo();
         boundaries.push_back(t);
         bufferPos=headerSize; bufferCount=0;
      }
      // Write the entry
      writeUint32(buffer+bufferPos,(*iter).subject); bufferPos+=4;
      writeUint32(buffer+bufferPos,(*iter).object); bufferPos+=4;
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
   DatabaseBuilder::Triple t=data.back(); t.object=chainer.getPageNo();
   boundaries.push_back(t);
   chainer.finish();
}
//---------------------------------------------------------------------------
void FullyAggregatedFactsSegment::loadFullyAggregatedFacts(void* reader_)
   // Load the triples aggregated into the database
{
   DatabaseBuilder::FactsReader& reader=*static_cast<DatabaseBuilder::FactsReader*>(reader_);

   // Write the leaf nodes
   vector<DatabaseBuilder::Triple> boundaries;
   packFullyAggregatedLeaves(&reader,&boundaries);

   // Only one leaf node? Special case this
   if (boundaries.size()==1) {
      vector<DatabaseBuilder::Triple> newBoundaries;
      packFullyAggregatedInner(&boundaries,&newBoundaries);
      swap(boundaries,newBoundaries);
   } else {
      // Write the inner nodes
      while (boundaries.size()>1) {
         vector<DatabaseBuilder::Triple> newBoundaries;
         packFullyAggregatedInner(&boundaries,&newBoundaries);
         swap(boundaries,newBoundaries);
      }
   }

   // Remember the index root
   indexRoot=boundaries.back().object;
   setSegmentData(slotIndexRoot,indexRoot);
}
//---------------------------------------------------------------------------
void FullyAggregatedFactsSegment::loadCounts(unsigned groups1)
   // Load count statistics
{
   this->groups1=groups1; setSegmentData(slotGroups1,groups1);
}
//---------------------------------------------------------------------------
FullyAggregatedFactsSegment::Scan::Hint::Hint()
   // Constructor
{
}
//---------------------------------------------------------------------------
FullyAggregatedFactsSegment::Scan::Hint::~Hint()
   // Destructor
{
}
//---------------------------------------------------------------------------
FullyAggregatedFactsSegment::Scan::Scan(Hint* hint)
   : seg(0),hint(hint)
   // Constructor
{
}
//---------------------------------------------------------------------------
FullyAggregatedFactsSegment::Scan::~Scan()
   // Destructor
{
   close();
}
//---------------------------------------------------------------------------
bool FullyAggregatedFactsSegment::Scan::first(FullyAggregatedFactsSegment& segment)
   // Start a new scan over the whole segment
{
   current=segment.readShared(segment.tableStart);
   seg=&segment;
   pos=posLimit=0;

   return next();
}
//---------------------------------------------------------------------------
bool FullyAggregatedFactsSegment::Scan::first(FullyAggregatedFactsSegment& segment,unsigned start1)
   // Start a new scan starting from the first entry >= the start condition
{
   // Lookup the right page
   if (!segment.lookup(start1,current))
      return false;

   // Place the iterator
   seg=&segment;
   pos=posLimit=0;

   // Skip over leading entries that are too small
   while (true) {
      if (!next())
         return false;

      if (getValue1()>=start1)
         return true;
   }
}
//---------------------------------------------------------------------------
bool FullyAggregatedFactsSegment::Scan::find(unsigned value1)
    // Perform a binary search
{
   const Triple* l=pos,*r=posLimit;
   while (l<r) {
      const Triple* m=l+((r-l)/2);
      if (m->value1>value1) {
         r=m;
      } else if (value1>m->value1) {
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
bool FullyAggregatedFactsSegment::Scan::readNextPage()
   // Read the next page
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
   unsigned count=readUint32Aligned(reader); reader+=4;
   Triple* writer=triples;
   (*writer).value1=value1;
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
         count=(info>>4)+1;
         value1+=(info&15);
         (*writer).value1=value1;
         (*writer).count=count;
         ++writer;
         continue;
      }
      // Decode the parts
      value1+=1;
      switch (info&127) {
         case 0: count=1; break;
         case 1: count=readDelta1(reader); reader+=1; break;
         case 2: count=readDelta2(reader); reader+=2; break;
         case 3: count=readDelta3(reader); reader+=3; break;
         case 4: count=readDelta4(reader); reader+=4; break;
         case 5: value1+=readDelta1(reader); count=1; reader+=1; break;
         case 6: value1+=readDelta1(reader); count=readDelta1(reader+1)+1; reader+=2; break;
         case 7: value1+=readDelta1(reader); count=readDelta2(reader+1)+1; reader+=3; break;
         case 8: value1+=readDelta1(reader); count=readDelta3(reader+1)+1; reader+=4; break;
         case 9: value1+=readDelta1(reader); count=readDelta4(reader+1)+1; reader+=5; break;
         case 10: value1+=readDelta2(reader); count=1; reader+=2; break;
         case 11: value1+=readDelta2(reader); count=readDelta1(reader+2)+1; reader+=3; break;
         case 12: value1+=readDelta2(reader); count=readDelta2(reader+2)+1; reader+=4; break;
         case 13: value1+=readDelta2(reader); count=readDelta3(reader+2)+1; reader+=5; break;
         case 14: value1+=readDelta2(reader); count=readDelta4(reader+2)+1; reader+=6; break;
         case 15: value1+=readDelta3(reader); count=1; reader+=3; break;
         case 16: value1+=readDelta3(reader); count=readDelta1(reader+3)+1; reader+=4; break;
         case 17: value1+=readDelta3(reader); count=readDelta2(reader+3)+1; reader+=5; break;
         case 18: value1+=readDelta3(reader); count=readDelta3(reader+3)+1; reader+=6; break;
         case 19: value1+=readDelta3(reader); count=readDelta4(reader+3)+1; reader+=7; break;
         case 20: value1+=readDelta4(reader); count=1; reader+=4; break;
         case 21: value1+=readDelta4(reader); count=readDelta1(reader+4)+1; reader+=5; break;
         case 22: value1+=readDelta4(reader); count=readDelta2(reader+4)+1; reader+=6; break;
         case 23: value1+=readDelta4(reader); count=readDelta3(reader+4)+1; reader+=7; break;
         case 24: value1+=readDelta4(reader); count=readDelta4(reader+4)+1; reader+=8; break;
      }
      (*writer).value1=value1;
      (*writer).count=count;
      ++writer;
   }

   // Update the entries
   pos=triples;
   posLimit=writer;

   // Check if we should make a skip
   if (hint) {
      unsigned next1=triples[0].value1;
      while (true) {
         // Compute the next hint
         hint->next(next1);

         // No entry on this page?
         const Triple* oldPos=pos;
         if (!find(next1)) {
            if (!seg->lookup(next1,current))
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
void FullyAggregatedFactsSegment::Scan::close()
   // Close the scan
{
   seg=0;
   current.reset();
}
//---------------------------------------------------------------------------
