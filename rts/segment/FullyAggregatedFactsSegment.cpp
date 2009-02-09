#include "rts/segment/FullyAggregatedFactsSegment.hpp"
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
/// The size of the header on each fact page
static const unsigned headerSize = 4;
//---------------------------------------------------------------------------
/// Helper functions
static inline unsigned readInner1(const unsigned char* page,unsigned slot) { return Segment::readUint32Aligned(page+16+8*slot); }
static inline unsigned readInnerPage(const unsigned char* page,unsigned slot) { return Segment::readUint32Aligned(page+16+8*slot+4); }
//---------------------------------------------------------------------------
FullyAggregatedFactsSegment::FullyAggregatedFactsSegment(DatabasePartition& partition,unsigned tableStart,unsigned indexRoot,unsigned pages,unsigned groups1)
   : Segment(partition),tableStart(tableStart),indexRoot(indexRoot),
     pages(pages),groups1(groups1)
   // Constructor
{
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
      if (readUint32Aligned(page)==0xFFFFFFFF) {
         // Perform a binary search. The test is more complex as we only have the upper bound for ranges
         unsigned left=0,right=readUint32Aligned(page+8);
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
         if (((m+1)<r)&&(!(m[1].value1>value1))) {
            l=m+1;
         } else {
            pos=l;
            return true;
         }
      } else {
         pos=m;
         return true;
      }
   }
   pos=posLimit;
   return false;
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
      unsigned nextPage=readUint32Aligned(page);
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
