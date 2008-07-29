#include "rts/segment/StatisticsSegment.hpp"
#include "rts/buffer/BufferManager.hpp"
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
StatisticsSegment::StatisticsSegment(BufferManager& bufferManager,unsigned statisticsPage)
   : Segment(bufferManager),statisticsPage(statisticsPage)
   // Constructor
{
}
//---------------------------------------------------------------------------
static unsigned readBucketStart1(BufferReference& page,unsigned slot)
   // Read the start of a bucket
{
   const unsigned char* data=static_cast<const unsigned char*>(page.getPage());
   return Segment::readUint32Aligned(data+4+(slot*18*4));
}
//---------------------------------------------------------------------------
static unsigned readBucketStart2(BufferReference& page,unsigned slot)
   // Read the start of a bucket
{
   const unsigned char* data=static_cast<const unsigned char*>(page.getPage());
   return Segment::readUint32Aligned(data+4+(slot*18*4)+4);
}
//---------------------------------------------------------------------------
static unsigned readBucketStart3(BufferReference& page,unsigned slot)
   // Read the start of a bucket
{
   const unsigned char* data=static_cast<const unsigned char*>(page.getPage());
   return Segment::readUint32Aligned(data+4+(slot*18*4)+8);
}
//---------------------------------------------------------------------------
static unsigned readBucketStop1(BufferReference& page,unsigned slot)
   // Read the end of a bucket
{
   const unsigned char* data=static_cast<const unsigned char*>(page.getPage());
   return Segment::readUint32Aligned(data+4+(slot*18*4)+12);
}
//---------------------------------------------------------------------------
static unsigned readBucketStop2(BufferReference& page,unsigned slot)
   // Read the end of a bucket
{
   const unsigned char* data=static_cast<const unsigned char*>(page.getPage());
   return Segment::readUint32Aligned(data+4+(slot*18*4)+16);
}
//---------------------------------------------------------------------------
static unsigned readBucketStop3(BufferReference& page,unsigned slot)
   // Read the end of a bucket
{
   const unsigned char* data=static_cast<const unsigned char*>(page.getPage());
   return Segment::readUint32Aligned(data+4+(slot*18*4)+20);
}
//---------------------------------------------------------------------------
static void readBucket(const unsigned char* pos,StatisticsSegment::Bucket& result)
   // Unpack a bucket
{
   result.start1=Segment::readUint32Aligned(pos);
   result.start2=Segment::readUint32Aligned(pos+4);
   result.start3=Segment::readUint32Aligned(pos+8);
   result.stop1=Segment::readUint32Aligned(pos+12);
   result.stop2=Segment::readUint32Aligned(pos+16);
   result.stop3=Segment::readUint32Aligned(pos+20);
   result.prefix1Card=Segment::readUint32Aligned(pos+24);
   result.prefix2Card=Segment::readUint32Aligned(pos+28);
   result.card=Segment::readUint32Aligned(pos+32);
   result.val1S=Segment::readUint32Aligned(pos+36);
   result.val1P=Segment::readUint32Aligned(pos+40);
   result.val1O=Segment::readUint32Aligned(pos+44);
   result.val2S=Segment::readUint32Aligned(pos+48);
   result.val2P=Segment::readUint32Aligned(pos+52);
   result.val2O=Segment::readUint32Aligned(pos+56);
   result.val3S=Segment::readUint32Aligned(pos+60);
   result.val3P=Segment::readUint32Aligned(pos+64);
   result.val3O=Segment::readUint32Aligned(pos+68);
}
//---------------------------------------------------------------------------
static void aggregateBuckets(BufferReference& page,unsigned start,unsigned stop,StatisticsSegment::Bucket& result)
   // Aggregate buckets
{
   const unsigned char* data=static_cast<const unsigned char*>(page.getPage());
   data+=4;
   for (unsigned index=start;index<stop;index++) {
      StatisticsSegment::Bucket b;
      readBucket(data+(index*(18*4)),b);
      result.prefix1Card+=b.prefix1Card;
      result.prefix2Card+=b.prefix2Card;
      result.card+=b.card;
      result.val1S+=b.val1S;
      result.val1P+=b.val1P;
      result.val1O+=b.val1O;
      result.val2S+=b.val2S;
      result.val2P+=b.val2P;
      result.val2O+=b.val2O;
      result.val3S+=b.val3S;
      result.val3P+=b.val3P;
      result.val3O+=b.val3O;
   }
   if (start<stop) {
      result.start1=readBucketStart1(page,start);
      result.start2=readBucketStart2(page,start);
      result.start3=readBucketStart3(page,start);
      result.stop1=readBucketStop1(page,stop-1);
      result.stop2=readBucketStop2(page,stop-1);
      result.stop3=readBucketStop3(page,stop-1);
   }
}
//---------------------------------------------------------------------------
void StatisticsSegment::lookup(Bucket& result)
   // Derive a bucket
{
   // Prepare the boundaries
   memset(&result,0,sizeof(result));
   result.start1=0;
   result.start2=0;
   result.start3=0;
   result.stop1=~0u;
   result.stop2=~0u;
   result.stop3=~0u;

   // Aggregate over all buckers
   {
      BufferReference page(readShared(statisticsPage));
      const unsigned char* data=static_cast<const unsigned char*>(page.getPage());
      unsigned count=Segment::readUint32Aligned(data);
      aggregateBuckets(page,0,count,result);
   }
}
//---------------------------------------------------------------------------
static bool findBucket(BufferReference& page,unsigned value1,StatisticsSegment::Bucket& result)
   // Find the first bucket containing value1
{
   memset(&result,0,sizeof(result));

   const unsigned char* data=static_cast<const unsigned char*>(page.getPage());
   unsigned count=Segment::readUint32Aligned(data);
   data+=4;
   unsigned l=0,r=count;
   while (l<r) {
      unsigned m=(l+r)/2;
      unsigned start1=readBucketStart1(page,m),stop1=readBucketStop1(page,m);
      if ((start1<=value1)&&(value1<=stop1)) {
         l=m;
         break;
      } else if (value1<start1) {
         r=m;
      } else {
         l=m+1;
      }
   }
   if (l<count) {
      unsigned start=l+1,stop=l+1;
      while ((start>0)&&(readBucketStop1(page,start-1)>value1))
         --start;
      while ((stop<count)&&(readBucketStart1(page,stop)<=value1))
         ++stop;
      if (start!=stop) {
         aggregateBuckets(page,start,stop,result);
         return true;
      }
   }
   return false;
}
//---------------------------------------------------------------------------
void StatisticsSegment::lookup(unsigned value1,Bucket& result)
   // Derive a bucket
{
   // Prepare the boundaries
   memset(&result,0,sizeof(result));
   result.start1=value1;
   result.start2=0;
   result.start3=0;
   result.stop1=value1;
   result.stop2=~0u;
   result.stop3=~0u;

   // Retrieve the matching bucket
   Bucket bucket;
   {
      BufferReference page(readShared(statisticsPage));
      if (!findBucket(page,value1,bucket))
         return;
   }

   // Adapt the values
   unsigned div=bucket.prefix1Card;
   result.prefix1Card=1;
   result.prefix2Card=(bucket.prefix2Card+div-1)/div;
   result.card=(bucket.card+div-1)/div;
   result.val1S=(bucket.val1S+div-1)/div;
   result.val1P=(bucket.val1P+div-1)/div;
   result.val1O=(bucket.val1O+div-1)/div;
   result.val2S=(bucket.val2S+div-1)/div;
   result.val2P=(bucket.val2P+div-1)/div;
   result.val2O=(bucket.val2O+div-1)/div;
   result.val3S=(bucket.val3S+div-1)/div;
   result.val3P=(bucket.val3P+div-1)/div;
   result.val3O=(bucket.val3O+div-1)/div;
}
//---------------------------------------------------------------------------
void StatisticsSegment::lookup(unsigned value1,unsigned value2,Bucket& result)
   // Derive a bucket
{
   // Prepare the boundaries
   memset(&result,0,sizeof(result));
   result.start1=value1;
   result.start2=value2;
   result.start3=0;
   result.stop1=value1;
   result.stop2=value2;
   result.stop3=~0u;

   // Retrieve the matching bucket
   Bucket bucket;
   {
      BufferReference page(readShared(statisticsPage));
      if (!findBucket(page,value1,bucket))
         return;
   }

   // Adapt the values
   unsigned div=bucket.prefix2Card;
   result.prefix1Card=1;
   result.prefix2Card=1;
   result.card=(bucket.card+div-1)/div;
   result.val1S=(bucket.val1S+div-1)/div;
   result.val1P=(bucket.val1P+div-1)/div;
   result.val1O=(bucket.val1O+div-1)/div;
   result.val2S=(bucket.val2S+div-1)/div;
   result.val2P=(bucket.val2P+div-1)/div;
   result.val2O=(bucket.val2O+div-1)/div;
   result.val3S=(bucket.val3S+div-1)/div;
   result.val3P=(bucket.val3P+div-1)/div;
   result.val3O=(bucket.val3O+div-1)/div;
}
//---------------------------------------------------------------------------
void StatisticsSegment::lookup(unsigned value1,unsigned value2,unsigned value3,Bucket& result)
   // Derive a bucket
{
   // Prepare the boundaries
   memset(&result,0,sizeof(result));
   result.start1=value1;
   result.start2=value2;
   result.start3=value3;
   result.stop1=value1;
   result.stop2=value2;
   result.stop3=value3;

   // Retrieve the matching bucket
   Bucket bucket;
   {
      BufferReference page(readShared(statisticsPage));
      if (!findBucket(page,value1,bucket))
         return;
   }

   // Adapt the values
   unsigned div=bucket.card;
   result.prefix1Card=1;
   result.prefix2Card=1;
   result.card=1;
   result.val1S=(bucket.val1S+div-1)/div;
   result.val1P=(bucket.val1P+div-1)/div;
   result.val1O=(bucket.val1O+div-1)/div;
   result.val2S=(bucket.val2S+div-1)/div;
   result.val2P=(bucket.val2P+div-1)/div;
   result.val2O=(bucket.val2O+div-1)/div;
   result.val3S=(bucket.val3S+div-1)/div;
   result.val3P=(bucket.val3P+div-1)/div;
   result.val3O=(bucket.val3O+div-1)/div;
}
//---------------------------------------------------------------------------
