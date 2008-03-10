#include "rts/segment/StatisticsSegment.hpp"
#include "rts/buffer/BufferManager.hpp"
#include <cstring>
//---------------------------------------------------------------------------
StatisticsSegment::StatisticsSegment(BufferManager& bufferManager,unsigned statisticsPage)
   : Segment(bufferManager),statisticsPage(statisticsPage)
   // Constructor
{
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
      data+=4;
      for (unsigned index=0;index<count;index++) {
         Bucket b;
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
   }
}
//---------------------------------------------------------------------------
static bool findBucket(BufferReference& page,unsigned value1,StatisticsSegment::Bucket& result)
   // Find the first bucket containing value1
{
   const unsigned char* data=static_cast<const unsigned char*>(page.getPage());
   unsigned count=Segment::readUint32Aligned(data);
   data+=4;
   unsigned l=0,r=count;
   while (l<r) {
      unsigned m=(l+r)/2;
      const unsigned char* pos=data+(m*(18*4));
      unsigned start1=Segment::readUint32Aligned(pos),stop1=Segment::readUint32Aligned(pos+12);
      if ((start1<=value1)&&(value1<=stop1)) {
         readBucket(pos,result);
         return true;
      }
   }
   if (l<count) {
      const unsigned char* pos=data+(l*(18*4));
      unsigned start1=Segment::readUint32Aligned(pos),stop1=Segment::readUint32Aligned(pos+12);
      if ((start1<=value1)&&(value1<=stop1)) {
         readBucket(pos,result);
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
