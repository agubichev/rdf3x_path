#include "rts/segment/ExactStatisticsSegment.hpp"
#include "rts/database/Database.hpp"
#include "rts/segment/FactsSegment.hpp"
#include "rts/segment/AggregatedFactsSegment.hpp"
#include "rts/segment/FullyAggregatedFactsSegment.hpp"
#include "infra/util/fastlz.hpp"
#include <algorithm>
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
ExactStatisticsSegment::ExactStatisticsSegment(BufferManager& bufferManager,Database& db,unsigned c2ps,unsigned c2po,unsigned c2so,unsigned c1s,unsigned c1p,unsigned c1o,unsigned long long c0ss,unsigned long long c0sp,unsigned long long c0so,unsigned long long c0ps,unsigned long long c0pp,unsigned long long c0po,unsigned long long c0os,unsigned long long c0op,unsigned long long c0oo)
   : Segment(bufferManager),db(db),c2ps(c2ps),c2po(c2po),c2so(c2so),c1s(c1s),c1p(c1p),c1o(c1o),c0ss(c0ss),c0sp(c0sp),c0so(c0so),c0ps(c0ps),c0pp(c0pp),c0po(c0po),c0os(c0os),c0op(c0op),c0oo(c0oo)
   // Constructor
{
   totalCardinality=db.getFacts(Database::Order_Subject_Predicate_Object).getCardinality();
}
//---------------------------------------------------------------------------
unsigned ExactStatisticsSegment::getCardinality(unsigned subjectConstant,unsigned predicateConstant,unsigned objectConstant) const
   // Compute the cardinality of a single pattern
{
   if (~subjectConstant) {
      if (~predicateConstant) {
         if (~objectConstant) {
            return 1;
         } else {
            AggregatedFactsSegment::Scan scan;
            if (scan.first(db.getAggregatedFacts(Database::Order_Subject_Predicate_Object),subjectConstant,predicateConstant))
               return scan.getCount();
            return 1;
         }
      } else {
         if (~objectConstant) {
            AggregatedFactsSegment::Scan scan;
            if (scan.first(db.getAggregatedFacts(Database::Order_Subject_Object_Predicate),subjectConstant,objectConstant))
               return scan.getCount();
            return 1;
         } else {
            FullyAggregatedFactsSegment::Scan scan;
            if (scan.first(db.getFullyAggregatedFacts(Database::Order_Subject_Predicate_Object),subjectConstant))
               return scan.getCount();
            return 1;
         }
      }
   } else {
      if (~predicateConstant) {
         if (~objectConstant) {
            AggregatedFactsSegment::Scan scan;
            if (scan.first(db.getAggregatedFacts(Database::Order_Predicate_Object_Subject),predicateConstant,objectConstant))
               return scan.getCount();
            return 1;
         } else {
            FullyAggregatedFactsSegment::Scan scan;
            if (scan.first(db.getFullyAggregatedFacts(Database::Order_Predicate_Subject_Object),predicateConstant))
               return scan.getCount();
            return 1;
         }
      } else {
         if (~objectConstant) {
            FullyAggregatedFactsSegment::Scan scan;
            if (scan.first(db.getFullyAggregatedFacts(Database::Order_Object_Subject_Predicate),objectConstant))
               return scan.getCount();
            return 1;
         } else {
            return totalCardinality;
         }
      }
   }
}
//---------------------------------------------------------------------------
static unsigned long long readUintV(const unsigned char*& reader)
   // Unpack a variable length entry
{
   unsigned long long result=0;
   unsigned shift=0;
   while (true) {
      unsigned char c=*(reader++);
      result=result|(static_cast<unsigned long long>(c&0x7F)<<shift);
      shift+=7;
      if (!(c&0x80)) break;
   }
   return result;
}
//---------------------------------------------------------------------------
bool ExactStatisticsSegment::getJoinInfo2(unsigned root,unsigned value1,unsigned value2,unsigned long long& s,unsigned long long& p,unsigned long long& o) const
   // Lookup join cardinalities for two constants
{
   // Traverse the B-Tree
#define readInner1(page,slot) Segment::readUint32Aligned((page)+16+12*(slot))
#define readInner2(page,slot) Segment::readUint32Aligned((page)+16+12*(slot)+4)
#define readInnerPage(page,slot) Segment::readUint32Aligned((page)+16+12*(slot)+8)
#define greater(a1,a2,b1,b2) (((a1)>(b1))||(((a1)==(b1))&&((a2)>(b2))))
   BufferReference ref;
   ref=readShared(root);
   while (true) {
      const unsigned char* page=static_cast<const unsigned char*>(ref.getPage());
      // Inner node?
      if (readUint32Aligned(page)==0xFFFFFFFF) {
         // Perform a binary search. The test is more complex as we only have the upper bound for ranges
         unsigned left=0,right=readUint32Aligned(page+8);
         while (left!=right) {
            unsigned middle=(left+right)/2;
            unsigned middle1=readInner1(page,middle),middle2=readInner2(page,middle);
            if (greater(value1,value2,middle1,middle2)) {
               left=middle+1;
            } else if ((!middle)||(greater(value1,value2,readInner1(page,middle-1),readInner2(page,middle-1)))) {
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
         // A leaf node
         break;
      }
   }
#undef greater
#undef readInnerPage
#undef readInner2
#undef readInner1

   // Decompress the leaf page
   const unsigned char* page=static_cast<const unsigned char*>(ref.getPage());
   unsigned char buffer[10*BufferManager::pageSize];
   unsigned compressedLen=Segment::readUint32(page+4);
   fastlz_decompress(page+8,compressedLen,buffer,sizeof(buffer));

   // Find the potential range for matches
   const unsigned char* reader=buffer;
   unsigned count,currentEntry;
   count=readUintV(reader);
   currentEntry=readUintV(reader);
   unsigned min=count,max=0;
   if (currentEntry==value1) {
      min=max=0;
   }
   for (unsigned index=1;index<count;index++) {
      currentEntry+=readUintV(reader);
      if (currentEntry==value1) {
         if (index<min) min=index;
         max=index;
      }
   }
   if (min>max) return false;

   // Find the exact position
   unsigned pos=count;
   for (unsigned index=0;index<count;index++) {
      unsigned long long v=readUintV(reader);
      if (v&1)
         currentEntry=(v>>1); else
         currentEntry+=(v>>1);
      if ((currentEntry==value2)&&(index>=min)&&(index<=max))
         pos=index;
   }
   if (pos==count) return false;

   // Lookup the join matches
   for (unsigned index=0;index<count;index++) {
      unsigned long long v=readUintV(reader);
      if (index==pos) s=v;
   }
   for (unsigned index=0;index<count;index++) {
      unsigned long long v=readUintV(reader);
      if (index==pos) p=v;
   }
   for (unsigned index=0;index<count;index++) {
      unsigned long long v=readUintV(reader);
      if (index==pos) o=v;
   }

   return true;
}
//---------------------------------------------------------------------------
bool ExactStatisticsSegment::getJoinInfo1(unsigned root,unsigned value1,unsigned long long& s1,unsigned long long& p1,unsigned long long& o1,unsigned long long& s2,unsigned long long& p2,unsigned long long& o2) const
   // Lookup join cardinalities for two constants
{
   // Traverse the B-Tree
#define readInner1(page,slot) Segment::readUint32Aligned(page+16+8*(slot))
#define readInnerPage(page,slot) Segment::readUint32Aligned(page+16+8*(slot)+4)
   BufferReference ref;
   ref=readShared(root);
   while (true) {
      const unsigned char* page=static_cast<const unsigned char*>(ref.getPage());
      // Inner node?
      if (readUint32Aligned(page)==0xFFFFFFFF) {
         // Perform a binary search. The test is more complex as we only have the upper bound for ranges
         unsigned left=0,right=readUint32Aligned(page+8);
         while (left!=right) {
            unsigned middle=(left+right)/2;
            unsigned middle1=readInner1(page,middle);
            if (value1>middle1) {
               left=middle+1;
            } else if ((!middle)||(value1>readInner1(page,middle-1))) {
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
         // A leaf node
         break;
      }
   }
#undef readInnerPage
#undef readInner1

   // Decompress the leaf page
   const unsigned char* page=static_cast<const unsigned char*>(ref.getPage());
   unsigned char buffer[10*BufferManager::pageSize];
   unsigned compressedLen=Segment::readUint32(page+4);
   fastlz_decompress(page+8,compressedLen,buffer,sizeof(buffer));

   // Find the exact position
   const unsigned char* reader=buffer;
   unsigned count,currentEntry;
   count=readUintV(reader);
   currentEntry=readUintV(reader);
   unsigned pos=count;
   if (currentEntry==value1) {
      pos=0;
   }
   for (unsigned index=1;index<count;index++) {
      currentEntry+=readUintV(reader);
      if (currentEntry==value1) {
         pos=index;
      }
   }
   if (pos==count) return false;

   // Lookup the join matches
   for (unsigned index=0;index<count;index++) {
      unsigned long long v=readUintV(reader);
      if (index==pos) s1=v;
   }
   for (unsigned index=0;index<count;index++) {
      unsigned long long v=readUintV(reader);
      if (index==pos) p1=v;
   }
   for (unsigned index=0;index<count;index++) {
      unsigned long long v=readUintV(reader);
      if (index==pos) o1=v;
   }
   for (unsigned index=0;index<count;index++) {
      unsigned long long v=readUintV(reader);
      if (index==pos) s2=v;
   }
   for (unsigned index=0;index<count;index++) {
      unsigned long long v=readUintV(reader);
      if (index==pos) p2=v;
   }
   for (unsigned index=0;index<count;index++) {
      unsigned long long v=readUintV(reader);
      if (index==pos) o2=v;
   }

   return true;
}
//---------------------------------------------------------------------------
bool ExactStatisticsSegment::getJoinInfo(unsigned long long* joinInfo,unsigned subjectConstant,unsigned predicateConstant,unsigned objectConstant) const
   // Lookup join cardinalities
{
   // Reset all entries to "impossible" first
   for (unsigned index=0;index<9;index++)
      joinInfo[index]=~static_cast<unsigned long long>(0);

   // Now look up relevant data
   if (~subjectConstant) {
      if (~predicateConstant) {
         if (~objectConstant) {
            return false; // all constants, cannot participate in a join!
         } else {
            return getJoinInfo2(c2ps,predicateConstant,subjectConstant,joinInfo[6],joinInfo[7],joinInfo[8]);
         }
      } else {
         if (~objectConstant) {
            return getJoinInfo2(c2so,subjectConstant,objectConstant,joinInfo[3],joinInfo[4],joinInfo[5]);
         } else {
            return getJoinInfo1(c1s,subjectConstant,joinInfo[3],joinInfo[4],joinInfo[5],joinInfo[6],joinInfo[7],joinInfo[8]);
         }
      }
   } else {
      if (~predicateConstant) {
         if (~objectConstant) {
            return getJoinInfo2(c2po,predicateConstant,objectConstant,joinInfo[0],joinInfo[1],joinInfo[2]);
         } else {
            return getJoinInfo1(c1p,predicateConstant,joinInfo[0],joinInfo[1],joinInfo[2],joinInfo[6],joinInfo[7],joinInfo[8]);
         }
      } else {
         if (~objectConstant) {
            return getJoinInfo1(c1o,objectConstant,joinInfo[0],joinInfo[1],joinInfo[2],joinInfo[3],joinInfo[4],joinInfo[5]);
         } else {
            joinInfo[0]=c0ss; joinInfo[1]=c0sp; joinInfo[2]=c0so;
            joinInfo[3]=c0ps; joinInfo[4]=c0pp; joinInfo[5]=c0po;
            joinInfo[6]=c0os; joinInfo[7]=c0op; joinInfo[8]=c0oo;
            return true;
         }
      }
   }
}
//---------------------------------------------------------------------------
double ExactStatisticsSegment::getJoinSelectivity(bool s1c,unsigned s1,bool p1c,unsigned p1,bool o1c,unsigned o1,bool s2c,unsigned s2,bool p2c,unsigned p2,bool o2c,unsigned o2) const
   // Compute the join selectivity
{
   // Compute the individual sizes
   double card1=getCardinality(s1c?s1:~0u,p1c?p1:~0u,o1c?o1:~0u);
   double card2=getCardinality(s2c?s2:~0u,p2c?p2:~0u,o2c?o2:~0u);

   // Check that 1 is smaller than 2
   if (card2<card1) {
      swap(card1,card2);
      swap(s1c,s2c);
      swap(s1,s2);
      swap(p1c,p2c);
      swap(p1,p2);
      swap(o1c,o2c);
      swap(o1,o2);
   }

   // Lookup the join info
   unsigned long long joinInfo[9]; double crossCard;
   if (!getJoinInfo(joinInfo,s1c?s1:~0u,p1c?p1:~0u,o1c?o1:~0u)) {
      // Could no locate 1, check 2
      if (!getJoinInfo(joinInfo,s2c?s2:~0u,p2c?p2:~0u,o2c?o2:~0u)) {
         // Could not locate either, guess!
         return 1; // we could guess 0 here, as the entry was not found, but this might be due to stale statistics
      } else {
         crossCard=card2*static_cast<double>(totalCardinality);
      }
   } else {
      crossCard=card1*static_cast<double>(totalCardinality);
   }

   // And construct the most likely result size
   double resultSize=crossCard;
   if ((s1==s2)&&(!s1c)&&(!s2c)) {
      resultSize=min(resultSize,static_cast<double>(joinInfo[0]));
      if (p1c&&o1c) resultSize=min(resultSize,card2);
      if (p2c&&o2c) resultSize=min(resultSize,card1);
   }
   if ((s1==p2)&&(!s1c)&&(!p2c)) {
      resultSize=min(resultSize,static_cast<double>(joinInfo[1]));
      if (p1c&&o1c) resultSize=min(resultSize,card2);
      if (s2c&&o2c) resultSize=min(resultSize,card1);
   }
   if ((s1==o2)&&(!s1c)&&(!o2c)) {
      resultSize=min(resultSize,static_cast<double>(joinInfo[2]));
      if (p1c&&o1c) resultSize=min(resultSize,card2);
      if (s2c&&p2c) resultSize=min(resultSize,card1);
   }
   if ((p1==s2)&&(!p1c)&&(!s2c)) {
      resultSize=min(resultSize,static_cast<double>(joinInfo[3]));
      if (s1c&&o1c) resultSize=min(resultSize,card2);
      if (p2c&&o2c) resultSize=min(resultSize,card1);
   }
   if ((p1==p2)&&(!p1c)&&(!p2c)) {
      resultSize=min(resultSize,static_cast<double>(joinInfo[4]));
      if (s1c&&o1c) resultSize=min(resultSize,card2);
      if (s2c&&o2c) resultSize=min(resultSize,card1);
   }
   if ((p1==o2)&&(!p1c)&&(!o2c)) {
      resultSize=min(resultSize,static_cast<double>(joinInfo[5]));
      if (s1c&&o1c) resultSize=min(resultSize,card2);
      if (s2c&&p2c) resultSize=min(resultSize,card1);
   }
   if ((o1==s2)&&(!o1c)&&(!s2c)) {
      resultSize=min(resultSize,static_cast<double>(joinInfo[6]));
      if (s1c&&p1c) resultSize=min(resultSize,card2);
      if (p2c&&o2c) resultSize=min(resultSize,card1);
   }
   if ((o1==p2)&&(!o1c)&&(!p2c)) {
      resultSize=min(resultSize,static_cast<double>(joinInfo[7]));
      if (s1c&&p1c) resultSize=min(resultSize,card2);
      if (s2c&&o2c) resultSize=min(resultSize,card1);
   }
   if ((o1==o2)&&(!o1c)&&(!o2c)) {
      resultSize=min(resultSize,static_cast<double>(joinInfo[8]));
      if (s1c&&p1c) resultSize=min(resultSize,card2);
      if (s2c&&p2c) resultSize=min(resultSize,card1);
   }

   // Derive selectivity
   return resultSize/crossCard;
}
//---------------------------------------------------------------------------
