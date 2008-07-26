#include "rts/database/DatabaseBuilder.hpp"
#include "rts/database/Database.hpp"
#include "rts/segment/FullyAggregatedFactsSegment.hpp"
#include "rts/segment/FactsSegment.hpp"
#include <iostream>
#include <vector>
#include <algorithm>
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
namespace {
//---------------------------------------------------------------------------
/// A bucket entry
struct Bucket {
   /// Start value
   unsigned start1,start2,start3;
   /// Stop value
   unsigned stop1,stop2,stop3;
   /// Cardinalities
   unsigned prefix1Card,prefix2Card,card;
   /// Number of join partners
   unsigned val1S,val1P,val1O,val2S,val2P,val2O,val3S,val3P,val3O;
};
//---------------------------------------------------------------------------
/// A reordered triple
struct ReorderedTriple {
   /// The values
   unsigned value1,value2,value3;
};
//---------------------------------------------------------------------------
struct SortByValue1 { bool operator()(const ReorderedTriple& a,const ReorderedTriple& b) const { return a.value1<b.value1; } };
struct SortByValue2 { bool operator()(const ReorderedTriple& a,const ReorderedTriple& b) const { return a.value2<b.value2; } };
struct SortByValue3 { bool operator()(const ReorderedTriple& a,const ReorderedTriple& b) const { return a.value3<b.value3; } };
//---------------------------------------------------------------------------
/// Number of buckets per page
static const unsigned bucketsPerPage = (BufferManager::pageSize-4) / sizeof(Bucket);
//---------------------------------------------------------------------------
ReorderedTriple buildTriple(const FactsSegment::Scan& scan)
   // Extract the current triple
{
   ReorderedTriple result;
   result.value1=scan.getValue1();
   result.value2=scan.getValue2();
   result.value3=scan.getValue3();
   return result;
}
//---------------------------------------------------------------------------
unsigned findJoins(Database& db,Database::DataOrder order,vector<pair<unsigned,unsigned> >& counts)
   // Find the number of join partners
{
   unsigned result=0;
   unsigned start=(*counts.begin()).first;

   // Scan the index
   FullyAggregatedFactsSegment::Scan scan;
   vector<pair<unsigned,unsigned> >::const_iterator iter=counts.begin(),limit=counts.end();
   unsigned current=(*iter).first;
   if (scan.first(db.getFullyAggregatedFacts(order),start)) do {
      loopWithoutNext:
      if (scan.getValue1()==current) {
         result+=scan.getCount()*((*iter).second);
         if ((++iter)==limit)
            break;
         current=(*iter).first;
      } else if (scan.getValue1()>current) {
         if ((++iter)==limit)
            break;
         current=(*iter).first;
         goto loopWithoutNext;
      }
   } while (scan.next());

   return result;
}
//---------------------------------------------------------------------------
unsigned mergeBuckets(Bucket& target,const Bucket& next)
   // Merge the statistics of two buckets
{
   target.stop1=next.stop1;
   target.stop2=next.stop2;
   target.stop3=next.stop3;
   target.prefix1Card+=next.prefix1Card;
   target.prefix2Card+=next.prefix2Card;
   target.card+=next.card;
   target.val1S+=next.val1S;
   target.val1P+=next.val1P;
   target.val1O+=next.val1O;
   target.val2S+=next.val2S;
   target.val2P+=next.val2P;
   target.val2O+=next.val2O;
   target.val2S+=next.val3P;
   target.val2P+=next.val3O;
   target.val2O+=next.val3S;

   return target.card;
}
//---------------------------------------------------------------------------
Bucket resolveBucket(const vector<ReorderedTriple>& triples)
   // Build the statitistics for one initial bucket
{
   // Construct the bounds
   Bucket result;
   result.start1=triples.front().value1;
   result.start2=triples.front().value2;
   result.start3=triples.front().value3;
   result.stop1=triples.back().value1;
   result.stop2=triples.back().value2;
   result.stop3=triples.back().value3;

   // Compute cardinalities
   result.prefix1Card=0;
   result.prefix2Card=0;
   { unsigned last1=~0u,last2=~0u;
   for (vector<ReorderedTriple>::const_iterator iter=triples.begin(),limit=triples.end();iter!=limit;++iter)
      if ((*iter).value1!=last1) {
         result.prefix1Card++;
         result.prefix2Card++;
         last1=(*iter).value1;
         last2=(*iter).value2;
      } else if ((*iter).value2!=last2) {
         result.prefix2Card++;
         last2=(*iter).value2;
      }
   }
   result.card=triples.size();

   // Extract the ids
   result.val1S=0;
   result.val1P=0;
   result.val1O=0;
   result.val2S=0;
   result.val2P=0;
   result.val2O=0;
   result.val3S=0;
   result.val3P=0;
   result.val3O=0;

   return result;
}
//---------------------------------------------------------------------------
void resolveBucketJoins(Database& db,Database::DataOrder order,Bucket& bucket)
   // Resolve the join selectivites for a bucket
{
   // First, rebuild the triples
   FactsSegment::Scan scan;
   vector<ReorderedTriple> triples;
   if (scan.first(db.getFacts(order),bucket.start1,bucket.start2,bucket.start3)) {
      triples.push_back(buildTriple(scan));
      while (scan.next()) {
         ReorderedTriple t=buildTriple(scan);
         if ((t.value1>bucket.stop1)||((t.value1==bucket.stop1)&&((t.value2>bucket.stop2)||((t.value2==bucket.stop2)&&(t.value3>bucket.stop3)))))
            break;
         triples.push_back(t);
      }
   }
   if (triples.size()!=bucket.card) {
      cout << "bug! got " << triples.size() << " triples, should have gotten " << bucket.card << endl;
      return;
   }

   // Compute prefix counts
   bucket.prefix1Card=0; bucket.prefix2Card=0;
   unsigned last1=~0,last2=~0u;
   for (vector<ReorderedTriple>::const_iterator iter=triples.begin(),limit=triples.end();iter!=limit;++iter) {
      if (last1!=(*iter).value1) {
         bucket.prefix1Card++; bucket.prefix2Card++;
         last1=(*iter).value1; last2=(*iter).value2;
      } else if (last2!=(*iter).value2) {
         bucket.prefix2Card++;
         last2=(*iter).value2;
      }
   }

   // Now project on individual values
   vector<pair<unsigned,unsigned> > counts1,counts2,counts3;
   sort(triples.begin(),triples.end(),SortByValue1());
   for (vector<ReorderedTriple>::const_iterator iter=triples.begin(),limit=triples.end();iter!=limit;++iter)
      if ((counts1.empty())||((*iter).value1)!=counts1.back().first)
         counts1.push_back(pair<unsigned,unsigned>((*iter).value1,1)); else
         counts1.back().second++;
   sort(triples.begin(),triples.end(),SortByValue2());
   for (vector<ReorderedTriple>::const_iterator iter=triples.begin(),limit=triples.end();iter!=limit;++iter)
      if ((counts2.empty())||((*iter).value2)!=counts2.back().first)
         counts2.push_back(pair<unsigned,unsigned>((*iter).value2,1)); else
         counts2.back().second++;
   sort(triples.begin(),triples.end(),SortByValue3());
   for (vector<ReorderedTriple>::const_iterator iter=triples.begin(),limit=triples.end();iter!=limit;++iter)
      if ((counts3.empty())||((*iter).value3)!=counts3.back().first)
         counts3.push_back(pair<unsigned,unsigned>((*iter).value3,1)); else
         counts3.back().second++;

   // And lookup join selecitivites
   bucket.val1S=findJoins(db,Database::Order_Subject_Predicate_Object,counts1);
   bucket.val1P=findJoins(db,Database::Order_Predicate_Subject_Object,counts1);
   bucket.val1O=findJoins(db,Database::Order_Object_Predicate_Subject,counts1);
   bucket.val2S=findJoins(db,Database::Order_Subject_Predicate_Object,counts2);
   bucket.val2P=findJoins(db,Database::Order_Predicate_Subject_Object,counts2);
   bucket.val2O=findJoins(db,Database::Order_Object_Predicate_Subject,counts2);
   bucket.val3S=findJoins(db,Database::Order_Subject_Predicate_Object,counts3);
   bucket.val3P=findJoins(db,Database::Order_Predicate_Subject_Object,counts3);
   bucket.val3O=findJoins(db,Database::Order_Object_Predicate_Subject,counts3);
}
//---------------------------------------------------------------------------
void buildBuckets(Database& db,Database::DataOrder order,vector<Bucket>& buckets)
   // Build the buckets for a specific order
{
   buckets.clear();

   // Open the appropriate segment
   FactsSegment::Scan scan;
   scan.first(db.getFacts(order));

   // Compute desired aggregation thresholds
   unsigned firstSize=db.getFacts(order).getCardinality()/(bucketsPerPage*100),tinySize=(firstSize+99)/100;

   // Initial pass, aggregate only by value1/value2
   vector<ReorderedTriple> triples;
   bool couldMerge=false;
   triples.push_back(buildTriple(scan));
   while (scan.next()) {
      ReorderedTriple t=buildTriple(scan);
      if ((t.value1!=triples.back().value1)||(t.value2!=triples.back().value2)) {
         // Build the next bucket
         Bucket b=resolveBucket(triples);
         triples.clear();

         // Merge tiny buckets up to 1/100 of the desired size
         if (couldMerge&&(b.card<=tinySize)) {
            if (mergeBuckets(buckets.back(),b)>=firstSize)
               couldMerge=false;
         } else {
            buckets.push_back(b);
            couldMerge=(b.card<=tinySize);
         }
      }
      triples.push_back(t);
   }
   buckets.push_back(resolveBucket(triples));
   triples.clear();

   // Merge more buckets
   while (buckets.size()>bucketsPerPage) {
      // Compute the min sum
      unsigned minSum=buckets[0].card+buckets[1].card;
      for (unsigned index=0,limit=buckets.size()-1;index<limit;index++) {
         unsigned sum=buckets[index].card+buckets[index+1].card;
         if (sum<minSum)
            minSum=sum;
      }
      // Merge all buckets with this size
      for (unsigned index=0;((index+1)<buckets.size())&&(buckets.size()>bucketsPerPage);index++) {
         if ((buckets[index].card+buckets[index+1].card)==minSum) {
            mergeBuckets(buckets[index],buckets[index+1]);
            buckets.erase(buckets.begin()+index+1);
         }
      }
   }

   // Solve the missing join selecitivites
   for (vector<Bucket>::iterator iter=buckets.begin(),limit=buckets.end();iter!=limit;++iter) {
      resolveBucketJoins(db,order,*iter);
   }
}
//---------------------------------------------------------------------------
void writeUint32(unsigned char* target,unsigned value)
   // Write a 32bit value
{
   target[0]=value>>24;
   target[1]=(value>>16)&0xFF;
   target[2]=(value>>8)&0xFF;
   target[3]=value&0xFF;
}
//---------------------------------------------------------------------------
void buildStatisticsPage(Database& db,Database::DataOrder order,unsigned char* page)
   // Prepare a page with statistics
{
   // Build the buckets
   vector<Bucket> buckets;
   buildBuckets(db,order,buckets);

   // Write the header
   unsigned char* writer=page;
   writeUint32(writer,buckets.size());
   writer+=4;

   // Write the buckets
   for (unsigned index=0;index<buckets.size();index++) {
      const Bucket& b=buckets[index];
      writeUint32(writer,b.start1); writer+=4;
      writeUint32(writer,b.start2); writer+=4;
      writeUint32(writer,b.start3); writer+=4;
      writeUint32(writer,b.stop1); writer+=4;
      writeUint32(writer,b.stop2); writer+=4;
      writeUint32(writer,b.stop3); writer+=4;
      writeUint32(writer,b.prefix1Card); writer+=4;
      writeUint32(writer,b.prefix2Card); writer+=4;
      writeUint32(writer,b.card); writer+=4;
      writeUint32(writer,b.val1S); writer+=4;
      writeUint32(writer,b.val1P); writer+=4;
      writeUint32(writer,b.val1O); writer+=4;
      writeUint32(writer,b.val2S); writer+=4;
      writeUint32(writer,b.val2P); writer+=4;
      writeUint32(writer,b.val2O); writer+=4;
      writeUint32(writer,b.val3S); writer+=4;
      writeUint32(writer,b.val3P); writer+=4;
      writeUint32(writer,b.val3O); writer+=4;
   }

   // Padding
   for (unsigned char* limit=page+BufferManager::pageSize;writer<limit;++writer)
      *writer=0;
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
void DatabaseBuilder::computeStatistics(unsigned order)
   // Compare specific statistics (after loading)
{
   // Open the database again
   Database db;
   if (!db.open(dbFile)) {
      cout << "Unable to open " << dbFile << endl;
      throw;
   }

   // Build the statistics page
   unsigned char statisticPage[pageSize];
   buildStatisticsPage(db,static_cast<Database::DataOrder>(order),statisticPage);

   // Close the database
   db.close();

   // And patch the statistics
   ofstream out(dbFile,ios::in|ios::out|ios::ate|ios::binary);
   if (!out.is_open()) {
      cout << "Unable to write " << dbFile << endl;
      throw;
   }
   out.seekp(directory.statistics[order]*pageSize,ios::beg);
   out.write(reinterpret_cast<char*>(statisticPage),pageSize);
}
//---------------------------------------------------------------------------
