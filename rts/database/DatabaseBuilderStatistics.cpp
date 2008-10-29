#include "rts/database/DatabaseBuilder.hpp"
#include "rts/database/Database.hpp"
#include "rts/segment/FullyAggregatedFactsSegment.hpp"
#include "rts/segment/FactsSegment.hpp"
#include "rts/operator/AggregatedIndexScan.hpp"
#include "rts/operator/Filter.hpp"
#include "rts/operator/FullyAggregatedIndexScan.hpp"
#include "rts/operator/HashGroupify.hpp"
#include "rts/operator/MergeJoin.hpp"
#include "rts/runtime/Runtime.hpp"
#include <iostream>
#include <vector>
#include <map>
#include <set>
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
static ReorderedTriple buildTriple(const FactsSegment::Scan& scan)
   // Extract the current triple
{
   ReorderedTriple result;
   result.value1=scan.getValue1();
   result.value2=scan.getValue2();
   result.value3=scan.getValue3();
   return result;
}
//---------------------------------------------------------------------------
static unsigned findJoins(Database& db,Database::DataOrder order,vector<pair<unsigned,unsigned> >& counts)
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
static unsigned mergeBuckets(Bucket& target,const Bucket& next)
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
static void resolveBucketJoins(Database& db,Database::DataOrder order,Bucket& bucket)
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
#if 0
   if (triples.size()!=bucket.card) {
      cout << "bug! got " << triples.size() << " triples, should have gotten " << bucket.card << endl;
      return;
   }
#endif

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
static void buildBuckets(Database& db,Database::DataOrder order,vector<Bucket>& buckets)
   // Build the buckets for a specific order
{
   buckets.clear();

   // Open the appropriate segment
   FactsSegment::Scan scan;
   scan.first(db.getFacts(order));

   // Compute desired aggregation thresholds
   unsigned firstSize=db.getFacts(order).getCardinality()/bucketsPerPage,tinySize=(firstSize+99)/100;

   // Initial pass, aggregate only by value1/value2
   Bucket currentBucket;
   currentBucket.start1=currentBucket.stop1=scan.getValue1();
   currentBucket.start2=currentBucket.stop2=scan.getValue1();
   currentBucket.start3=currentBucket.stop3=scan.getValue3();
   currentBucket.prefix1Card=1; currentBucket.prefix2Card=1; currentBucket.card=1;

   while (scan.next()) {
      unsigned v1=scan.getValue1(),v2=scan.getValue2();
      if ((v1!=currentBucket.stop1)||(v2!=currentBucket.stop2)) {
         // Merge tiny buckets up to 1/100 of the desired size
         if (currentBucket.card<tinySize) {
            if (v1!=currentBucket.stop1)
               currentBucket.prefix1Card++;
            if (v2!=currentBucket.stop2)
               currentBucket.prefix2Card++;
            currentBucket.stop1=v1;
            currentBucket.stop2=v2;
            currentBucket.stop3=scan.getValue3();
            currentBucket.card++;
            continue;
         }
         // Start a new bucket
         buckets.push_back(currentBucket);
         currentBucket.start1=currentBucket.stop1=v1;
         currentBucket.start2=currentBucket.stop2=v2;
         currentBucket.start3=currentBucket.stop3=scan.getValue3();
         currentBucket.prefix1Card=1; currentBucket.prefix2Card=1; currentBucket.card=1;
      } else {
         currentBucket.stop3=scan.getValue3();
         currentBucket.card++;
      }
   }
   buckets.push_back(currentBucket);

   // Merge more buckets
   while (buckets.size()>bucketsPerPage) {
      // Compute the min sum
      vector<unsigned> sums;
      for (unsigned index=0,limit=buckets.size()-1;index<limit;index++)
         sums.push_back(buckets[index].card+buckets[index+1].card);
      std::sort(sums.begin(),sums.end());
      unsigned minSum=sums[sums.size()-bucketsPerPage];

      // Merge all buckets with this size
      unsigned writer=0;
      for (unsigned index=1;index<buckets.size();index++) {
         // Does the rest fit?
         if ((writer+(buckets.size()-index))<bucketsPerPage) {
            for (;index<buckets.size();index++)
               buckets[++writer]=buckets[index];
            break;
         }
         // Should we merge?
         if ((buckets[writer].card+buckets[index].card)<=minSum) {
            mergeBuckets(buckets[writer],buckets[index]);
         } else {
            // No, keep it
            buckets[++writer]=buckets[index];
         }
      }
      // Adjust the size
      buckets.resize(writer+1);
   }

   // Solve the missing join selecitivites
   for (vector<Bucket>::iterator iter=buckets.begin(),limit=buckets.end();iter!=limit;++iter) {
      resolveBucketJoins(db,order,*iter);
   }
}
//---------------------------------------------------------------------------
static void writeUint32(unsigned char* target,unsigned value)
   // Write a 32bit value
{
   target[0]=value>>24;
   target[1]=(value>>16)&0xFF;
   target[2]=(value>>8)&0xFF;
   target[3]=value&0xFF;
}
//---------------------------------------------------------------------------
static void buildStatisticsPage(Database& db,Database::DataOrder order,unsigned char* page)
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
   // Compute specific statistics (after loading)
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
   out.seekp(static_cast<unsigned long long>(directory.statistics[order])*pageSize,ios::beg);
   out.write(reinterpret_cast<char*>(statisticPage),pageSize);
}
//---------------------------------------------------------------------------
namespace {
//---------------------------------------------------------------------------
/// Maximum number of paths fitting on a page (upper bound)
static const unsigned maxPathPerPage = DatabaseBuilder::pageSize / (3*4);
//---------------------------------------------------------------------------
// Information about a single path
struct PathInfo {
   /// The steps
   vector<unsigned> steps;
   /// The count
   unsigned count;

   /// Order by decreasing size
   bool operator<(const PathInfo& p) const { return count>p.count; }
};
//---------------------------------------------------------------------------
void collectTopPaths(unsigned k,vector<PathInfo>& result,Operator* plan,unsigned outputCount,Register* output)
   // Collect the top k paths
{
   result.clear();

   // Collect the plans
   unsigned count,cutoff=0;
   if ((count=plan->first())!=0) do {
      // Safe to discard?
      if (count<cutoff)
         continue;

      // No, construct a new path
      PathInfo path;
      for (unsigned index=0;index<outputCount;index++)
         path.steps.push_back(output[index].value);
      path.count=count;
      result.push_back(path);

      // Sort and prune if needed
      if (result.size()>10*k) {
         sort(result.begin(),result.end());
         result.resize(k);
         cutoff=result.back().count;
      }
   } while ((count=plan->next())!=0);

   // Final sort and cutoff
   sort(result.begin(),result.end());
   if (result.size()>k)
      result.resize(k);
}
//---------------------------------------------------------------------------
bool couldMerge(const map<vector<unsigned>,unsigned>& mergeInfo,const vector<unsigned>& steps)
   // Are all subsequences of size-1 available?
{
   if (steps.empty())
      return true;

   // Sequence 1
   vector<unsigned> seq=steps;
   seq.erase(seq.begin());
   if (!mergeInfo.count(seq))
      return false;

   // Sequence 2
   seq=steps;
   seq.pop_back();
   if (!mergeInfo.count(seq))
      return false;

   return true;
}
//---------------------------------------------------------------------------
void doMerge(vector<PathInfo>& result,map<vector<unsigned>,unsigned>& mergeInfo,const vector<unsigned>& steps,unsigned count)
   // Perform a merge, adding all required subsequences first
{
   if (steps.empty())
      return;

   // Check subsequence 1
   vector<unsigned> seq=steps;
   seq.erase(seq.begin());
   if (mergeInfo[seq]) {
      doMerge(result,mergeInfo,seq,mergeInfo[seq]);
      mergeInfo[seq]=0;
   }

   // Check subsequence 2
   seq=steps;
   seq.pop_back();
   if (mergeInfo[seq]) {
      doMerge(result,mergeInfo,seq,mergeInfo[seq]);
      mergeInfo[seq]=0;
   }

   // And the result itself
   PathInfo p;
   p.steps=steps;
   p.count=count;
   result.push_back(p);
}
//---------------------------------------------------------------------------
void mergeLevels(unsigned k,vector<PathInfo>& result,const vector<PathInfo>& level1,const vector<PathInfo> level2)
   // Merge two levels
{
   // Prepare the merge info for level1
   map<vector<unsigned>,unsigned> mergeInfo;
   for (vector<PathInfo>::const_iterator iter=level1.begin(),limit=level1.end();iter!=limit;++iter)
      mergeInfo[(*iter).steps]=(*iter).count;

   // Perform the merge
   result.clear();
   unsigned index1=0,index2=0;
   while (result.size()<k) {
      // Did we reach the end of one list?
      if (index1>=level1.size()) {
         if (index2>=level2.size()) break;
         if (couldMerge(mergeInfo,level2[index2].steps))
            result.push_back(level2[index2]);
         index2++;
         continue;
      }
      if (index2>=level2.size()) {
         result.push_back(level1[index1]);
         index1++;
         continue;
      }
      // No, perform a real merge
      if (level1[index1].count>=level2[index2].count) {
         result.push_back(level1[index1]);
         mergeInfo[level1[index1].steps]=0;
         continue;
      }
      if (couldMerge(mergeInfo,level2[index2].steps)) {
         doMerge(result,mergeInfo,level2[index2].steps,level2[index2].count);
      }
      index2++;
   }
   // Final check
   if (result.size()>k)
      result.resize(k);
}
//---------------------------------------------------------------------------
void buildChainStatisticsPage(Database& db,unsigned char* statisticPage)
   // Compute frequent chain path
{
   // Compute the initial seeds
   vector<PathInfo> pathLevel1;
   {
      Register regs[1];
      regs[0].reset();
      FullyAggregatedIndexScan* scan=FullyAggregatedIndexScan::create(db,Database::Order_Predicate_Subject_Object,0,false,regs,false,0,false);
      collectTopPaths(maxPathPerPage,pathLevel1,scan,1,regs);
      delete scan;
   }

   // Compute the next level
   vector<PathInfo> pathLevel2;
   {
      // Compute predicate filters
      vector<unsigned> values;
      for (vector<PathInfo>::const_iterator iter=pathLevel1.begin(),limit=pathLevel1.end();iter!=limit;++iter)
         values.push_back((*iter).steps.front());
      sort(values.begin(),values.end());

      // And build an execution Plan
      Register regs[4];
      for (unsigned index=0;index<4;index++)
         regs[index].reset();
      AggregatedIndexScan* scan1=AggregatedIndexScan::create(db,Database::Order_Object_Predicate_Subject,0,false,regs+0,false,regs+2,false);
      Filter* filter1=new Filter(scan1,regs+0,values,false);
      AggregatedIndexScan* scan2=AggregatedIndexScan::create(db,Database::Order_Subject_Predicate_Object,regs+3,false,regs+1,false,0,false);
      Filter* filter2=new Filter(scan2,regs+1,values,false);
      vector<Register*> leftTail,rightTail,group;
      leftTail.push_back(regs+0); rightTail.push_back(regs+1);
      MergeJoin* join=new MergeJoin(filter1,regs+2,leftTail,filter2,regs+3,rightTail);
      group.push_back(regs+0); group.push_back(regs+1);
      HashGroupify* groupify=new HashGroupify(join,group);

      // Run it
      collectTopPaths(maxPathPerPage,pathLevel2,groupify,2,regs);
      delete groupify;
   }

   // XXX integrate the full recursion algorithm

   // Merge the levels
   vector<PathInfo> result;
   mergeLevels(maxPathPerPage,result,pathLevel1,pathLevel2);

   // And fill the page
   memset(statisticPage,0,DatabaseBuilder::pageSize);
   unsigned count=0,ofs=4;
   for (vector<PathInfo>::const_iterator iter=result.begin(),limit=result.end();iter!=limit;++iter) {
      unsigned len=4+4+(4*(*iter).steps.size());
      if (ofs+len>DatabaseBuilder::pageSize)
         break;
      writeUint32(statisticPage+ofs,(*iter).steps.size()); ofs+=4;
      for (vector<unsigned>::const_iterator iter2=(*iter).steps.begin(),limit2=(*iter).steps.end();iter2!=limit2;++iter2) {
         writeUint32(statisticPage+ofs,(*iter2)); ofs+=4;
      }
      writeUint32(statisticPage+ofs,(*iter).count); ofs+=4;
      count++;
   }
   writeUint32(statisticPage,count);
}
//---------------------------------------------------------------------------
// Information about a star path
struct StarInfo {
   /// The steps
   set<unsigned> steps;
   /// The count
   unsigned count;

   /// Order by decreasing size
   bool operator<(const StarInfo& p) const { return count>p.count; }
};
//---------------------------------------------------------------------------
void collectTopStars(unsigned k,vector<StarInfo>& result,Operator* plan,unsigned outputCount,Register* output)
   // Collect the top k stars
{
   result.clear();

   // Collect the plans
   unsigned count,cutoff=0;
   if ((count=plan->first())!=0) do {
      // Safe to discard?
      if (count<cutoff)
         continue;

      // Can discard due to value order?
      bool invalid=false;
      for (unsigned index=1;index<outputCount;index++)
         if (output[index].value<=output[index-1].value)
            invalid=true;
      if (invalid) continue;

      // No, construct a new star
      StarInfo star;
      for (unsigned index=0;index<outputCount;index++)
         star.steps.insert(output[index].value);
      star.count=count;
      result.push_back(star);

      // Sort and prune if needed
      if (result.size()>10*k) {
         sort(result.begin(),result.end());
         result.resize(k);
         cutoff=result.back().count;
      }
   } while ((count=plan->next())!=0);

   // Final sort and cutoff
   sort(result.begin(),result.end());
   if (result.size()>k)
      result.resize(k);
}
//---------------------------------------------------------------------------
bool couldMerge(const map<set<unsigned>,unsigned>& mergeInfo,const set<unsigned>& steps)
   // Are all subsets of size-1 available?
{
   if (steps.empty())
      return true;

   // All subsets
   for (unsigned index=0;index<steps.size();index++) {
      set<unsigned> sub;
      unsigned index2=0;
      for (set<unsigned>::const_iterator iter=steps.begin(),limit=steps.end();iter!=limit;++iter,++index2)
         if (index!=index2)
            sub.insert(*iter);
      if (!mergeInfo.count(sub))
         return false;
   }

   return true;
}
//---------------------------------------------------------------------------
void doMerge(vector<StarInfo>& result,map<set<unsigned>,unsigned>& mergeInfo,const set<unsigned>& steps,unsigned count)
   // Perform a merge, adding all required subsets first
{
   if (steps.empty())
      return;

   // Check all subsets
   for (unsigned index=0;index<steps.size();index++) {
      set<unsigned> sub;
      unsigned index2=0;
      for (set<unsigned>::const_iterator iter=steps.begin(),limit=steps.end();iter!=limit;++iter,++index2)
         if (index!=index2)
            sub.insert(*iter);
      if (mergeInfo[sub]) {
         doMerge(result,mergeInfo,sub,mergeInfo[sub]);
         mergeInfo[sub]=0;
      }
   }

   // And the result itself
   StarInfo s;
   s.steps=steps;
   s.count=count;
   result.push_back(s);
}
//---------------------------------------------------------------------------
void mergeLevels(unsigned k,vector<StarInfo>& result,const vector<StarInfo>& level1,const vector<StarInfo> level2)
   // Merge two levels
{
   // Prepare the merge info for level1
   map<set<unsigned>,unsigned> mergeInfo;
   for (vector<StarInfo>::const_iterator iter=level1.begin(),limit=level1.end();iter!=limit;++iter)
      mergeInfo[(*iter).steps]=(*iter).count;

   // Perform the merge
   result.clear();
   unsigned index1=0,index2=0;
   while (result.size()<k) {
      // Did we reach the end of one list?
      if (index1>=level1.size()) {
         if (index2>=level2.size()) break;
         if (couldMerge(mergeInfo,level2[index2].steps))
            result.push_back(level2[index2]);
         index2++;
         continue;
      }
      if (index2>=level2.size()) {
         result.push_back(level1[index1]);
         index1++;
         continue;
      }
      // No, perform a real merge
      if (level1[index1].count>=level2[index2].count) {
         result.push_back(level1[index1]);
         mergeInfo[level1[index1].steps]=0;
         continue;
      }
      if (couldMerge(mergeInfo,level2[index2].steps)) {
         doMerge(result,mergeInfo,level2[index2].steps,level2[index2].count);
      }
      index2++;
   }
   // Final check
   if (result.size()>k)
      result.resize(k);
}
//---------------------------------------------------------------------------
void buildStarStatisticsPage(Database& db,unsigned char* statisticPage)
   // Compute statistics about star patterns
{
   // Compute the initial seeds
   vector<StarInfo> pathLevel1;
   {
      Register regs[1];
      regs[0].reset();
      FullyAggregatedIndexScan* scan=FullyAggregatedIndexScan::create(db,Database::Order_Predicate_Subject_Object,0,false,regs,false,0,false);
      collectTopStars(maxPathPerPage,pathLevel1,scan,1,regs);
      delete scan;
   }

   // Compute the next level
   vector<StarInfo> pathLevel2;
   {
      // Compute predicate filters
      vector<unsigned> values;
      for (vector<StarInfo>::const_iterator iter=pathLevel1.begin(),limit=pathLevel1.end();iter!=limit;++iter)
         values.push_back(*((*iter).steps.begin()));
      sort(values.begin(),values.end());

      // And build an execution Plan
      Register regs[4];
      for (unsigned index=0;index<4;index++)
         regs[index].reset();
      AggregatedIndexScan* scan1=AggregatedIndexScan::create(db,Database::Order_Subject_Predicate_Object,regs+2,false,regs+0,false,0,false);
      Filter* filter1=new Filter(scan1,regs+0,values,false);
      AggregatedIndexScan* scan2=AggregatedIndexScan::create(db,Database::Order_Subject_Predicate_Object,regs+3,false,regs+1,false,0,false);
      Filter* filter2=new Filter(scan2,regs+1,values,false);
      vector<Register*> leftTail,rightTail,group;
      leftTail.push_back(regs+0); rightTail.push_back(regs+1);
      MergeJoin* join=new MergeJoin(filter1,regs+2,leftTail,filter2,regs+3,rightTail);
      group.push_back(regs+0); group.push_back(regs+1);
      HashGroupify* groupify=new HashGroupify(join,group);

      // Run it
      collectTopStars(maxPathPerPage,pathLevel2,groupify,2,regs);
      delete groupify;
   }

   // XXX integrate the full recursion algorithm

   // Merge the levels
   vector<StarInfo> result;
   mergeLevels(maxPathPerPage,result,pathLevel1,pathLevel2);

   // And fill the page
   memset(statisticPage,0,DatabaseBuilder::pageSize);
   unsigned count=0,ofs=4;
   for (vector<StarInfo>::const_iterator iter=result.begin(),limit=result.end();iter!=limit;++iter) {
      unsigned len=4+4+(4*(*iter).steps.size());
      if (ofs+len>DatabaseBuilder::pageSize)
         break;
      writeUint32(statisticPage+ofs,(*iter).steps.size()); ofs+=4;
      for (set<unsigned>::const_iterator iter2=(*iter).steps.begin(),limit2=(*iter).steps.end();iter2!=limit2;++iter2) {
         writeUint32(statisticPage+ofs,(*iter2)); ofs+=4;
      }
      writeUint32(statisticPage+ofs,(*iter).count); ofs+=4;
      count++;
   }
   writeUint32(statisticPage,count);
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
void DatabaseBuilder::computePathStatistics()
   // Compute statistics about frequent paths (after loading)
{
   // Open the database again
   Database db;
   if (!db.open(dbFile)) {
      cout << "Unable to open " << dbFile << endl;
      throw;
   }

   // Build the statistics pages
   unsigned char statisticPageChain[pageSize];
   buildChainStatisticsPage(db,statisticPageChain);
   unsigned char statisticPageStar[pageSize];
   buildStarStatisticsPage(db,statisticPageStar);

   // Close the database
   db.close();

   // And patch the statistics
   ofstream out(dbFile,ios::in|ios::out|ios::ate|ios::binary);
   if (!out.is_open()) {
      cout << "Unable to write " << dbFile << endl;
      throw;
   }
   out.seekp(static_cast<unsigned long long>(directory.pathStatistics[0])*pageSize,ios::beg);
   out.write(reinterpret_cast<char*>(statisticPageChain),pageSize);
   out.seekp(static_cast<unsigned long long>(directory.pathStatistics[1])*pageSize,ios::beg);
   out.write(reinterpret_cast<char*>(statisticPageStar),pageSize);
}
//---------------------------------------------------------------------------
