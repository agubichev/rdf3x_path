#include "rts/database/DatabaseBuilder.hpp"
#include "rts/database/Database.hpp"
#include "rts/segment/AggregatedFactsSegment.hpp"
#include "rts/segment/FactsSegment.hpp"
#include "rts/runtime/Runtime.hpp"
#include "rts/operator/FullyAggregatedIndexScan.hpp"
#include "infra/util/fastlz.hpp"
#include <iostream>
#include <cassert>
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
namespace {
//---------------------------------------------------------------------------
/// Output for two-constant statistics
class Dumper2 {
   private:
   /// An entry
   struct Entry {
      /// The constant values
      unsigned value1,value2;
      /// The join partners
      unsigned long long s,p,o;
   };
   /// The maximum number of entries per page
   static const unsigned maxEntries = 32768;

   /// The output
   ofstream& out;
   /// The current page
   unsigned& page;
   /// The entries
   Entry entries[maxEntries];
   /// The current count
   unsigned count;

   /// Write entries to a buffer
   bool writeEntries(unsigned count,unsigned char* pageBuffer,unsigned nextPage);
   /// Write some entries
   void writeSome(bool potentiallyLast);

   public:
   /// Constructor
   Dumper2(ofstream& out,unsigned& page) : out(out),page(page),count(0) {}

   /// Add an entry
   void add(unsigned value1,unsigned value2,unsigned long long s,unsigned long long p,unsigned long long o);
   /// Flush pending entries
   void flush();
};
//---------------------------------------------------------------------------
static unsigned char* writeUIntV(unsigned char* writer,unsigned long long v)
   // Write a value with variable length
{
   while (v>128) {
      *writer=static_cast<unsigned char>((v&0x7F)|0x80);
      v>>=7;
      ++writer;
   }
   *writer=static_cast<unsigned char>(v);
   return writer+1;
}
//---------------------------------------------------------------------------
bool Dumper2::writeEntries(unsigned count,unsigned char* pageBuffer,unsigned nextPage)
   // Write a page
{
   // Refuse to handle empty ranges
   if (!count)
      return false;

   // Temp space
   static const unsigned maxSize=10*BufferManager::pageSize;
   unsigned char buffer1[maxSize+32];
   unsigned char buffer2[maxSize+(maxSize/15)];

   // Write the entries
   unsigned char* writer=buffer1,*limit=buffer1+maxSize;
   writer=writeUIntV(writer,entries[0].value1);
   for (unsigned index=1;index<count;index++) {
      writer=writeUIntV(writer,entries[index].value1-entries[index-1].value1);
      if (writer>limit) return false;
   }
   unsigned last=~0u;
   for (unsigned index=0;index<count;index++) {
      unsigned long long v=static_cast<unsigned long long>(entries[index].value2)<<1;
      if (entries[index].value2<last)
         v=(static_cast<unsigned long long>(entries[index].value2)<<1)|1; else
         v=static_cast<unsigned long long>(entries[index].value2-last)<<1;
      last=entries[index].value2;
      writer=writeUIntV(writer,v);
      if (writer>limit) return false;
   }
   for (unsigned index=0;index<count;index++) {
      writer=writeUIntV(writer,entries[index].s);
      if (writer>limit) return false;
   }
   for (unsigned index=0;index<count;index++) {
      writer=writeUIntV(writer,entries[index].p);
      if (writer>limit) return false;
   }
   for (unsigned index=0;index<count;index++) {
      writer=writeUIntV(writer,entries[index].o);
      if (writer>limit) return false;
   }

   // Compress them
   unsigned len=fastlz_compress(buffer1,writer-buffer1,buffer2);
   if (len>=(BufferManager::pageSize-8))
      return false;

   // And write the page
   pageBuffer[0]=(nextPage>>24)&0xFF;
   pageBuffer[1]=(nextPage>>16)&0xFF;
   pageBuffer[2]=(nextPage>>8)&0xFF;
   pageBuffer[3]=(nextPage>>0)&0xFF;
   pageBuffer[4]=(len>>24)&0xFF;
   pageBuffer[5]=(len>>16)&0xFF;
   pageBuffer[6]=(len>>8)&0xFF;
   pageBuffer[7]=(len>>0)&0xFF;
   memcpy(pageBuffer+8,buffer2,len);
   memset(pageBuffer+8+len,0,BufferManager::pageSize-(8+len));

   return true;
}
//---------------------------------------------------------------------------
void Dumper2::writeSome(bool potentiallyLast)
   /// Write some entries
{
   // Find the maximum fill size
   unsigned char pageBuffer[2*BufferManager::pageSize];
   unsigned l=0,r=count,best=1;
   while (l<r) {
      unsigned m=(l+r)/2;
      if (writeEntries(m+1,pageBuffer,0)) {
         if (m+1>best)
            best=m+1;
         l=m+1;
      } else {
         r=m;
      }
   }
   // Write the page
   if (best<count)
      potentiallyLast=0;
   writeEntries(best,pageBuffer,potentiallyLast?0:(page+1));
   out.write(reinterpret_cast<char*>(pageBuffer),BufferManager::pageSize);
   page++;

   // And move the entries
   memmove(entries,entries+best,sizeof(Entry)*(count-best));
   count-=best;

   cout << "packed " << best << " entries into one page" << std::endl;
}
//---------------------------------------------------------------------------
void Dumper2::add(unsigned value1,unsigned value2,unsigned long long s,unsigned long long p,unsigned long long o)
   // Add an entry
{
   // Full? Then write some entries
   if (count==maxEntries)
      writeSome(false);

   // Append
   entries[count].value1=value1;
   entries[count].value2=value2;
   entries[count].s=s;
   entries[count].p=p;
   entries[count].o=o;
   ++count;
}
//---------------------------------------------------------------------------
void Dumper2::flush()
   // Flush pending entries
{
   while (count)
      writeSome(true);
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
static void computeExact2Leaves(Database& db,ofstream& out,unsigned& page,Database::DataOrder order)
   // Compute the exact statistics for patterns with two constants
{
   Dumper2 dumper(out,page);

   FactsSegment::Scan scan;
   if (scan.first(db.getFacts(order))) {
      // Prepare scanning the aggregated indices
      Register mergeValue,valueS,valueP,valueO;
      mergeValue.reset(); valueS.reset(); valueP.reset(); valueO.reset();
      FullyAggregatedIndexScan* scanS=FullyAggregatedIndexScan::create(db,Database::Order_Subject_Predicate_Object,&valueS,false,0,false,0,false);
      FullyAggregatedIndexScan* scanP=FullyAggregatedIndexScan::create(db,Database::Order_Predicate_Subject_Object,0,false,&valueP,false,0,false);
      FullyAggregatedIndexScan* scanO=FullyAggregatedIndexScan::create(db,Database::Order_Object_Subject_Predicate,0,false,0,false,&valueO,false);
      scanS->addMergeHint(&mergeValue,&valueS);
      scanP->addMergeHint(&mergeValue,&valueP);
      scanO->addMergeHint(&mergeValue,&valueO);

      // And scan
      unsigned last1=~0u,last2=~0u;
      unsigned lastS=0,lastP=0,lastO=0;
      unsigned long long countS=0,countP=0,countO=0;
      do {
         // A new entry?
         if ((scan.getValue1()!=last1)||(scan.getValue2()!=last2)) {
            if (~last1) {
               dumper.add(last1,last2,countS,countP,countO);
            }
            last1=scan.getValue1();
            last2=scan.getValue2();
            countS=0;
            countP=0;
            countO=0;
            mergeValue.value=scan.getValue3();
            lastS=scanS->first();
            lastP=scanP->first();
            lastO=scanO->first();
         }
         // Compare
         mergeValue.value=scan.getValue3();
         while (lastS) {
            if (mergeValue.value==valueS.value) {
               countS+=lastS;
               break;
            }
            if (mergeValue.value<valueS.value)
               break;
            lastS=scanS->next();
         }
         while (lastP) {
            if (mergeValue.value==valueP.value) {
               countP+=lastP;
               break;
            }
            if (mergeValue.value<valueP.value)
               break;
            lastP=scanP->next();
         }
         while (lastO) {
            if (mergeValue.value==valueO.value) {
               countO+=lastO;
               break;
            }
            if (mergeValue.value<valueO.value)
               break;
            lastO=scanO->next();
         }
      } while (scan.next());
      // Add the last entry
      if (~last1) {
         dumper.add(last1,last2,countS,countP,countO);
      }
      // Cleanup
      delete scanS;
      delete scanP;
      delete scanO;
   }

   // Write pending entries if any
   dumper.flush();
}
//---------------------------------------------------------------------------
static unsigned computeExact2(Database& db,ofstream& out,unsigned& page,Database::DataOrder order)
   // Compute the exact statistics for patterns with two constants
{
   computeExact2Leaves(db,out,page,order);
   return 0;
}
//---------------------------------------------------------------------------
static unsigned collectMatches(FullyAggregatedIndexScan* scan,bool first,const Register& v1,const Register& v2,unsigned& c)
   // Collect matching entries
{
   // First?
   if (first) {
      c=scan->first();
      if (!c) return 0;
   }
   while (true) {
      // Compare
      if (v1.value==v2.value)
         return c;
      if (v1.value<v2.value)
         return 0;
      c=scan->next();
      if (!c) return 0;
   }
}
//---------------------------------------------------------------------------
static void computeExact1Leaves(Database& db,ofstream& /*out*/,unsigned& /*page*/,Database::DataOrder order1,Database::DataOrder order2)
   // Compute the exact statistics for patterns with one constant
{
   AggregatedFactsSegment::Scan scan1,scan2;
   if (scan1.first(db.getAggregatedFacts(order1))&&scan2.first(db.getAggregatedFacts(order2))) {
      // Prepare scanning the aggregated indices
      Register mergeValue1,valueS1,valueP1,valueO1;
      mergeValue1.reset(); valueS1.reset(); valueP1.reset(); valueO1.reset();
      FullyAggregatedIndexScan* scanS1=FullyAggregatedIndexScan::create(db,Database::Order_Subject_Predicate_Object,&valueS1,false,0,false,0,false);
      FullyAggregatedIndexScan* scanP1=FullyAggregatedIndexScan::create(db,Database::Order_Predicate_Subject_Object,0,false,&valueP1,false,0,false);
      FullyAggregatedIndexScan* scanO1=FullyAggregatedIndexScan::create(db,Database::Order_Object_Subject_Predicate,0,false,0,false,&valueO1,false);
      scanS1->addMergeHint(&mergeValue1,&valueS1);
      scanP1->addMergeHint(&mergeValue1,&valueP1);
      scanO1->addMergeHint(&mergeValue1,&valueO1);
      Register mergeValue2,valueS2,valueP2,valueO2;
      mergeValue2.reset(); valueS2.reset(); valueP2.reset(); valueO2.reset();
      FullyAggregatedIndexScan* scanS2=FullyAggregatedIndexScan::create(db,Database::Order_Subject_Predicate_Object,&valueS2,false,0,false,0,false);
      FullyAggregatedIndexScan* scanP2=FullyAggregatedIndexScan::create(db,Database::Order_Predicate_Subject_Object,0,false,&valueP2,false,0,false);
      FullyAggregatedIndexScan* scanO2=FullyAggregatedIndexScan::create(db,Database::Order_Object_Subject_Predicate,0,false,0,false,&valueO2,false);
      scanS2->addMergeHint(&mergeValue2,&valueS2);
      scanP2->addMergeHint(&mergeValue2,&valueP2);
      scanO2->addMergeHint(&mergeValue2,&valueO2);

      // And scan
      bool done=false;
      while (!done) {
         // Read scan1
         unsigned last1=scan1.getValue1(),cs1,cp1,co1;
         unsigned long long countS1=0,countP1=0,countO1=0;
         bool first1=true;
         while (true) {
            if (scan1.getValue1()!=last1)
               break;
            mergeValue1.value=scan1.getValue2();
            countS1+=collectMatches(scanS1,first1,mergeValue1,valueS1,cs1);
            countP1+=collectMatches(scanP1,first1,mergeValue1,valueP1,cp1);
            countO1+=collectMatches(scanO1,first1,mergeValue1,valueO1,co1);
            first1=false;
            if (!scan1.next()) {
               done=true;
               break;
            }
         }

         // Read scan2
         unsigned last2=scan2.getValue1(),cs2,cp2,co2;
         unsigned long long countS2=0,countP2=0,countO2=0;
         bool first2=true;
         while (true) {
            if (scan2.getValue1()!=last2)
               break;
            mergeValue2.value=scan2.getValue2();
            countS2+=collectMatches(scanS2,first2,mergeValue2,valueS2,cs2);
            countP2+=collectMatches(scanP2,first2,mergeValue2,valueP2,cp2);
            countO2+=collectMatches(scanO2,first2,mergeValue2,valueO2,co2);
            first2=false;
            if (!scan2.next()) {
               done=true;
               break;
            }
         }

         // Produce output tuple
         assert(last1==last2);
         cout << last1 << " " << countS1 << " " << countP1 << " " << countO1 << " " << countS2 << " " << countP2 << " " << countO2 << std::endl;
      }

      // Cleanup
      delete scanS1;
      delete scanP1;
      delete scanO1;
      delete scanS2;
      delete scanP2;
      delete scanO2;
   }
}
//---------------------------------------------------------------------------
static unsigned computeExact1(Database& db,ofstream& out,unsigned& page,Database::DataOrder order1,Database::DataOrder order2)
   // Compute the exact statistics for patterns with one constant
{
   computeExact1Leaves(db,out,page,order1,order2);
   return 0;
}
//---------------------------------------------------------------------------
static unsigned long long computeExact0(Database& db,Database::DataOrder order1,Database::DataOrder order2)
   // Compute the exact statistics for patterns without constants
{
   FullyAggregatedFactsSegment::Scan scan1,scan2;
   if (scan1.first(db.getFullyAggregatedFacts(order1))&&scan2.first(db.getFullyAggregatedFacts(order2))) {
      unsigned long long result=0;
      while (true) {
         if (scan1.getValue1()<scan2.getValue1()) {
            if (!scan1.next()) break;
         } else if (scan1.getValue1()>scan2.getValue1()) {
            if (!scan2.next()) break;
         } else {
            result+=static_cast<unsigned long long>(scan1.getCount())*
                    static_cast<unsigned long long>(scan2.getCount());
            if (!scan1.next()) break;
            if (!scan2.next()) break;
         }
      }
      return result;
   } else {
      return 0;
   }
}
//---------------------------------------------------------------------------
void DatabaseBuilder::computeExactStatistics()
   // Compute exact statistics (after loading)
{
   // Open the database again
   Database db;
   if (!db.open(dbFile)) {
      cout << "Unable to open " << dbFile << endl;
      throw;
   }

   // Prepare for appending
   ofstream out(dbFile,ios::in|ios::out|ios::ate|ios::binary);
   if (!out.is_open()) {
      cout << "Unable to write " << dbFile << endl;
      throw;
   }
   unsigned page=out.tellp()/pageSize;

   // Compute the exact 2 statistics
   unsigned exactPS=computeExact2(db,out,page,Database::Order_Predicate_Subject_Object);
   unsigned exactPO=computeExact2(db,out,page,Database::Order_Predicate_Object_Subject);
   unsigned exactSO=computeExact2(db,out,page,Database::Order_Subject_Object_Predicate);

   // Compute the exact 1 statistics
   unsigned exactS=computeExact1(db,out,page,Database::Order_Subject_Predicate_Object,Database::Order_Subject_Object_Predicate);
   unsigned exactP=computeExact1(db,out,page,Database::Order_Predicate_Subject_Object,Database::Order_Predicate_Object_Subject);
   unsigned exactO=computeExact1(db,out,page,Database::Order_Object_Subject_Predicate,Database::Order_Object_Predicate_Subject);

   // Compute the exact 0 statistics
   unsigned long long exact0SS=computeExact0(db,Database::Order_Subject_Predicate_Object,Database::Order_Subject_Predicate_Object);
   unsigned long long exact0SP=computeExact0(db,Database::Order_Subject_Predicate_Object,Database::Order_Predicate_Subject_Object);
   unsigned long long exact0SO=computeExact0(db,Database::Order_Subject_Predicate_Object,Database::Order_Object_Subject_Predicate);
   unsigned long long exact0PS=computeExact0(db,Database::Order_Predicate_Subject_Object,Database::Order_Subject_Predicate_Object);
   unsigned long long exact0PP=computeExact0(db,Database::Order_Predicate_Subject_Object,Database::Order_Predicate_Subject_Object);
   unsigned long long exact0PO=computeExact0(db,Database::Order_Predicate_Subject_Object,Database::Order_Object_Subject_Predicate);
   unsigned long long exact0OS=computeExact0(db,Database::Order_Object_Subject_Predicate,Database::Order_Subject_Predicate_Object);
   unsigned long long exact0OP=computeExact0(db,Database::Order_Object_Subject_Predicate,Database::Order_Predicate_Subject_Object);
   unsigned long long exact0OO=computeExact0(db,Database::Order_Object_Subject_Predicate,Database::Order_Object_Subject_Predicate);
   cout << exact0SS << " " << exact0SP << " " << exact0SO << " " << exact0PS << " " << exact0PP << " " << exact0PO << " " << exact0OS << " " << exact0OP << " " << exact0OO << std::endl;

   // Update the directory page XXX
//   out.seekp(static_cast<unsigned long long>(directory.statistics[order])*pageSize,ios::beg);
//   out.write(reinterpret_cast<char*>(statisticPage),pageSize);
cout << exactPS << " " << exactPO << " " << exactSO << " " << exactS << " " << exactP << " " << exactO << endl;

   // Close the database
   out.flush();
   out.close();
   db.close();

}
//---------------------------------------------------------------------------
