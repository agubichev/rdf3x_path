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
static void writePage(fstream& out,unsigned page,const void* data)
   // Write a page to the file
{
   unsigned long long ofs=static_cast<unsigned long long>(page)*static_cast<unsigned long long>(BufferManager::pageSize);
   if (static_cast<unsigned long long>(out.tellp())!=ofs) {
      cout << "internal error: tried to write page " << page << " (ofs " << ofs << ") at position " << out.tellp() << endl;
      throw;
   }
   out.write(static_cast<const char*>(data),BufferManager::pageSize);
}
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
   fstream& out;
   /// The current page
   unsigned& page;
   /// The entries
   Entry entries[maxEntries];
   /// The current count
   unsigned count;
   /// The page bounadries
   vector<pair<pair<unsigned,unsigned>,unsigned> >& boundaries;

   /// Write entries to a buffer
   bool writeEntries(unsigned count,unsigned char* pageBuffer,unsigned nextPage);
   /// Write some entries
   void writeSome(bool potentiallyLast);

   public:
   /// Constructor
   Dumper2(fstream& out,unsigned& page,vector<pair<pair<unsigned,unsigned>,unsigned> >& boundaries) : out(out),page(page),count(0),boundaries(boundaries) {}

   /// Add an entry
   void add(unsigned value1,unsigned value2,unsigned long long s,unsigned long long p,unsigned long long o);
   /// Flush pending entries
   void flush();
};
//---------------------------------------------------------------------------
static unsigned char* writeUIntV(unsigned char* writer,unsigned long long v)
   // Write a value with variable length
{
   while (v>=128) {
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
   writer=writeUIntV(writer,count);
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
   writePage(out,page,pageBuffer);
   boundaries.push_back(pair<pair<unsigned,unsigned>,unsigned>(pair<unsigned,unsigned>(entries[best-1].value1,entries[best-1].value2),page));
   page++;

   // And move the entries
   memmove(entries,entries+best,sizeof(Entry)*(count-best));
   count-=best;
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
/// Output for one-constant statistics
class Dumper1 {
   private:
   /// An entry
   struct Entry {
      /// The constant value
      unsigned value1;
      /// The join partners
      unsigned long long s1,p1,o1,s2,p2,o2;
   };
   /// The maximum number of entries per page
   static const unsigned maxEntries = 32768;

   /// The output
   fstream& out;
   /// The current page
   unsigned& page;
   /// The entries
   Entry entries[maxEntries];
   /// The current count
   unsigned count;
   /// The page bounadries
   vector<pair<unsigned,unsigned> >& boundaries;

   /// Write entries to a buffer
   bool writeEntries(unsigned count,unsigned char* pageBuffer,unsigned nextPage);
   /// Write some entries
   void writeSome(bool potentiallyLast);

   public:
   /// Constructor
   Dumper1(fstream& out,unsigned& page,vector<pair<unsigned,unsigned> >& boundaries) : out(out),page(page),count(0),boundaries(boundaries) {}

   /// Add an entry
   void add(unsigned value1,unsigned long long s1,unsigned long long p1,unsigned long long o1,unsigned long long s2,unsigned long long p2,unsigned long long o2);
   /// Flush pending entries
   void flush();
};
//---------------------------------------------------------------------------
bool Dumper1::writeEntries(unsigned count,unsigned char* pageBuffer,unsigned nextPage)
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
   writer=writeUIntV(writer,count);
   writer=writeUIntV(writer,entries[0].value1);
   for (unsigned index=1;index<count;index++) {
      writer=writeUIntV(writer,entries[index].value1-entries[index-1].value1);
      if (writer>limit) return false;
   }
   for (unsigned index=0;index<count;index++) {
      writer=writeUIntV(writer,entries[index].s1);
      if (writer>limit) return false;
   }
   for (unsigned index=0;index<count;index++) {
      writer=writeUIntV(writer,entries[index].p1);
      if (writer>limit) return false;
   }
   for (unsigned index=0;index<count;index++) {
      writer=writeUIntV(writer,entries[index].o1);
      if (writer>limit) return false;
   }
   for (unsigned index=0;index<count;index++) {
      writer=writeUIntV(writer,entries[index].s2);
      if (writer>limit) return false;
   }
   for (unsigned index=0;index<count;index++) {
      writer=writeUIntV(writer,entries[index].p2);
      if (writer>limit) return false;
   }
   for (unsigned index=0;index<count;index++) {
      writer=writeUIntV(writer,entries[index].o2);
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
void Dumper1::writeSome(bool potentiallyLast)
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
   writePage(out,page,pageBuffer);
   boundaries.push_back(pair<unsigned,unsigned>(entries[best-1].value1,page));
   page++;

   // And move the entries
   memmove(entries,entries+best,sizeof(Entry)*(count-best));
   count-=best;
}
//---------------------------------------------------------------------------
void Dumper1::add(unsigned value1,unsigned long long s1,unsigned long long p1,unsigned long long o1,unsigned long long s2,unsigned long long p2,unsigned long long o2)
   // Add an entry
{
   // Full? Then write some entries
   if (count==maxEntries)
      writeSome(false);

   // Append
   entries[count].value1=value1;
   entries[count].s1=s1;
   entries[count].p1=p1;
   entries[count].o1=o1;
   entries[count].s2=s2;
   entries[count].p2=p2;
   entries[count].o2=o2;
   ++count;
}
//---------------------------------------------------------------------------
void Dumper1::flush()
   // Flush pending entries
{
   while (count)
      writeSome(true);
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
static void buildCountMap(Database& db,const char* fileName)
   // Build a map with aggregated counts
{
   // Prepare the output file
   ofstream out(fileName,ios::out|ios::binary|ios::trunc);
   if (!out.is_open()) {
      cerr << "unable to write " << fileName << endl;
      throw;
   }

   // Scan all aggregated indices at once
   FullyAggregatedFactsSegment::Scan scanS,scanO,scanP;
   bool doneS=!scanS.first(db.getFullyAggregatedFacts(Database::Order_Subject_Predicate_Object));
   bool doneP=!scanP.first(db.getFullyAggregatedFacts(Database::Order_Predicate_Subject_Object));
   bool doneO=!scanO.first(db.getFullyAggregatedFacts(Database::Order_Object_Subject_Predicate));
   for (unsigned id=0;(!doneS)||(!doneP)||(!doneO);++id) {
      unsigned counts[3];
      if (doneS||(scanS.getValue1()>id)) {
         counts[0]=0;
      } else {
         counts[0]=scanS.getCount();
         doneS=!scanS.next();
      }
      if (doneP||(scanP.getValue1()>id)) {
         counts[1]=0;
      } else {
         counts[1]=scanP.getCount();
         doneP=!scanP.next();
      }
      if (doneO||(scanO.getValue1()>id)) {
         counts[2]=0;
      } else {
         counts[2]=scanO.getCount();
         doneO=!scanO.next();
      }
      out.write(reinterpret_cast<char*>(counts),3*sizeof(unsigned));
   }
   out.flush();
}
//---------------------------------------------------------------------------
static void addCounts(const char* countMap,unsigned id,unsigned long long multiplicity,unsigned long long& countS,unsigned long long & countP,unsigned long long& countO)
   // Add all counts
{
   const unsigned* base=reinterpret_cast<const unsigned*>(countMap)+(3*id);
   countS+=multiplicity*static_cast<unsigned long long>(base[0]);
   countP+=multiplicity*static_cast<unsigned long long>(base[1]);
   countO+=multiplicity*static_cast<unsigned long long>(base[2]);
}
//---------------------------------------------------------------------------
static void computeExact2Leaves(Database& db,fstream& out,unsigned& page,vector<pair<pair<unsigned,unsigned>,unsigned> >& boundaries,Database::DataOrder order,const char* countMap)
   // Compute the exact statistics for patterns with two constants
{
   Dumper2 dumper(out,page,boundaries);

   FactsSegment::Scan scan;
   if (scan.first(db.getFacts(order))) {
      // And scan
      unsigned last1=~0u,last2=~0u;
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
         }
         // Add entries
         addCounts(countMap,scan.getValue3(),1,countS,countP,countO);
      } while (scan.next());
      // Add the last entry
      if (~last1) {
         dumper.add(last1,last2,countS,countP,countO);
      }
   }

   // Write pending entries if any
   dumper.flush();
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
static void writeUint64(unsigned char* target,unsigned long long value)
   // Write a 64bit value
{
   for (unsigned index=0;index<8;index++)
      target[index]=static_cast<unsigned char>((value>>(8*(7-index)))&0xFF);
}
//---------------------------------------------------------------------------
static unsigned computeExact2Inner(fstream& out,const vector<pair<pair<unsigned,unsigned>,unsigned> >& data,vector<pair<pair<unsigned,unsigned>,unsigned> >& boundaries,unsigned page)
   // Create inner nodes
{
   const unsigned headerSize = 16; // marker+next+count+padding
   unsigned char buffer[BufferManager::pageSize];
   unsigned bufferPos=headerSize,bufferCount=0;

   for (vector<pair<pair<unsigned,unsigned>,unsigned> >::const_iterator iter=data.begin(),limit=data.end();iter!=limit;++iter) {
      // Do we have to start a new page?
      if ((bufferPos+12)>BufferManager::pageSize) {
         writeUint32(buffer,0xFFFFFFFF);
         writeUint32(buffer+4,page+1);
         writeUint32(buffer+8,bufferCount);
         writeUint32(buffer+12,0);
         for (unsigned index=bufferPos;index<BufferManager::pageSize;index++)
            buffer[index]=0;
         writePage(out,page,buffer);
         boundaries.push_back(pair<pair<unsigned,unsigned>,unsigned>((*(iter-1)).first,page));
         ++page;
         bufferPos=headerSize; bufferCount=0;
      }
      // Write the entry
      writeUint32(buffer+bufferPos,(*iter).first.first); bufferPos+=4;
      writeUint32(buffer+bufferPos,(*iter).first.second); bufferPos+=4;
      writeUint32(buffer+bufferPos,(*iter).second); bufferPos+=4;
      bufferCount++;
   }
   // Write the least page
   writeUint32(buffer,0xFFFFFFFF);
   writeUint32(buffer+4,0);
   writeUint32(buffer+8,bufferCount);
   writeUint32(buffer+12,0);
   for (unsigned index=bufferPos;index<BufferManager::pageSize;index++)
      buffer[index]=0;
   writePage(out,page,buffer);
   boundaries.push_back(pair<pair<unsigned,unsigned>,unsigned>(data.back().first,page));
   ++page;

   return page;
}
//---------------------------------------------------------------------------
static unsigned computeExact2(Database& db,fstream& out,unsigned& page,Database::DataOrder order,const char* countMap)
   // Compute the exact statistics for patterns with two constants
{
   // Write the leave nodes
   vector<pair<pair<unsigned,unsigned>,unsigned> > boundaries;
   computeExact2Leaves(db,out,page,boundaries,order,countMap);

   // Only one leaf node? Special case this
   if (boundaries.size()==1) {
      vector<pair<pair<unsigned,unsigned>,unsigned> > newBoundaries;
      page=computeExact2Inner(out,boundaries,newBoundaries,page);
      return page-1;
   }

   // Write the inner nodes
   while (boundaries.size()>1) {
      vector<pair<pair<unsigned,unsigned>,unsigned> > newBoundaries;
      page=computeExact2Inner(out,boundaries,newBoundaries,page);
      swap(boundaries,newBoundaries);
   }
   return page-1;
}
//---------------------------------------------------------------------------
static void computeExact1Leaves(Database& db,fstream& out,unsigned& page,vector<pair<unsigned,unsigned> >& boundaries,Database::DataOrder order1,Database::DataOrder order2,const char* countMap)
   // Compute the exact statistics for patterns with one constant
{
   Dumper1 dumper(out,page,boundaries);

   AggregatedFactsSegment::Scan scan1,scan2;
   if (scan1.first(db.getAggregatedFacts(order1))&&scan2.first(db.getAggregatedFacts(order2))) {
      // Scan
      bool done=false;
      while (!done) {
         // Read scan1
         unsigned last1=scan1.getValue1();
         unsigned long long countS1=0,countP1=0,countO1=0;
         while (true) {
            if (scan1.getValue1()!=last1)
               break;
            addCounts(countMap,scan1.getValue2(),scan1.getCount(),countS1,countP1,countO1);
            if (!scan1.next()) {
               done=true;
               break;
            }
         }

         // Read scan2
         unsigned last2=scan2.getValue1();
         unsigned long long countS2=0,countP2=0,countO2=0;
         while (true) {
            if (scan2.getValue1()!=last2)
               break;
            addCounts(countMap,scan2.getValue2(),scan2.getCount(),countS2,countP2,countO2);
            if (!scan2.next()) {
               done=true;
               break;
            }
         }

         // Produce output tuple
         assert(last1==last2);
         dumper.add(last1,countS1,countP1,countO1,countS2,countP2,countO2);
      }
   }

   // Write pending entries if any
   dumper.flush();
}
//---------------------------------------------------------------------------
static unsigned computeExact1Inner(fstream& out,const vector<pair<unsigned,unsigned> >& data,vector<pair<unsigned,unsigned> >& boundaries,unsigned page)
   // Create inner nodes
{
   const unsigned headerSize = 16; // marker+next+count+padding
   unsigned char buffer[BufferManager::pageSize];
   unsigned bufferPos=headerSize,bufferCount=0;

   for (vector<pair<unsigned,unsigned> >::const_iterator iter=data.begin(),limit=data.end();iter!=limit;++iter) {
      // Do we have to start a new page?
      if ((bufferPos+8)>BufferManager::pageSize) {
         writeUint32(buffer,0xFFFFFFFF);
         writeUint32(buffer+4,page+1);
         writeUint32(buffer+8,bufferCount);
         writeUint32(buffer+12,0);
         for (unsigned index=bufferPos;index<BufferManager::pageSize;index++)
            buffer[index]=0;
         writePage(out,page,buffer);
         boundaries.push_back(pair<unsigned,unsigned>((*(iter-1)).first,page));
         ++page;
         bufferPos=headerSize; bufferCount=0;
      }
      // Write the entry
      writeUint32(buffer+bufferPos,(*iter).first); bufferPos+=4;
      writeUint32(buffer+bufferPos,(*iter).second); bufferPos+=4;
      bufferCount++;
   }
   // Write the least page
   writeUint32(buffer,0xFFFFFFFF);
   writeUint32(buffer+4,0);
   writeUint32(buffer+8,bufferCount);
   writeUint32(buffer+12,0);
   for (unsigned index=bufferPos;index<BufferManager::pageSize;index++)
      buffer[index]=0;
   writePage(out,page,buffer);
   boundaries.push_back(pair<unsigned,unsigned>(data.back().first,page));
   ++page;

   return page;
}
//---------------------------------------------------------------------------
static unsigned computeExact1(Database& db,fstream& out,unsigned& page,Database::DataOrder order1,Database::DataOrder order2,const char* countMap)
   // Compute the exact statistics for patterns with one constant
{
   // Write the leave nodes
   vector<pair<unsigned,unsigned> > boundaries;
   computeExact1Leaves(db,out,page,boundaries,order1,order2,countMap);

   // Only one leaf node? Special case this
   if (boundaries.size()==1) {
      vector<pair<unsigned,unsigned> > newBoundaries;
      page=computeExact1Inner(out,boundaries,newBoundaries,page);
      return page-1;
   }

   // Write the inner nodes
   while (boundaries.size()>1) {
      vector<pair<unsigned,unsigned> > newBoundaries;
      page=computeExact1Inner(out,boundaries,newBoundaries,page);
      swap(boundaries,newBoundaries);
   }
   return page-1;
}
//---------------------------------------------------------------------------
static unsigned long long computeExact0(MemoryMappedFile& countMap,unsigned ofs1,unsigned ofs2)
   // Compute the exact statistics for patterns without constants
{
   const unsigned* data=reinterpret_cast<const unsigned*>(countMap.getBegin());
   const unsigned* limit=reinterpret_cast<const unsigned*>(countMap.getEnd());
   unsigned long long result=0;
   for (const unsigned* iter=data;iter<limit;iter+=3) {
      result+=static_cast<unsigned long long>(iter[ofs1])*static_cast<unsigned long long>(iter[ofs2]);
   }
   return result;
}
//---------------------------------------------------------------------------
void DatabaseBuilder::computeExactStatistics(const char* tmpFile)
   // Compute exact statistics (after loading)
{
   // Open the database again
   Database db;
   if (!db.open(dbFile)) {
      cout << "Unable to open " << dbFile << endl;
      throw;
   }

   // Build aggregated count map
   buildCountMap(db,tmpFile);
   MemoryMappedFile countMap;
   if (!countMap.open(tmpFile)) {
      cout << "Unable to open " << tmpFile << endl;
      throw;
   }

   // Prepare for appending
   fstream out(dbFile,ios::in|ios::out|ios::ate|ios::binary);
   if (!out.is_open()) {
      cout << "Unable to write " << dbFile << endl;
      throw;
   }
   unsigned page=out.tellp()/pageSize;

   // Compute the exact 2 statistics
   unsigned exactPS=computeExact2(db,out,page,Database::Order_Predicate_Subject_Object,countMap.getBegin());
   unsigned exactPO=computeExact2(db,out,page,Database::Order_Predicate_Object_Subject,countMap.getBegin());
   unsigned exactSO=computeExact2(db,out,page,Database::Order_Subject_Object_Predicate,countMap.getBegin());

   // Compute the exact 1 statistics
   unsigned exactS=computeExact1(db,out,page,Database::Order_Subject_Predicate_Object,Database::Order_Subject_Object_Predicate,countMap.getBegin());
   unsigned exactP=computeExact1(db,out,page,Database::Order_Predicate_Subject_Object,Database::Order_Predicate_Object_Subject,countMap.getBegin());
   unsigned exactO=computeExact1(db,out,page,Database::Order_Object_Subject_Predicate,Database::Order_Object_Predicate_Subject,countMap.getBegin());

   // Compute the exact 0 statistics
   unsigned long long exact0SS=computeExact0(countMap,0,0);
   unsigned long long exact0SP=computeExact0(countMap,0,1);
   unsigned long long exact0SO=computeExact0(countMap,0,2);
   unsigned long long exact0PS=computeExact0(countMap,1,0);
   unsigned long long exact0PP=computeExact0(countMap,1,1);
   unsigned long long exact0PO=computeExact0(countMap,1,2);
   unsigned long long exact0OS=computeExact0(countMap,2,0);
   unsigned long long exact0OP=computeExact0(countMap,2,1);
   unsigned long long exact0OO=computeExact0(countMap,2,2);

   // Update the directory page
   unsigned char directory[BufferManager::pageSize];
   out.seekp(0,ios_base::beg);
   out.read(reinterpret_cast<char*>(directory),BufferManager::pageSize);
   unsigned char* base=directory+292;
   writeUint32(base,exactPS);
   writeUint32(base+4,exactPO);
   writeUint32(base+8,exactSO);
   writeUint32(base+12,exactS);
   writeUint32(base+16,exactP);
   writeUint32(base+20,exactO);
   writeUint64(base+24,exact0SS);
   writeUint64(base+32,exact0SP);
   writeUint64(base+40,exact0SO);
   writeUint64(base+48,exact0PS);
   writeUint64(base+56,exact0PP);
   writeUint64(base+64,exact0PO);
   writeUint64(base+72,exact0OS);
   writeUint64(base+80,exact0OP);
   writeUint64(base+88,exact0OO);
   out.seekp(0,ios_base::beg);
   writePage(out,0,directory);

   // Close the database
   out.flush();
   out.close();
   db.close();
   countMap.close();
   remove(tmpFile);
}
//---------------------------------------------------------------------------
