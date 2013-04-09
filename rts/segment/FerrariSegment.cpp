#include "rts/segment/FerrariSegment.hpp"
#include "rts/database/Database.hpp"
#include "rts/database/DatabaseBuilder.hpp"
#include "rts/database/DatabasePartition.hpp"
#include "rts/operator/MergeJoin.hpp"
#include "infra/util/fastlz.hpp"
#include "rts/operator/AggregatedIndexScan.hpp"
#include "rts/segment/FactsSegment.hpp"
#include "rts/segment/DictionarySegment.hpp"
#include "rts/segment/FullyAggregatedFactsSegment.hpp"
#include "rts/ferrari/Graph.hpp"
#include "rts/ferrari/Index.hpp"
#include "infra/osdep/Timestamp.hpp"
#include "rts/runtime/Runtime.hpp"
#include <algorithm>
#include <vector>
#include <iostream>
#include <cstring>
//---------------------------------------------------------------------------
// RDF-3X
// (c) 2013 Andrey Gubichev. Web site: http://www.mpi-inf.mpg.de/~neumann/rdf3x
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
// Info slots
static const unsigned slotDirectoryPage = 0;
//---------------------------------------------------------------------------
static bool contains(const vector<unsigned>& allNodes,unsigned id)
   // Is the id in the list?
{
   vector<unsigned>::const_iterator pos=lower_bound(allNodes.begin(),allNodes.end(),id);
   return ((pos!=allNodes.end())&&((*pos)==id));
}
//---------------------------------------------------------------------------
static string lookupId(Database& db,unsigned id)
   // Lookup a string id
{
   const char* start=0,*stop=0; Type::ID type; unsigned subType;
   db.getDictionary().lookupById(id,start,stop,type,subType);
   return string(start,stop);
}
//---------------------------------------------------------------------------
static void findPredicates(Database& db, vector<unsigned>& predicates){
	predicates.clear();
   Register ls,lo,lp,rs,ro,rp;
   ls.reset(); lp.reset(); lo.reset(); rs.reset(); rp.reset(); ro.reset();
   AggregatedIndexScan* scan1=AggregatedIndexScan::create(db,Database::Order_Object_Predicate_Subject,0,false,&lp,false,&lo,false,0);
   AggregatedIndexScan* scan2=AggregatedIndexScan::create(db,Database::Order_Subject_Predicate_Object,&rs,false,&rp,false,0,false,0);
   vector<Register*> lt,rt; lt.push_back(&lp); rt.push_back(&rp);
   MergeJoin join(scan1,&lo,lt,scan2,&rs,rt,0);

   map<unsigned,unsigned> predCount;
   if (join.first()) do {
      if (lp.value==rp.value){
      	predCount[lp.value]++;
      }
   } while (join.next());

   for (auto t:predCount){
   	cerr<<lookupId(db,t.first)<<" "<<t.second<<endl;
   	predicates.push_back(t.first);
   }
   cerr<<"#predicates: "<<predicates.size()<<endl;
}
//---------------------------------------------------------------------------
FerrariSegment::FerrariSegment(DatabasePartition& partition)
   : Segment(partition),directoryPage(0)
   // Constructor
{
}
//---------------------------------------------------------------------------
Segment::Type FerrariSegment::getType() const
   // Get the type
{
   return Segment::Type_Ferrari;
}
//---------------------------------------------------------------------------
/// Refresh segment info stored in the partition
void FerrariSegment::refreshInfo()
{
   Segment::refreshInfo();
   directoryPage=getSegmentData(slotDirectoryPage);
   BufferReference page(readShared(directoryPage));
   const unsigned char* directory=static_cast<const unsigned char*>(page.getPage());
   unsigned pred=readUint32(directory);
   cerr<<"number of predicates: "<<pred<<endl;
}
//---------------------------------------------------------------------------
class FerrariSegment::Dumper{
private:
   /// The maximum number of entries per page
   static const unsigned maxEntries = 32768;
   /// The segment
   FerrariSegment& seg;
   /// Page chainer
   DatabaseBuilder::PageChainer chainer;
   /// The entries
   vector<vector<unsigned> > &data;
   /// The current count
   unsigned count;
   /// Current offset in the data
   unsigned offset;
   /// Pages
   vector<unsigned>& pages;
   /// Write entries to a buffer
   bool writeEntries(unsigned start,unsigned count,unsigned char* pageBuffer);
   /// Write some entries
   unsigned writeSome();
public:
   /// Constructor
   Dumper(FerrariSegment& seg,vector<vector<unsigned> >& data,vector<unsigned>& pages) : seg(seg),chainer(8),data(data),count(0),offset(0),pages(pages) {}

   void write();
};
//---------------------------------------------------------------------------
void FerrariSegment::Dumper::write(){
	while (offset < data.size()){
		count=std::min(static_cast<unsigned>(data.size()-offset), maxEntries);
		offset=offset+writeSome();
		cerr<<"count: "<<count<<endl;
		cerr<<"offset: "<<offset<<endl;

	}
   chainer.finish();

}
//---------------------------------------------------------------------------
unsigned FerrariSegment::Dumper::writeSome(){
	// Find the maximum fill size
	unsigned char pageBuffer[2*BufferReference::pageSize];
	unsigned l=0,r=count,best=1;
	while (l<r) {
		unsigned m=(l+r)/2;
		if (writeEntries(offset,m+1,pageBuffer)) {
			if (m+1>best)
				best=m+1;
			l=m+1;
		} else
			r=m;
	}
   // Write the page
   writeEntries(offset,best,pageBuffer);
   chainer.store(&seg,pageBuffer);
   pages.push_back(chainer.getPageNo());
	count=best;
	return best;
}
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
bool FerrariSegment::Dumper::writeEntries(unsigned start,unsigned count,unsigned char* pageBuffer){
   // Refuse to handle empty ranges
   if (!count)
      return false;

   // Temp space
   static const unsigned maxSize=10*BufferReference::pageSize;
   unsigned char buffer1[maxSize+32];
   unsigned char buffer2[maxSize+(maxSize/15)];

   // Write the entries
   unsigned char* writer=buffer1,*limit=buffer1+maxSize;
   writer=writeUIntV(writer,count);
   for (unsigned i=start; i<start+count; i++){
   	writer=writeUIntV(writer,data[i].size());
   	if (writer>limit) return false;
   	for (unsigned j=0; j<data[i].size(); j++){
      	writer=writeUIntV(writer,data[i][j]);
      	if (writer>limit) return false;
   	}
   }

   // Compress them
   unsigned len=fastlz_compress(buffer1,writer-buffer1,buffer2);
   if (len>=(BufferReference::pageSize-16))
      return false;
   // And write the page
   writeUint32(pageBuffer+8,0);
   writeUint32(pageBuffer+12,len);
   cerr<<"len "<<len<<endl;
   memcpy(pageBuffer+16,buffer2,len);
   memset(pageBuffer+16+len,0,BufferReference::pageSize-(16+len));

   return true;
}
//static unsigned writeSome(DatabaseBuilder::PageChainer& chainer,vector<vector<unsigned> >& data){
//   // Find the maximum fill size
//   unsigned char pageBuffer[2*BufferReference::pageSize];
//   unsigned l=0,r=count,best=1;
//   while (l<r) {
//      unsigned m=(l+r)/2;
//      if (writeEntries(m+1,pageBuffer)) {
//         if (m+1>best)
//            best=m+1;
//         l=m+1;
//      } else {
//         r=m;
//      }
//   }
//
//}
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
static void packGraphInner(FerrariSegment& seg,const vector<unsigned>& data, vector<unsigned>& pages){
	// inner (page) nodes for the serialized graph
   const unsigned headerSize = 24; // LSN+marker+next+count+padding
   DatabaseBuilder::PageChainer chainer(12);
   unsigned char buffer[BufferReference::pageSize];
   unsigned bufferPos=headerSize,bufferCount=0;
   for (const auto iter:data) {
      // Do we have to start a new page?
      if ((bufferPos+12)>BufferReference::pageSize) {
         writeUint32(buffer+8,0xFFFFFFFF);
         writeUint32(buffer+16,bufferCount);
         writeUint32(buffer+20,0);
         for (unsigned index=bufferPos;index<BufferReference::pageSize;index++)
            buffer[index]=0;
         chainer.store(&seg,buffer);
         pages.push_back(chainer.getPageNo());
         bufferPos=headerSize; bufferCount=0;
      }
      // Write the entry
      writeUint32(buffer+bufferPos,(iter)); bufferPos+=4;
      bufferCount++;
   }
   // Write the last page
   writeUint32(buffer+8,0xFFFFFFFF);
   writeUint32(buffer+16,bufferCount);
   writeUint32(buffer+20,0);
   for (unsigned index=bufferPos;index<BufferReference::pageSize;index++)
      buffer[index]=0;
   chainer.store(&seg,buffer);
   pages.push_back(chainer.getPageNo());
   chainer.finish();
}
//---------------------------------------------------------------------------
unsigned FerrariSegment::packGraph(Graph* g){
   vector<unsigned> pages;
   cerr<<"data size: "<<g->nb.size()<<endl;
   FerrariSegment::Dumper d(*this,g->nb,pages);
   d.write();
   cerr<<"pages: "<<pages.size()<<endl;
   if (pages.size()==1){
   	vector<unsigned> newPages;
   	packGraphInner(*this,pages,newPages);
   	return newPages.back();
   }

   while (pages.size()>1){
   	vector<unsigned> newPages;
   	packGraphInner(*this,pages,newPages);
   	swap(pages,newPages);
   }
   return pages.back();
}

void FerrariSegment::computeFerrari(Database& db){
   unsigned nodeCount=0;
   {
      FullyAggregatedFactsSegment::Scan scan;
      if (scan.first(db.getFullyAggregatedFacts(Database::Order_Subject_Predicate_Object))) do {
      	if (scan.getValue1()>nodeCount)
      		nodeCount=scan.getValue1();
      } while (scan.next());
   }
   nodeCount++;
   cerr<<"nodes: "<<nodeCount<<endl;
   vector<unsigned> predicates;
   findPredicates(db, predicates);
   vector<Graph*> graphs;
   {
      FactsSegment::Scan scan;
      unsigned current=~0u;
      bool global = true;
      unsigned seeds=5;
      vector<pair<unsigned,unsigned> > edge_list;
      if (scan.first(db.getFacts(Database::Order_Predicate_Subject_Object),0,0,0)) do {
         // A new node?
         if (scan.getValue1()!=current) {
         	// add new Graph
         	if (~current&&contains(predicates,current)){
            	cerr<<"predicate: "<<lookupId(db,current)<<" "<<current<<endl;
            	Timestamp t1;
            	Graph* g = new Graph(edge_list, nodeCount);
            	Timestamp t2;
            	cerr<<"   time to build the graph: "<<t2-t1<<" ms"<<endl;
            	// construct an index
            	Timestamp a;
            	Index *bm = new Index(g, seeds, ~0u, global);
            	bm->build();
            	graphs.push_back(g);
         	}
            current=scan.getValue1();
         	edge_list.clear();
         }
         edge_list.push_back({scan.getValue2(),scan.getValue3()});
      }while (scan.next());
   }

   Graph* g=graphs[0];
   unsigned graphPacked=packGraph(g);
   // Write the directory page
   BufferReferenceModified page;
   allocPage(page);
   unsigned char* directory=static_cast<unsigned char*>(page.getPage());
   writeUint32(directory,graphPacked);
   directoryPage=page.getPageNo();
   page.unfixWithoutRecovery();
   setSegmentData(slotDirectoryPage,directoryPage);
}
