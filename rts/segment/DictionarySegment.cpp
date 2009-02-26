#include "rts/segment/DictionarySegment.hpp"
#include "rts/buffer/BufferReference.hpp"
#include "rts/database/DatabaseBuilder.hpp"
#include "infra/util/Hash.hpp"
#include <vector>
#include <cstring>
#include <iostream>
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
// Info slots
static const unsigned slotTableStart = 0;
static const unsigned slotNextId = 1;
static const unsigned slotMappingStart = 2;
static const unsigned slotIndexRoot = 3;
//---------------------------------------------------------------------------
/// Helper functions
static inline unsigned readInnerHash(const unsigned char* page,unsigned slot) { return Segment::readUint32Aligned(page+24+8*slot); }
static inline unsigned readInnerPage(const unsigned char* page,unsigned slot) { return Segment::readUint32Aligned(page+24+8*slot+4); }
static inline unsigned readLeafHash(const unsigned char* page,unsigned slot) { return Segment::readUint32Aligned(page+16+8*slot); }
static inline unsigned readLeafPage(const unsigned char* page,unsigned slot) { return Segment::readUint32Aligned(page+16+8*slot+4); }
//---------------------------------------------------------------------------
DictionarySegment::DictionarySegment(DatabasePartition& partition)
   : Segment(partition),tableStart(0),nextId(0),mappingStart(0),indexRoot(0)
   // Constructor
{
}
//---------------------------------------------------------------------------
Segment::Type DictionarySegment::getType() const
   // Get the type
{
   return Segment::Type_Dictionary;
}
//---------------------------------------------------------------------------
void DictionarySegment::refreshInfo()
   // Refresh segment info stored in the partition
{
   tableStart=getSegmentData(slotTableStart);
   nextId=getSegmentData(slotNextId);
   mappingStart=getSegmentData(slotMappingStart);
   indexRoot=getSegmentData(slotIndexRoot);
}
//---------------------------------------------------------------------------
bool DictionarySegment::lookupOnPage(unsigned pageNo,const string& text,unsigned hash,unsigned& id)
   // Lookup an id for a given string on a certain page in the raw string table
{
   BufferReference ref(readShared(pageNo));
   const unsigned char* page=static_cast<const unsigned char*>(ref.getPage());
   unsigned count=readUint32Aligned(page+12),pos=16;

   for (unsigned index=0;index<count;index++) {
      if (pos+12>BufferReference::pageSize)
         break;
      unsigned len=readUint32(page+pos+8);
      if ((readUint32(page+pos+4)==hash)&&(len==text.length())) {
         // Check if the string is really identical
         if (memcmp(page+pos+12,text.c_str(),len)==0) {
            id=readUint32(page+pos);
            return true;
         }
      }
      pos+=12+len;
   }
   return false;
}
//---------------------------------------------------------------------------
bool DictionarySegment::lookup(const string& text,unsigned& id)
   // Lookup an id for a given string
{
   // Determine the hash value
   unsigned hash=Hash::hash(text);

   // Traverse the B-Tree
   BufferReference ref(readShared(indexRoot));
   while (true) {
      const unsigned char* page=static_cast<const unsigned char*>(ref.getPage());
      // Inner node?
      if (readUint32Aligned(page+8)==0xFFFFFFFF) {
         // Perform a binary search. The test is more complex as we only have the upper bound for ranges
         unsigned left=0,right=readUint32Aligned(page+16);
         while (left!=right) {
            unsigned middle=(left+right)/2;
            unsigned hashAtMiddle=readInnerHash(page,middle);
            if (hash>hashAtMiddle) {
               left=middle+1;
            } else if ((!middle)||(hash>readInnerHash(page,middle-1))) {
               ref=readShared(readInnerPage(page,middle));
               break;
            } else {
               right=middle;
            }
         }
         // Unsuccessful search?
         if (left==right)
            return false;
      } else {
         // A leaf node. Perform a binary search on the exact value.
         unsigned left=0,right=readUint32Aligned(page+12);
         while (left!=right) {
            unsigned middle=(left+right)/2;
            unsigned hashAtMiddle=readLeafHash(page,middle);
            if (hash>hashAtMiddle) {
               left=middle+1;
            } else if (hash<hashAtMiddle) {
               right=middle;
            } else {
               // We found a match. Adjust the bounds as there can be collisions
               left=middle;
               right=readUint32Aligned(page+12);
               while (left&&(readLeafHash(page,left-1)==hash))
                  --left;
               break;
            }
         }
         // Unsuccessful search?
         if (left==right)
            return false;

         // Scan all candidates in the collision list
         for (;left<right;++left) {
            // End of the collision list?
            if (readLeafHash(page,left)!=hash)
               return false;
            // No, lookup the candidate
            if (lookupOnPage(readLeafPage(page,left),text,hash,id))
               return true;
         }
         // We reached the end of the page
         return false;
      }
   }
}
//---------------------------------------------------------------------------
bool DictionarySegment::lookupById(unsigned id,const char*& start,const char*& stop)
   // Lookup a string for a given id
{
   // Compute position in directory
   const unsigned entriesOnFirstPage = (BufferReference::pageSize-16)/8;
   const unsigned entriesPerPage = (BufferReference::pageSize-8)/8;
   unsigned dirPage,dirSlot;
   if (id<entriesOnFirstPage) {
      dirPage=mappingStart;
      dirSlot=id+1;
   } else {
      dirPage=mappingStart+1+((id-entriesOnFirstPage)/entriesPerPage);
      dirSlot=(id-entriesOnFirstPage)%entriesPerPage;
   }

   // Lookup the direct mapping entry
   BufferReference ref(readShared(dirPage));
   unsigned pageNo=readUint32(static_cast<const unsigned char*>(ref.getPage())+8+8*dirSlot);
   unsigned ofsLen=readUint32(static_cast<const unsigned char*>(ref.getPage())+8+8*dirSlot+4);
   unsigned ofs=ofsLen>>16,len=(ofsLen&0xFFFF);

   // Now search the entry on the page itself
   ref=readShared(pageNo);
   const char* page=static_cast<const char*>(ref.getPage());

   // Load the real len for long strings
   if (len==0xFFFF)
      len=readUint32(reinterpret_cast<const unsigned char*>(page+ofs-4));

   // And return the string bounds
   start=page+ofs; stop=start+len;

   return true;
}
//---------------------------------------------------------------------------
string DictionarySegment::mapId(unsigned id)
   // Lookup a string for a given id
{
   const char* start,*stop;
   if (lookupById(id,start,stop))
      return string(start,stop); else
      return string();
}
//---------------------------------------------------------------------------
void DictionarySegment::loadStrings(void* reader_)
   // Load the raw strings (must be in id order)
{
   DatabaseBuilder::StringsReader& reader=*static_cast<DatabaseBuilder::StringsReader*>(reader_);
   static const unsigned pageSize = BufferReference::pageSize;

   // Prepare the buffer
   const unsigned headerSize = 16; // LSN+next+count
   DatabaseBuilder::PageChainer chainer(8);
   unsigned char* buffer=0;
   unsigned bufferPos=headerSize,bufferCount=0;

   // Read the strings
   unsigned len; const char* data;
   unsigned id=0;
   while (reader.next(len,data)) {
      // Is the page full?
      if ((bufferPos+12+len>pageSize)&&(bufferCount)) {
         for (unsigned index=bufferPos;index<pageSize;index++)
            buffer[index]=0;
         writeUint32(buffer+12,bufferCount);
         buffer=static_cast<unsigned char*>(chainer.nextPage(this));
         bufferPos=headerSize; bufferCount=0;
      }
      // Check the len, handle an overlong string
      if (bufferPos+12+len>pageSize) {
         // Write the first page
         unsigned hash=Hash::hash(data,len);
         writeUint32(buffer+12,1);
         writeUint32(buffer+bufferPos,id);
         writeUint32(buffer+bufferPos+4,hash);
         writeUint32(buffer+bufferPos+8,len);
         memcpy(buffer+bufferPos+12,data,pageSize-(bufferPos+12));
         reader.rememberInfo(chainer.getPageNo(),(bufferPos<<16)|(0xFFFF),hash);
         buffer=static_cast<unsigned char*>(chainer.nextPage(this));
         ++id;

         // Write all intermediate pages
         const char* dataIter=data;
         unsigned iterLen=len;
         dataIter+=pageSize-(bufferPos+12);
         iterLen-=pageSize-(bufferPos+12);
         while (iterLen>(pageSize-headerSize)) {
            writeUint32(buffer+12,0);
            memcpy(buffer+headerSize,dataIter,pageSize-headerSize);
            buffer=static_cast<unsigned char*>(chainer.nextPage(this));
            dataIter+=pageSize-headerSize;
            iterLen-=pageSize-headerSize;
         }

         // Write the last page
         if (iterLen) {
            writeUint32(buffer+12,0);
            memcpy(buffer+headerSize,dataIter,iterLen);
            for (unsigned index=headerSize+iterLen;index<pageSize;index++)
               buffer[index]=0;
            buffer=static_cast<unsigned char*>(chainer.nextPage(this));
         }

         continue;
      }

      // Hash the current string...
      unsigned hash=Hash::hash(data,len);

      // ...store it...
      if (!buffer)
         buffer=static_cast<unsigned char*>(chainer.nextPage(this));
      writeUint32(buffer+bufferPos,id); bufferPos+=4;
      writeUint32(buffer+bufferPos,hash); bufferPos+=4;
      writeUint32(buffer+bufferPos,len); bufferPos+=4;
      unsigned ofs=bufferPos;
      for (unsigned index=0;index<len;index++)
         buffer[bufferPos++]=data[index];
      ++bufferCount;

      // ...and remember its position
      reader.rememberInfo(chainer.getPageNo(),(ofs<<16)|(len),hash);
      ++id;
   }
   // Flush the last page
   if (buffer) {
      for (unsigned index=bufferPos;index<pageSize;index++)
         buffer[index]=0;
      writeUint32(buffer+12,bufferCount);
   }
   chainer.finish();

   // Remember start and count
   tableStart=chainer.getFirstPageNo();
   setSegmentData(slotTableStart,tableStart);
   nextId=id;
   setSegmentData(slotNextId,nextId);
}
//---------------------------------------------------------------------------
void DictionarySegment::loadStringMappings(void* reader_)
   // Load the string mappings (must be in id order)
{
   DatabaseBuilder::StringInfoReader& reader=*static_cast<DatabaseBuilder::StringInfoReader*>(reader_);

   // Prepare the buffer
   unsigned char buffer[BufferReference::pageSize]={0};
   unsigned bufferPos=8+8;
   unsigned firstPage=0;

   // Dump the page number
   unsigned stringPage,stringOfsLen;
   while (reader.next(stringPage,stringOfsLen)) {
      // Is the page full?
      if (bufferPos==BufferReference::pageSize) {
         BufferReferenceModified currentPage;
         allocPage(currentPage);
         if (!firstPage) firstPage=currentPage.getPageNo();
         memcpy(currentPage.getPage(),buffer,BufferReference::pageSize);
         currentPage.unfixWithoutRecovery();
         bufferPos=8;
      }
      // Write the page number and ofs/len
      writeUint32(buffer+bufferPos,stringPage); bufferPos+=4;
      writeUint32(buffer+bufferPos,stringOfsLen); bufferPos+=4;
   }
   // Write the last page
   for (unsigned index=bufferPos;index<BufferReference::pageSize;index++)
      buffer[index]=0;
   BufferReferenceModified currentPage;
   allocPage(currentPage);
   if (!firstPage) firstPage=currentPage.getPageNo();
   memcpy(currentPage.getPage(),buffer,BufferReference::pageSize);
   unsigned lastPage=currentPage.getPageNo();
   currentPage.unfixWithoutRecovery();

   // Update the head
   currentPage=modifyExclusive(firstPage);
   writeUint32(static_cast<unsigned char*>(currentPage.getPage())+8+4,lastPage-firstPage+1);
   currentPage.unfixWithoutRecovery();

   // And remember the start
   mappingStart=firstPage;
   setSegmentData(slotMappingStart,mappingStart);
}
//---------------------------------------------------------------------------
void DictionarySegment::writeStringLeaves(void* reader_,void* boundaries_)
   // Write the leaf nodes of the string index
{
   DatabaseBuilder::StringInfoReader& reader=*static_cast<DatabaseBuilder::StringInfoReader*>(reader_);
   vector<pair<unsigned,unsigned> >& boundaries=*static_cast<vector<pair<unsigned,unsigned> >*>(boundaries_);

   // Prepare the buffer
   const unsigned headerSize = 16; // LSN+next+count
   DatabaseBuilder::PageChainer chainer(8);
   unsigned char buffer[BufferReference::pageSize];
   unsigned bufferPos=headerSize,bufferCount=0;

   // Scan the strings
   vector<unsigned> pages;
   unsigned stringHash=0,stringPage=0,lastStringHash=0,lastStringPage=0,previousHash=0;
   bool hasLast=false;
   while (hasLast||reader.next(stringHash,stringPage)) {
      // Collect all identical hash values
      if (hasLast) { stringHash=lastStringHash; stringPage=lastStringPage; hasLast=false; }
      pages.clear();
      pages.push_back(stringPage);
      unsigned nextStringHash,nextStringPage;
      while (reader.next(nextStringHash,nextStringPage)) {
         if (nextStringHash!=stringHash) {
            lastStringHash=nextStringHash; lastStringPage=nextStringPage;
            hasLast=true;
            break;
         } else {
            pages.push_back(nextStringPage);
         }
      }

      // Too big for the current page?
      if ((bufferPos+8*pages.size())>BufferReference::pageSize) {
         // Too big for any page?
         if ((headerSize+8*pages.size())>BufferReference::pageSize) {
            cout << "error: too many hash collisions in string table, chaining currently not implemented." << endl;
            throw;
         }
         // Write the current page
         for (unsigned index=bufferPos;index<BufferReference::pageSize;index++)
            buffer[index]=0;
         writeUint32(buffer+12,bufferCount);
         chainer.store(this,buffer);
         boundaries.push_back(pair<unsigned,unsigned>(previousHash,chainer.getPageNo()));
         bufferPos=headerSize; bufferCount=0;
      }
      // Write the chain
      for (vector<unsigned>::const_iterator iter=pages.begin(),limit=pages.end();iter!=limit;++iter) {
         writeUint32(buffer+bufferPos,stringHash); bufferPos+=4;
         writeUint32(buffer+bufferPos,*iter); bufferPos+=4;
         bufferCount++;
      }
      previousHash=stringHash;
   }

   // Flush the last page
   for (unsigned index=bufferPos;index<BufferReference::pageSize;index++)
      buffer[index]=0;
   writeUint32(buffer+12,bufferCount);
   chainer.store(this,buffer);
   boundaries.push_back(pair<unsigned,unsigned>(previousHash,chainer.getPageNo()));
   chainer.finish();
}
//---------------------------------------------------------------------------
void DictionarySegment::writeStringInner(const void* data_,void* boundaries_)
   // Write inner nodes
{
   const vector<pair<unsigned,unsigned> >& data=*static_cast<const vector<pair<unsigned,unsigned> >*>(data_);
   vector<pair<unsigned,unsigned> >& boundaries=*static_cast<vector<pair<unsigned,unsigned> >*>(boundaries_);

   const unsigned headerSize = 24; // LSN+marker+next+count+padding
   DatabaseBuilder::PageChainer chainer(12);
   unsigned char buffer[BufferReference::pageSize];
   unsigned bufferPos=headerSize,bufferCount=0;

   for (vector<pair<unsigned,unsigned> >::const_iterator iter=data.begin(),limit=data.end();iter!=limit;++iter) {
      // Do we have to start a new page?
      if ((bufferPos+8)>BufferReference::pageSize) {
         writeUint32(buffer+8,0xFFFFFFFF);
         writeUint32(buffer+12,0);
         writeUint32(buffer+16,bufferCount);
         writeUint32(buffer+20,0);
         for (unsigned index=bufferPos;index<BufferReference::pageSize;index++)
            buffer[index]=0;
         chainer.store(this,buffer);
         boundaries.push_back(pair<unsigned,unsigned>((*(iter-1)).first,chainer.getPageNo()));
         bufferPos=headerSize; bufferCount=0;
      }
      // Write the entry
      writeUint32(buffer+bufferPos,(*iter).first); bufferPos+=4;
      writeUint32(buffer+bufferPos,(*iter).second); bufferPos+=4;
      bufferCount++;
   }
   // Write the least page
   writeUint32(buffer+8,0xFFFFFFFF);
   writeUint32(buffer+12,0);
   writeUint32(buffer+16,bufferCount);
   writeUint32(buffer+20,0);
   for (unsigned index=bufferPos;index<BufferReference::pageSize;index++)
      buffer[index]=0;
   chainer.store(this,buffer);
   boundaries.push_back(pair<unsigned,unsigned>(data.back().first,chainer.getPageNo()));
   chainer.finish();
}
//---------------------------------------------------------------------------
void DictionarySegment::loadStringHashes(void* reader_)
   // Write the string index
{
   DatabaseBuilder::StringInfoReader& reader=*static_cast<DatabaseBuilder::StringInfoReader*>(reader_);

   // Write the leaf nodes
   vector<pair<unsigned,unsigned> > boundaries;
   writeStringLeaves(&reader,&boundaries);

   // Only one leaf node? Special case this
   if (boundaries.size()==1) {
      vector<pair<unsigned,unsigned> > newBoundaries;
      writeStringInner(&boundaries,&newBoundaries);
      swap(boundaries,newBoundaries);
   } else {
      // Write the inner nodes
      while (boundaries.size()>1) {
         vector<pair<unsigned,unsigned> > newBoundaries;
         writeStringInner(&boundaries,&newBoundaries);
         swap(boundaries,newBoundaries);
      }
   }

   // Remember the index root
   indexRoot=boundaries.back().second;
   setSegmentData(slotIndexRoot,indexRoot);
}
//---------------------------------------------------------------------------
