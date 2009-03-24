#include "rts/segment/DictionarySegment.hpp"
#include "rts/buffer/BufferReference.hpp"
#include "rts/segment/BTree.hpp"
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
/// Index hash-value -> string
class DictionarySegment::HashIndex : public BTree<HashIndex>
{
   public:
   /// The size of an inner key
   static const unsigned innerKeySize = 4;
   /// An inner key
   struct InnerKey {
      /// The values
      unsigned hash;

      /// Constructor
      InnerKey() : hash(0) {}
      /// Constructor
      InnerKey(unsigned hash) : hash(hash) {}

      /// Compare
      bool operator==(const InnerKey& o) const { return (hash==o.hash); }
      /// Compare
      bool operator<(const InnerKey& o) const { return (hash<o.hash); }
   };
   /// Read an inner key
   static void readInnerKey(InnerKey& key,const unsigned char* ptr) {
      key.hash=Segment::readUint32Aligned(ptr);
   }
   /// Write an inner key
   static void writeInnerKey(unsigned char* ptr,const InnerKey& key) {
      Segment::writeUint32Aligned(ptr,key.hash);
   }
   /// A leaf entry
   struct LeafEntry {
      /// The key value
      unsigned hash;
      /// The payload
      unsigned page;

      /// Compare
      bool operator==(const LeafEntry& o) const { return (hash==o.hash); }
      /// Compare
      bool operator<(const LeafEntry& o) const { return (hash<o.hash); }
      /// Compare
      bool operator<(const InnerKey& o) const { return (hash<o.hash); }
   };
   /// A leaf entry source
   class LeafEntrySource {
      private:
      /// The real source
      DictionarySegment::HashSource& source;

      public:
      /// Constructor
      LeafEntrySource(DictionarySegment::HashSource& source) : source(source) {}

      /// Read the next entry
      bool next(LeafEntry& l) { return source.next(l.hash,l.page); }
      /// Mark last entry as conflict
      void markAsConflict() { }
   };
   /// Derive an inner key
   static InnerKey deriveInnerKey(const LeafEntry& e) { return InnerKey(e.hash); }
   /// Read the first leaf entry
   static void readFirstLeafEntryKey(InnerKey& key,const unsigned char* ptr) {
      key.hash=Segment::readUint32Aligned(ptr);
   }

   private:
   /// The segment
   DictionarySegment& segment;

   public:
   /// Constructor
   explicit HashIndex(DictionarySegment& segment) : segment(segment) {}

   /// Get the segment
   Segment& getSegment() const { return segment; }
   /// Read a specific page
   BufferRequest readShared(unsigned page) const { return segment.readShared(page); }
   /// Read a specific page
   BufferRequestExclusive readExclusive(unsigned page) const { return segment.readExclusive(page); }
   /// Allocate a new page
   bool allocPage(BufferReferenceModified& page) { return segment.allocPage(page); }
   /// Get the root page
   unsigned getRootPage() const { return segment.indexRoot; }
   /// Set the root page
   void setRootPage(unsigned page);
   /// Store info about the leaf pages
   void updateLeafInfo(unsigned firstLeaf,unsigned leafCount);

   /// Check for duplicates/conflicts and "merge" if equired
   static bool mergeConflictWith(const LeafEntry& newEntry,LeafEntry& oldEntry) { return (oldEntry.hash==newEntry.hash)&&(oldEntry.page==newEntry.page); }

   /// Pack leaf entries
   static unsigned packLeafEntries(unsigned char* writer,unsigned char* limit,vector<LeafEntry>::const_iterator entriesStart,vector<LeafEntry>::const_iterator entriesLimit);
   /// Unpack leaf entries
   static void unpackLeafEntries(vector<LeafEntry>& entries,const unsigned char* reader,const unsigned char* limit);

   /// Size of the leaf header (used for scans)
   using BTree<HashIndex>::leafHeaderSize;
};
//---------------------------------------------------------------------------
void DictionarySegment::HashIndex::setRootPage(unsigned page)
   // Se the root page
{
   segment.indexRoot=page;
   segment.setSegmentData(slotIndexRoot,segment.indexRoot);
}
//---------------------------------------------------------------------------
void DictionarySegment::HashIndex::updateLeafInfo(unsigned /*firstLeaf*/,unsigned /*leafCount*/)
   // Store info about the leaf pages
{
}
//---------------------------------------------------------------------------
unsigned DictionarySegment::HashIndex::packLeafEntries(unsigned char* writer,unsigned char* writerLimit,vector<DictionarySegment::HashIndex::LeafEntry>::const_iterator entriesStart,vector<DictionarySegment::HashIndex::LeafEntry>::const_iterator entriesLimit)
   // Store the hash/page pairs
{
   // Too small?
   if ((writerLimit-writer)<4)
      return 0;

   // Compute the output len
   unsigned maxLen=((writerLimit-writer)-4)/8;
   unsigned inputLen=entriesLimit-entriesStart;
   unsigned len=min(maxLen,inputLen);

   // Write the count
   Segment::writeUint32Aligned(writer,len); writer+=4;

   // Store the entries
   for (unsigned index=0;index<len;++index,++entriesStart) {
      Segment::writeUint32Aligned(writer,(*entriesStart).hash); writer+=4;
      Segment::writeUint32Aligned(writer,(*entriesStart).page); writer+=4;
   }

   // Pad the remaining space
   memset(writer,0,writerLimit-writer);
   return len;
}
//---------------------------------------------------------------------------
void DictionarySegment::HashIndex::unpackLeafEntries(vector<DictionarySegment::HashIndex::LeafEntry>& entries,const unsigned char* reader,const unsigned char* /*limit*/)
   // Read the hash/page pairs
{
   // Read the len
   unsigned len=Segment::readUint32Aligned(reader); reader+=4;

   // Read the entries
   entries.resize(len);
   for (unsigned index=0;index<len;index++) {
      entries[index].hash=Segment::readUint32Aligned(reader); reader+=4;
      entries[index].page=Segment::readUint32Aligned(reader); reader+=4;
   }
}
//---------------------------------------------------------------------------
DictionarySegment::StringSource::~StringSource()
   // Destructor
{
}
//---------------------------------------------------------------------------
DictionarySegment::IdSource::~IdSource()
   // Destructor
{
}
//---------------------------------------------------------------------------
DictionarySegment::HashSource::~HashSource()
   // Destructor
{
}
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

   // Find the leaf page
   BufferReference ref;
   if (!HashIndex(*this).findLeaf(ref,HashIndex::InnerKey(hash)))
      return false;

   // A leaf node. Perform a binary search on the exact value.
   const unsigned char* page=static_cast<const unsigned char*>(ref.getPage());
   unsigned left=0,right=readUint32Aligned(page+HashIndex::leafHeaderSize);
   while (left!=right) {
      unsigned middle=(left+right)/2;
      unsigned hashAtMiddle=readUint32Aligned(page+HashIndex::leafHeaderSize+4+8*middle);
      if (hash>hashAtMiddle) {
         left=middle+1;
      } else if (hash<hashAtMiddle) {
         right=middle;
      } else {
         // We found a match. Adjust the bounds as there can be collisions
         left=middle;
         right=readUint32Aligned(page+HashIndex::leafHeaderSize);
         while (left&&(readUint32Aligned(page+HashIndex::leafHeaderSize+4+8*(left-1))==hash))
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
      if (readUint32Aligned(page+HashIndex::leafHeaderSize+4+8*left)!=hash)
         return false;
      // No, lookup the candidate
      if (lookupOnPage(readUint32Aligned(page+HashIndex::leafHeaderSize+4+(8*left)+4),text,hash,id))
         return true;
   }
   // We reached the end of the page
   return false;
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
void DictionarySegment::loadStrings(StringSource& reader)
   // Load the raw strings (must be in id order)
{
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
void DictionarySegment::loadStringMappings(IdSource& reader)
   // Load the string mappings (must be in id order)
{
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
void DictionarySegment::loadStringHashes(HashSource& reader)
   // Write the string index
{
   HashIndex::LeafEntrySource source(reader);
   HashIndex(*this).performBulkload(source);
}
//---------------------------------------------------------------------------
namespace {
//---------------------------------------------------------------------------
struct EntryInfo {
   /// The info
   unsigned page,ofsLen,hash;

   /// Constructor
   EntryInfo(unsigned page,unsigned ofsLen,unsigned hash) : page(page),ofsLen(ofsLen),hash(hash) {}
};
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
void DictionarySegment::appendStrings(const std::vector<std::string>& strings)
   // Load new strings into the dictionary
{
   static const unsigned pageSize = BufferReference::pageSize;

   // Prepare the buffer
   const unsigned headerSize = 16; // LSN+next+count
   DatabaseBuilder::PageChainer chainer(8);
   unsigned char* buffer=0;
   unsigned bufferPos=headerSize,bufferCount=0;

   // Read the strings
   unsigned id=nextId;
   vector<EntryInfo> info;
   info.reserve(strings.size());
   for (vector<string>::const_iterator iter=strings.begin(),limit=strings.end();iter!=limit;++iter) {
      const char* data=(*iter).c_str();
      unsigned len=(*iter).size();
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
         info.push_back(EntryInfo(chainer.getPageNo(),(bufferPos<<16)|(0xFFFF),hash));
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
      info.push_back(EntryInfo(chainer.getPageNo(),(ofs<<16)|(len),hash));
      ++id;
   }
   // Flush the last page
   if (buffer) {
      for (unsigned index=bufferPos;index<pageSize;index++)
         buffer[index]=0;
      writeUint32(buffer+12,bufferCount);
   }
   // XXX link to tableStart (or even better put everything behind the existing chain)
   chainer.finish();

   // Remember start and count
   tableStart=chainer.getFirstPageNo();
   setSegmentData(slotTableStart,tableStart);
   nextId=id;
   setSegmentData(slotNextId,nextId);

   // XXX load id->pos mapping (stored in info, sort by id)
   // XXX load hash->pos mapping (stored in info, sort by hash)
}
//---------------------------------------------------------------------------
