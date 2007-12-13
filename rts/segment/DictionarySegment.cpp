#include "rts/segment/DictionarySegment.hpp"
#include "rts/buffer/BufferManager.hpp"
#include "infra/util/Hash.hpp"
//---------------------------------------------------------------------------
/// Helper functions
static inline unsigned readInnerHash(const unsigned char* page,unsigned slot) { return Segment::readUint32Aligned(page+16+8*slot); }
static inline unsigned readInnerPage(const unsigned char* page,unsigned slot) { return Segment::readUint32Aligned(page+16+8*slot+4); }
static inline unsigned readLeafHash(const unsigned char* page,unsigned slot) { return Segment::readUint32Aligned(page+8+8*slot); }
static inline unsigned readLeafPage(const unsigned char* page,unsigned slot) { return Segment::readUint32Aligned(page+8+8*slot+4); }
//---------------------------------------------------------------------------
DictionarySegment::DictionarySegment(BufferManager& bufferManager,unsigned tableStart,unsigned mappingStart,unsigned indexRoot)
   : Segment(bufferManager),tableStart(tableStart),mappingStart(mappingStart),indexRoot(indexRoot)
   // Constructor
{
}
//---------------------------------------------------------------------------
bool DictionarySegment::lookupOnPage(unsigned pageNo,const std::string& text,unsigned hash,unsigned& id)
   // Lookup an id for a given string on a certain page in the raw string table
{
   BufferReference ref(readShared(pageNo));
   const unsigned char* page=static_cast<const unsigned char*>(ref.getPage());
   unsigned count=1000+readUint32Aligned(page+4),pos=8;

   for (unsigned index=0;index<count;index++) {
      if (pos+12>BufferManager::pageSize)
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
bool DictionarySegment::lookup(const std::string& text,unsigned& id)
   // Lookup an id for a given string
{
   // Determine the hash value
   unsigned hash=Hash::hash(text);

   // Traverse the B-Tree
   BufferReference ref(readShared(indexRoot));
   while (true) {
      const unsigned char* page=static_cast<const unsigned char*>(ref.getPage());
      // Inner node?
      if (readUint32Aligned(page)==0xFFFFFFFF) {
         // Perform a binary search. The test is more complex as we only have the upper bound for ranges
         unsigned left=0,right=readUint32Aligned(page+8);
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
         unsigned left=0,right=readUint32Aligned(page+4);
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
               right=readUint32Aligned(page+8);
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
bool DictionarySegment::lookupById(unsigned id,std::string& text)
   // Lookup a string for a given id
{
   // Lookup the direct mapping entry
   const unsigned entriesPerPage = BufferManager::pageSize/4;
   BufferReference ref(readShared(mappingStart+(id/entriesPerPage)));
   unsigned pageNo=readUint32(static_cast<const unsigned char*>(ref.getPage())+(4*(id%entriesPerPage)));

   // Now search the entry on the page itself
   ref=readShared(pageNo);
   const unsigned char* page=static_cast<const unsigned char*>(ref.getPage());
   unsigned count=readUint32Aligned(page+4),pos=8;

   for (unsigned index=0;index<count;index++) {
      unsigned len=readUint32(page+pos+8);
      if (readUint32(page+pos)==id) {
         text.assign(reinterpret_cast<const char*>(page+pos+12),len);
         return true;
      }
      pos+=12+len;
   }

   return false;
}
//---------------------------------------------------------------------------