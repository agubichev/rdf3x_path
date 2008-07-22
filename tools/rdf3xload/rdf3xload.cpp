#include "Sorter.hpp"
#include "StringLookup.hpp"
#include "TempFile.hpp"
#include "TurtleParser.hpp"
#include "infra/osdep/MemoryMappedFile.hpp"
#include <iostream>
#include <cassert>
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
bool smallAddressSpace()
   // Is the address space too small?
{
   return sizeof(void*)<8;
}
//---------------------------------------------------------------------------
static bool parse(istream& in,TempFile& facts,TempFile& strings)
   // Parse the input and store it into temporary files
{
   TurtleParser parser(in);
   StringLookup lookup;

   // Read the triples
   try {
      string subject,predicate,object;
      while (parser.parse(subject,predicate,object)) {
         // Construct IDs
         unsigned subjectId=lookup.lookupValue(strings,subject);
         unsigned predicateId=lookup.lookupPredicate(strings,predicate);
         unsigned objectId=lookup.lookupValue(strings,object);

         // And write the triple
         facts.writeId(subjectId);
         facts.writeId(predicateId);
         facts.writeId(objectId);
      }
   } catch (const TurtleParser::Exception&) {
      return false;
   }

   return true;
}
//---------------------------------------------------------------------------
static const char* skipStringId(const char* reader)
   // Skip a materialized string/id pair
{
   return TempFile::skipId(TempFile::skipString(reader));
}
//---------------------------------------------------------------------------
static const char* skipIdString(const char* reader)
   // Skip a materialized id/string pair
{
   return TempFile::skipString(TempFile::skipId(reader));
}
//---------------------------------------------------------------------------
static const char* skipIdId(const char* reader)
   // Skip a materialized id/id
{
   return TempFile::skipId(TempFile::skipId(reader));
}
//---------------------------------------------------------------------------
static const char* skipIdIdId(const char* reader)
   // Skip a materialized id/id/id
{
   return TempFile::skipId(TempFile::skipId(TempFile::skipId(reader)));
}
//---------------------------------------------------------------------------
static int cmpIds(uint64_t leftId,uint64_t rightId)
   // Compare two ids
{
   if (leftId&1) {
      if ((!rightId&1))
         return 1;
   } else {
      if (rightId&1)
         return -1;
   }
   if (leftId<rightId) return -1;
   if (leftId>rightId) return 1;
   return 0;
}
//---------------------------------------------------------------------------
static int compareStringId(const char* left,const char* right)
   // Sort by string and within same strings by id
{
   // Read the string length
   uint64_t leftLen,rightLen;
   left=TempFile::readId(left,leftLen);
   right=TempFile::readId(right,rightLen);

   // Compare the strings
   int cmp=memcmp(left,right,min(leftLen,rightLen));
   if (cmp) return cmp;
   if (leftLen<rightLen) return -1;
   if (leftLen>rightLen) return 1;
   left+=leftLen;
   right+=rightLen;

   // Compare the ids
   uint64_t leftId,rightId;
   TempFile::readId(left,leftId);
   TempFile::readId(right,rightId);
   return cmpIds(leftId,rightId);
}
//---------------------------------------------------------------------------
static int compareId(const char* left,const char* right)
   // Sort by id
{
   uint64_t leftId,rightId;
   TempFile::readId(left,leftId);
   TempFile::readId(right,rightId);
   return cmpIds(leftId,rightId);
}
//---------------------------------------------------------------------------
static void buildDictionary(TempFile& rawStrings,TempFile& stringTable,TempFile& stringIds)
   // Build the dictionary
{
   cerr << "Building the dictionary..." << endl;

   // Sort the strings to resolve duplicates
   TempFile sortedStrings(rawStrings.getBaseFile());
   Sorter::sort(rawStrings,sortedStrings,skipStringId,compareStringId);
   rawStrings.discard();

   // Build the id map and the string list
   TempFile rawIdMap(rawStrings.getBaseFile()),stringList(rawStrings.getBaseFile());
   {
      MemoryMappedFile strings;
      assert(strings.open(sortedStrings.getFile().c_str()));
      uint64_t lastId=0; unsigned lastLen=0; const char* lastStr=0;
      for (const char* iter=strings.getBegin(),*limit=strings.getEnd();iter!=limit;) {
         // Read the entry
         unsigned stringLen; const char* stringStart;
         iter=TempFile::readString(iter,stringLen,stringStart);
         uint64_t id;
         iter=TempFile::readId(iter,id);

         // A new one?
         if ((!lastStr)||((stringLen==lastLen)&&(memcmp(lastStr,stringStart,stringLen)==0))) {
            stringList.writeId(id);
            stringList.writeString(stringLen,stringStart);
            rawIdMap.writeId(id);
            rawIdMap.writeId(id);
            lastId=id; lastLen=stringLen; lastStr=stringStart;
         } else {
            rawIdMap.writeId(lastId);
            rawIdMap.writeId(id);
         }
      }
   }
   sortedStrings.discard();

   // Sort the string list
   Sorter::sort(stringList,stringTable,skipIdString,compareId);
   stringList.discard();

   // Sort the ID map
   TempFile idMap(rawStrings.getBaseFile());
   Sorter::sort(rawIdMap,idMap,skipIdId,compareId);
   rawIdMap.discard();

   // Construct new ids
   TempFile newIds(rawStrings.getBaseFile());
   {
      MemoryMappedFile in;
      assert(in.open(idMap.getFile().c_str()));
      uint64_t lastId=0,newId=0;
      for (const char* iter=in.getBegin(),*limit=in.getEnd();iter!=limit;) {
         uint64_t firstId,currentId;
         iter=TempFile::readId(iter,firstId);
         iter=TempFile::readId(iter,currentId);
         if (firstId!=lastId) {
            ++newId;
            lastId=firstId;
         }
         newIds.writeId(currentId);
         newIds.writeId(newId);
      }
   }

   // And a final sort
   Sorter::sort(newIds,stringIds,skipIdId,compareId);
   newIds.discard();
}
//---------------------------------------------------------------------------
static inline int cmpValue(uint64_t l,uint64_t r) { return (l<r)?-1:((l>r)?1:0); }
//---------------------------------------------------------------------------
static inline int cmpTriples(uint64_t l1,uint64_t l2,uint64_t l3,uint64_t r1,uint64_t r2,uint64_t r3)
   // Compar two triples
{
   int c=cmpValue(l1,r1);
   if (c) return c;
   c=cmpValue(l2,r2);
   if (c) return c;
   return cmpValue(l3,r3);
}
//---------------------------------------------------------------------------
static inline void loadTriple(const char* data,uint64_t& v1,uint64_t& v2,uint64_t& v3)
   // Load a triple
{
   TempFile::readId(TempFile::readId(TempFile::readId(data,v1),v2),v3);
}
//---------------------------------------------------------------------------
static int compare123(const char* left,const char* right)
   // Sort by id
{
   uint64_t l1,l2,l3,r1,r2,r3;
   loadTriple(left,l1,l2,l3);
   loadTriple(right,r1,r2,r3);

   return cmpTriples(l1,l2,l3,r1,r2,r3);
}
//---------------------------------------------------------------------------
static void resolveIds(TempFile& rawFacts,TempFile& stringIds,TempFile& facts)
   // Resolve the triple ids
{
   MemoryMappedFile map;
   assert(map.open(stringIds.getFile().c_str()));

   // Sort by subject
   TempFile sortedBySubject(rawFacts.getBaseFile());
   Sorter::sort(rawFacts,sortedBySubject,skipIdIdId,compareId);
   rawFacts.discard();

   // Resolve the subject
   TempFile subjectResolved(rawFacts.getBaseFile());
   {
      MemoryMappedFile in;
      assert(in.open(sortedBySubject.getFile().c_str()));
      uint64_t from=0,to=0;
      const char* reader=map.getBegin();
      for (const char* iter=in.getBegin(),*limit=in.getEnd();iter!=limit;) {
         uint64_t subject,predicate,object;
         iter=TempFile::readId(iter,subject);
         iter=TempFile::readId(iter,predicate);
         iter=TempFile::readId(iter,object);
         while (from<subject)
            reader=TempFile::readId(TempFile::readId(reader,from),to);
         subjectResolved.writeId(predicate);
         subjectResolved.writeId(object);
         subjectResolved.writeId(to);
      }
   }

   // Sort by predicate
   TempFile sortedByPredicate(rawFacts.getBaseFile());
   Sorter::sort(subjectResolved,sortedByPredicate,skipIdIdId,compareId);
   subjectResolved.discard();

   // Resolve the predicate
   TempFile predicateResolved(rawFacts.getBaseFile());
   {
      MemoryMappedFile in;
      assert(in.open(sortedByPredicate.getFile().c_str()));
      uint64_t from=0,to=0;
      const char* reader=map.getBegin();
      for (const char* iter=in.getBegin(),*limit=in.getEnd();iter!=limit;) {
         uint64_t subject,predicate,object;
         iter=TempFile::readId(iter,predicate);
         iter=TempFile::readId(iter,object);
         iter=TempFile::readId(iter,subject);
         while (from<predicate)
            reader=TempFile::readId(TempFile::readId(reader,from),to);
         predicateResolved.writeId(object);
         predicateResolved.writeId(subject);
         predicateResolved.writeId(to);
      }
   }

   // Sort by object
   TempFile sortedByObject(rawFacts.getBaseFile());
   Sorter::sort(predicateResolved,sortedByObject,skipIdIdId,compareId);
   predicateResolved.discard();

   // Resolve the object
   TempFile objectResolved(rawFacts.getBaseFile());
   {
      MemoryMappedFile in;
      assert(in.open(sortedByObject.getFile().c_str()));
      uint64_t from=0,to=0;
      const char* reader=map.getBegin();
      for (const char* iter=in.getBegin(),*limit=in.getEnd();iter!=limit;) {
         uint64_t subject,predicate,object;
         iter=TempFile::readId(iter,object);
         iter=TempFile::readId(iter,subject);
         iter=TempFile::readId(iter,predicate);
         while (from<object)
            reader=TempFile::readId(TempFile::readId(reader,from),to);
         objectResolved.writeId(subject);
         objectResolved.writeId(predicate);
         objectResolved.writeId(object);
      }
   }

   // Final sort by subject, predicate, object, eliminaing duplicates
   Sorter::sort(objectResolved,facts,skipIdIdId,compare123,true);
}
//---------------------------------------------------------------------------
int main(int argc,char* argv[])
{
   // Warn first
   if (smallAddressSpace())
      cerr << "Warning: Running RDF-3X on a 32 bit system is not supported and will fail for large data sets. Please use a 64 bit system instead!" << endl;

   // Check the arguments
   if (argc<2) {
      cerr << "RDF-3X turtle importer" << endl
           << "usage: " << argv[0] << " <database> [input]" << endl
           << "without input file data is read from stdin" << endl;
      return 1;
   }

   // Parse the input
   TempFile rawFacts(argv[1]),rawStrings(argv[1]);
   if (argc==3) {
      ifstream in(argv[2]);
      if (!in.is_open()) {
         cerr << "Unable to open " << argv[2] << endl;
         return 1;
      }
      if (!parse(in,rawFacts,rawStrings))
         return 1;
   } else {
      if (!parse(cin,rawFacts,rawStrings))
         return 1;
   }

   // Build the string dictionary
   TempFile stringTable(argv[1]),stringIds(argv[1]);
   buildDictionary(rawStrings,stringTable,stringIds);

   // Resolve the ids
   TempFile facts(argv[1]);
   resolveIds(rawFacts,stringIds,facts);
}
//---------------------------------------------------------------------------
