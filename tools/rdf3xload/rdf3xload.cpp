#include "Sorter.hpp"
#include "StringLookup.hpp"
#include "TempFile.hpp"
#include "cts/parser/TurtleParser.hpp"
#include "infra/osdep/MemoryMappedFile.hpp"
#include "rts/database/DatabaseBuilder.hpp"
#include "rts/segment/PathSelectivitySegment.hpp"
#include <iostream>
#include <cassert>
#include <cstring>
#include <fstream>
#include <sstream>
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
#define ensure(x) if (!(x)) assert(false)
//---------------------------------------------------------------------------
bool smallAddressSpace()
   // Is the address space too small?
{
   return sizeof(void*)<8;
}
//---------------------------------------------------------------------------
static const char* skipStringIdId(const char* reader)
   // Skip a materialized string/id pair
{
   return TempFile::skipId(TempFile::skipId(TempFile::skipString(reader)));
}
//---------------------------------------------------------------------------
static const char* skipStringStringString(const char* reader)
   // Skip a materialized string/id pair
{
   return TempFile::skipString(TempFile::skipString(TempFile::skipString(reader)));
}
//---------------------------------------------------------------------------
static const char* skipIdStringId(const char* reader)
   // Skip a materialized id/string/id triple
{
   return TempFile::skipId(TempFile::skipString(TempFile::skipId(reader)));
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
      if (!(rightId&1))
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
static int compareStrings(const char* left, const char* right)
   //Sort by strings
{
   // Read the string length
   uint64_t leftLen,rightLen;
   left=TempFile::readId(left,leftLen);
   right=TempFile::readId(right,rightLen);
   // Compare the subject strings
   int cmp=memcmp(left,right,min(leftLen,rightLen));
   if (cmp) return cmp;
   if (leftLen<rightLen) return -1;
   if (leftLen>rightLen) return 1;
   left+=leftLen;
   right+=rightLen;
   left=TempFile::readId(left,leftLen);
   right=TempFile::readId(right,rightLen);

   // Compare the predicate strings
   cmp=memcmp(left,right,min(leftLen,rightLen));
   if (cmp) return cmp;
   if (leftLen<rightLen) return -1;
   if (leftLen>rightLen) return 1;

   return 1;
}
//---------------------------------------------------------------------------
static int compareStringIdId(const char* left,const char* right)
   // Sort by string, type, and within same strings/types by id
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

   // Compare the types
   uint64_t leftType,rightType;
   left=TempFile::readId(left,leftType);
   right=TempFile::readId(right,rightType);
   if (leftType<rightType) return -1;
   if (leftType>rightType) return 1;

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
static int compareValue(const char* left,const char* right)
   // Sort by integer value
{
   uint64_t leftId,rightId;
   TempFile::readId(left,leftId);
   TempFile::readId(right,rightId);
   if (leftId<rightId)
      return -1;
   if (leftId>rightId)
      return 1;
   return 0;
}
//---------------------------------------------------------------------------
static int compareValueDir(const char* left,const char* right)
   // Sort by node id, then direction
{
   uint64_t leftId,rightId;

   left=TempFile::readId(left,leftId);
   right=TempFile::readId(right,rightId);

   uint64_t dir1, dir2;
   TempFile::readId(left,dir1);
   TempFile::readId(right,dir2);
   if (leftId<rightId)
      return -1;
   if (leftId>rightId)
      return 1;
   if (dir1<dir2)
	   return -1;
   if (dir1>dir2)
	   return 1;
   return 0;
}
//---------------------------------------------------------------------------
static void writeURI(ostream& out, const char* start,const char* stop)
   // Write a URI
{
   out << "<";
   for (const char* iter=start;iter<stop;++iter) {
      char c=*iter;
      switch (c) {
         case '\t': out << "\\t"; break;
         case '\n': out << "\\n"; break;
         case '\r': out << "\\r"; break;
         case '>': out << "\\>"; break;
         case '\\': out << "\\\\"; break;
         default: out << c; break;
      }
   }
   out << ">";
}
//---------------------------------------------------------------------------
static void writeLiteral(ostream& out, const char* start,const char* stop)
   // Write a literal
{
   out << "\"";
   for (const char* iter=start;iter<stop;++iter) {
      char c=*iter;
      switch (c) {
         case '\t': out << "\\t"; break;
         case '\n': out << "\\n"; break;
         case '\r': out << "\\r"; break;
         case '\"': out << "\\\""; break;
         case '\\': out << "\\\\"; break;
         default: out << c; break;
      }
   }
   out << "\"";
}
//---------------------------------------------------------------------------
static void writeObject(ostream& out, const char* start,const char* stop, Type::ID type, const char* startType, const char* stopType)
   // Write an object
{
	switch (type) {
		case Type::URI: writeURI(out,start,stop); break;
		case Type::Literal: writeLiteral(out,start,stop); break;
		case Type::CustomLanguage:{
	         writeLiteral(out,start,stop);
	         out << "@";
	         for (const char* iter=startType;iter!=stopType;++iter)
	            out << (*iter);
	         } break;
	    case Type::CustomType: {
	         writeLiteral(out,start,stop);
	         out << "^^";
	         writeURI(out,startType,stopType);
	         } break;
	    case Type::String: writeLiteral(out,start,stop); out << "^^<http://www.w3.org/2001/XMLSchema#string>"; break;
	    case Type::Integer: writeLiteral(out,start,stop); out << "^^<http://www.w3.org/2001/XMLSchema#integer>"; break;
	    case Type::Decimal: writeLiteral(out,start,stop); out << "^^<http://www.w3.org/2001/XMLSchema#decimal>"; break;
	    case Type::Double: writeLiteral(out,start,stop); out << "^^<http://www.w3.org/2001/XMLSchema#double>"; break;
	    case Type::Boolean: writeLiteral(out,start,stop); out << "^^<http://www.w3.org/2001/XMLSchema#boolean>"; break;

	}
}
//---------------------------------------------------------------------------
static bool sortInput(istream& in,ofstream& out,TempFile& facts){

   cerr<<"Sorting input triples..."<<endl;

   TurtleParser parser(in);
   TempFile rawInput(facts.getBaseFile()),sortedInput(facts.getBaseFile());
   // Read the triples
   try {
	   string subject,predicate,object,objectSubType;
	   Type::ID objectType;
	   while (true) {
	      try {
	         if (!parser.parse(subject,predicate,object,objectType,objectSubType))
		       break;
	         } catch (const TurtleParser::Exception& e) {
	            cerr << e.message << endl;
	            // recover...
	            while (in.get()!='\n') ;
	            continue;
	         }
	      // write tuples in tmp file
	      ostringstream s,p,o;
	      writeURI(s,subject.c_str(),subject.c_str()+subject.length());
	      writeURI(p,predicate.c_str(),predicate.c_str()+predicate.length());
	      writeObject(o,object.c_str(),object.c_str()+object.length(),objectType,objectSubType.c_str(),objectSubType.c_str()+objectSubType.length());
	      rawInput.writeString(s.str().length(),s.str().c_str());
	      rawInput.writeString(p.str().length(),p.str().c_str());
	      rawInput.writeString(o.str().length(),o.str().c_str());
	   }
   }
   catch (const TurtleParser::Exception&) {
	   return false;
   }

   // sort triples
   Sorter::sort(rawInput,sortedInput,skipStringStringString,compareStrings);
   rawInput.discard();

   //write sorted triples into tmp .n3 file
   MemoryMappedFile sortedTuples;
   ensure(sortedTuples.open(sortedInput.getFile().c_str()));
   for (const char* iter=sortedTuples.getBegin(),*limit=sortedTuples.getEnd();iter!=limit;) {
      unsigned sLen; const char* sStart;
	  iter=TempFile::readString(iter,sLen,sStart);
	  unsigned pLen; const char* pStart;
	  iter=TempFile::readString(iter,pLen,pStart);
	  unsigned oLen; const char* oStart;
	  iter=TempFile::readString(iter,oLen,oStart);
	  out<<string(sStart,sStart+sLen)<<" "<<string(pStart,pStart+pLen)<<" "<<string(oStart,oStart+oLen)<<". "<<endl;
   }

   out.flush();
   out.close();

   return true;
}
//---------------------------------------------------------------------------
static bool parse(istream& in,const char* name,StringLookup& lookup,TempFile& facts,TempFile& strings,map<unsigned,unsigned>& subTypes)
   // Parse the input and store it into temporary files
{
   cout << "Parsing " << name << "..." << endl;

   TurtleParser parser(in);
   map<string,unsigned> languages,types;


   try {
      string subject,predicate,object,objectSubType;
      Type::ID objectType;
      while (true) {
         try {
            if (!parser.parse(subject,predicate,object,objectType,objectSubType))
	       break;
         } catch (const TurtleParser::Exception& e) {
            cerr << e.message << endl;
            // recover...
            while (in.get()!='\n') ;
            continue;
         }
         // Construct IDs
         unsigned subjectId=lookup.lookupValue(strings,subject,Type::URI,0);
         unsigned predicateId=lookup.lookupPredicate(strings,predicate);
         unsigned subType=0;
         if (objectType==Type::CustomLanguage) {
            if (languages.count(objectSubType)) {
               subType=languages[objectSubType];
            } else {
               subType=languages[objectSubType]=lookup.lookupValue(strings,objectSubType,Type::Literal,0);
               subTypes[subType]=subType;
            }
         } else if (objectType==Type::CustomType) {
            if (types.count(objectSubType)) {
               subType=types[objectSubType];
            } else {
               subType=types[objectSubType]=lookup.lookupValue(strings,objectSubType,Type::URI,0);
               subTypes[subType]=subType;
            }
         }
         unsigned objectId=lookup.lookupValue(strings,object,objectType,subType);

         // And write the triple
         facts.writeId(subjectId);
         facts.writeId(predicateId);
         facts.writeId(objectId);
/*         cerr<<"writing sub,pred,obj: "<<subjectId<<", "<<predicateId<<", "<<objectId<<endl;*/
      }
   } catch (const TurtleParser::Exception&) {
      return false;
   }


   return true;
}
//---------------------------------------------------------------------------
static void buildDictionary(TempFile& rawStrings,TempFile& stringTable,TempFile& stringIds,map<unsigned,unsigned>& subTypes)
   // Build the dictionary
{
   cout << "Building the dictionary..." << endl;

   // Sort the strings to resolve duplicates
   TempFile sortedStrings(rawStrings.getBaseFile());
   Sorter::sort(rawStrings,sortedStrings,skipStringIdId,compareStringIdId);
   rawStrings.discard();

   // Build the id map and the string list
   TempFile rawIdMap(rawStrings.getBaseFile()),stringList(rawStrings.getBaseFile());
   {
      MemoryMappedFile strings;
      ensure(strings.open(sortedStrings.getFile().c_str()));
      uint64_t lastId=0; unsigned lastLen=0; const char* lastStr=0; uint64_t lastType=0;
      for (const char* iter=strings.getBegin(),*limit=strings.getEnd();iter!=limit;) {
         // Read the entry
         unsigned stringLen; const char* stringStart;
         iter=TempFile::readString(iter,stringLen,stringStart);
         uint64_t id,type;
         iter=TempFile::readId(iter,type);
         iter=TempFile::readId(iter,id);
/*         cerr<<"sorted string, type, id: "<<string(stringStart,stringStart+stringLen)<<" "<<type<<" "<<id<<endl;*/
         // A new one?
         if ((!lastStr)||(stringLen!=lastLen)||(memcmp(lastStr,stringStart,stringLen)!=0)||(type!=lastType)) {
            stringList.writeId(id);
            stringList.writeString(stringLen,stringStart);
            stringList.writeId(type);
            rawIdMap.writeId(id);
            rawIdMap.writeId(id);
            lastId=id; lastLen=stringLen; lastStr=stringStart; lastType=type;
/*          cerr<<"new string: wrote to rawIdMap: "<<id<<" "<<id<<endl;*/
         } else {
            rawIdMap.writeId(lastId);
            rawIdMap.writeId(id);
/*          cerr<<"already have it: "<<lastId<<" "<<id<<endl;*/
         }
      }
   }
   sortedStrings.discard();

   // Sort the string list
   Sorter::sort(stringList,stringTable,skipIdStringId,compareId);
   stringList.discard();

   // Sort the ID map
   TempFile idMap(rawStrings.getBaseFile());
   Sorter::sort(rawIdMap,idMap,skipIdId,compareId);
   rawIdMap.discard();

   // Construct new ids
   TempFile newIds(rawStrings.getBaseFile());
   {
      MemoryMappedFile in;
      ensure(in.open(idMap.getFile().c_str()));
      uint64_t lastId=0,newId=0;
      for (const char* iter=in.getBegin(),*limit=in.getEnd();iter!=limit;) {
         uint64_t firstId,currentId;
         iter=TempFile::readId(iter,firstId);
         iter=TempFile::readId(iter,currentId);
         if (firstId!=lastId) {
            ++newId;
            lastId=firstId;
         }
/*         cerr<<"current id, new id: "<<currentId<<" "<<newId<<endl;*/
         newIds.writeId(currentId);
         newIds.writeId(newId);
         if (subTypes.count(currentId))
            subTypes[currentId]=newId;
      }
   }

   // And a final sort
   Sorter::sort(newIds,stringIds,skipIdId,compareValue);
   newIds.discard();

   // Resolve the subtypes if necessary
   if (!subTypes.empty()) {
      TempFile fixedTypes(rawStrings.getBaseFile());
      MemoryMappedFile in;
      ensure(in.open(stringTable.getFile().c_str()));
      for (const char* iter=in.getBegin(),*limit=in.getEnd();iter!=limit;) {
         uint64_t id,typeInfo;
         const char* value; unsigned valueLen;
         iter=TempFile::readId(TempFile::readString(TempFile::readId(iter,id),valueLen,value),typeInfo);
         unsigned type=typeInfo&0xFF,subType=(typeInfo>>8);
         if (Type::hasSubType(static_cast<Type::ID>(type))) {
            assert(subTypes.count(subType));
            typeInfo=type|(subTypes[subType]<<8);
         } else {
            assert(subType==0);
         }
         fixedTypes.writeId(id);
         fixedTypes.writeString(valueLen,value);
         fixedTypes.writeId(typeInfo);
      }

      fixedTypes.close();
      fixedTypes.swap(stringTable);
   }
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
static int compare132(const char* left,const char* right)
   // Sort by id
{
   uint64_t l1,l2,l3,r1,r2,r3;
   loadTriple(left,l1,l2,l3);
   loadTriple(right,r1,r2,r3);

   return cmpTriples(l1,l3,l2,r1,r3,r2);
}
//---------------------------------------------------------------------------
static int compare213(const char* left,const char* right)
   // Sort by id
{
   uint64_t l1,l2,l3,r1,r2,r3;
   loadTriple(left,l1,l2,l3);
   loadTriple(right,r1,r2,r3);

   return cmpTriples(l2,l1,l3,r2,r1,r3);
}
//---------------------------------------------------------------------------
static int compare231(const char* left,const char* right)
   // Sort by id
{
   uint64_t l1,l2,l3,r1,r2,r3;
   loadTriple(left,l1,l2,l3);
   loadTriple(right,r1,r2,r3);

   return cmpTriples(l2,l3,l1,r2,r3,r1);
}
//---------------------------------------------------------------------------
static int compare312(const char* left,const char* right)
   // Sort by id
{
   uint64_t l1,l2,l3,r1,r2,r3;
   loadTriple(left,l1,l2,l3);
   loadTriple(right,r1,r2,r3);

   return cmpTriples(l3,l1,l2,r3,r1,r2);
}
//---------------------------------------------------------------------------
static int compare321(const char* left,const char* right)
   // Sort by id
{
   uint64_t l1,l2,l3,r1,r2,r3;
   loadTriple(left,l1,l2,l3);
   loadTriple(right,r1,r2,r3);

   return cmpTriples(l3,l2,l1,r3,r2,r1);
}
//---------------------------------------------------------------------------
struct IdStatistics{
	// max id in the id->string mapping
	unsigned maxId;
	// max id in the first, naive mapping from the parser
	unsigned maxOldId;
	// max id of the predicate
	unsigned maxPredicatedId;
	// number of Triples
	unsigned tripleCount;
};
//---------------------------------------------------------------------------
static void resolveIds(TempFile& rawFacts,TempFile& stringIds,TempFile& facts,IdStatistics& stat)
   // Resolve the triple ids
{
   cout << "Resolving string ids..." << endl;

   MemoryMappedFile map;
   ensure(map.open(stringIds.getFile().c_str()));

   // Sort by subject
   TempFile sortedBySubject(rawFacts.getBaseFile());
   Sorter::sort(rawFacts,sortedBySubject,skipIdIdId,compareValue);
   rawFacts.discard();

   // Resolve the subject
   TempFile subjectResolved(rawFacts.getBaseFile());
   {
      MemoryMappedFile in;
      ensure(in.open(sortedBySubject.getFile().c_str()));
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
         if (to>stat.maxId)
        	 stat.maxId=to;
         if (from>stat.maxOldId)
        	 stat.maxOldId=from;
         stat.tripleCount++;
      }
   }
   sortedBySubject.discard();
   // Sort by predicate
   TempFile sortedByPredicate(rawFacts.getBaseFile());
   Sorter::sort(subjectResolved,sortedByPredicate,skipIdIdId,compareValue);
   subjectResolved.discard();
   // Resolve the predicate
   TempFile predicateResolved(rawFacts.getBaseFile());
   {
      MemoryMappedFile in;
      ensure(in.open(sortedByPredicate.getFile().c_str()));
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
         if (to>stat.maxPredicatedId)
        	 stat.maxPredicatedId=to;
      }
   }
   sortedByPredicate.discard();
   // Sort by object
   TempFile sortedByObject(rawFacts.getBaseFile());
   Sorter::sort(predicateResolved,sortedByObject,skipIdIdId,compareValue);
   predicateResolved.discard();
   // Resolve the object
   TempFile objectResolved(rawFacts.getBaseFile());
   {
      MemoryMappedFile in;
      ensure(in.open(sortedByObject.getFile().c_str()));
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
         objectResolved.writeId(to);
         if (to>stat.maxId)
        	 stat.maxId=to;
         if (from>stat.maxOldId)
        	 stat.maxOldId=from;
      }
   }
   sortedByObject.discard();
   // Final sort by subject, predicate, object, eliminaing duplicates
   Sorter::sort(objectResolved,facts,skipIdIdId,compare123,true);

}
//---------------------------------------------------------------------------
static void renameIds(TempFile& stringIds,TempFile& facts,TempFile& newFacts,TempFile& stringTable,TempFile& newStringTable,IdStatistics& stat)
   // Rename the triple ids
{
	   {
		   MemoryMappedFile in;
		   ensure(in.open(stringTable.getFile().c_str()));
		   for (const char* iter=in.getBegin(),*limit=in.getEnd();iter!=limit;) {
			   unsigned len;
			   uint64_t id,type;
			   const char* data=0;
			   iter=TempFile::readId(iter,id);
			   iter=TempFile::readString(iter,len,data);
			   iter=TempFile::readId(iter,type);
			   cerr<<"string table before start: "<<id<<" "<<" "<<string(data,data+len)<<endl;
		   }
	   }
	   {
		   MemoryMappedFile map;
		   ensure(map.open(stringIds.getFile().c_str()));
		   {
			   for (const char* iter=map.getBegin(),*limit=map.getEnd();iter!=limit;) {
				   uint64_t from,to;
		           iter=TempFile::readId(TempFile::readId(iter,from),to);
		           cerr<<"id mapping before start: "<<from<<" "<<to<<endl;
			   }
		   }


	   }
   cout << "Renaming ids..." << endl;
   MemoryMappedFile map;
   ensure(map.open(stringIds.getFile().c_str()));
   vector<unsigned> idmapping;
   idmapping.resize(stat.maxOldId);
   {
	   for (const char* iter=map.getBegin(),*limit=map.getEnd();iter!=limit;) {
		   uint64_t from,to;
           iter=TempFile::readId(TempFile::readId(iter,from),to);
           idmapping[from]=to;
	   }
   }

   // assign new ids
   TempFile newTripleIds(stringIds.getBaseFile());
   vector<unsigned> old2new, new2old;

   {
	   MemoryMappedFile in;
	   old2new.resize(stat.maxId+1,0); new2old.resize(stat.maxId+1,0);
	   unsigned curId=stat.maxPredicatedId+1;
	   cout<<"maxpredicate, maxid "<<stat.maxPredicatedId<<", "<<stat.maxId<<endl;
	   ensure(in.open(facts.getFile().c_str()));
	   for (const char* iter=in.getBegin(),*limit=in.getEnd();iter!=limit;) {
		   uint64_t subject,predicate,object;
		   iter=TempFile::readId(iter,subject);
		   iter=TempFile::readId(iter,predicate);
       	   iter=TempFile::readId(iter,object);
       	   uint64_t newsubj,newpred,newobj;
       	   newpred=predicate;
       	   old2new[predicate]=predicate;
       	   new2old[predicate]=predicate;
       	   //start changing id's, but skip predicates
       	   if (subject>stat.maxPredicatedId){
    	   cout<<"subject, pred, obj: "<<subject<<" "<<predicate<<" "<<object<<endl;
       		   if (old2new[object]>0){
       			   newobj=old2new[object];
    		   cout<<"already seen this object: "<<newobj<<endl;
       		   }
       		   else {
    		   // skip the values that are assigned already
       			   while (new2old[curId]>0)
       				   curId++;

       			   newobj=curId;
       			   old2new[object]=newobj;
       			   new2old[newobj]=object;
    		   cout<<"new object "<<newobj<<endl;

       			   curId++;
       		   }
       		   if (old2new[subject]>0){
       			   newsubj=old2new[subject];
    		   cout<<"already seen this subject: "<<newsubj<<endl;
       		   }
       		   else if (new2old[subject]>0){
       			   newsubj=curId;
       			   curId++;
       			   old2new[subject]=newsubj;
       			   new2old[newsubj]=subject;
//    		   if (curId==1)
//    			   cout<<"old2new: "<<subject<<" "<<newsubj<<endl;
    		   cout<<"new subject: "<<newsubj<<endl;

       		   } else {
    		   cout<<"keep the subject name the same: "<<subject<<endl;
       			   newsubj=subject;
       			   old2new[subject]=subject;
       			   new2old[subject]=subject;
       		   }
       	   }
       	   else{
    	   cout<<"subject id too small, keep all the same: "<<subject<<" "<<predicate<<" "<<object<<endl;
       		   old2new[subject]=subject;
       		   old2new[object]=object;
       		   new2old[subject]=subject;
       		   new2old[object]=object;
       		   newsubj=subject;
       		   newobj=object;
       	   }
	   cout<<"new subject, pred, obj: "<<newsubj<<" "<<newpred<<" "<<newobj<<endl;
	   cout<<"mapping for comparison: "<<old2new[subject]<<" "<<old2new[predicate]<<" "<<old2new[object]<<endl;

       	   newTripleIds.writeId(newsubj);
       	   newTripleIds.writeId(newpred);
       	   newTripleIds.writeId(newobj);
	   }
   }

   for (unsigned i=0; i < old2new.size(); i++)
	   cout<<"mapping: "<<i<<" to "<<old2new[i]<<endl;

   Sorter::sort(newTripleIds,newFacts,skipIdIdId,compare123,true);
   newTripleIds.discard();

   TempFile newStringList(newFacts.getBaseFile());
   {
	   MemoryMappedFile in;
	   ensure(in.open(stringTable.getFile().c_str()));
	   for (const char* iter=in.getBegin(),*limit=in.getEnd();iter!=limit;) {
		   unsigned len;
		   uint64_t id,type;
		   const char* data=0;
		   iter=TempFile::readId(iter,id);
		   iter=TempFile::readString(iter,len,data);
		   iter=TempFile::readId(iter,type);
		   id=idmapping[id];
		   cerr<<"writing to strings: "<<id<<" "<<old2new[id]<<" "<<string(data,data+len)<<endl;
		   newStringList.writeId(old2new[id]);
		   newStringList.writeString(len,data);
		   newStringList.writeId(type);
	   }
   }

   Sorter::sort(newStringList,newStringTable,skipIdStringId,compareValue);

   ///debugging
   {
	   MemoryMappedFile in;
	   ensure(in.open(facts.getFile().c_str()));
	   for (const char* iter=in.getBegin(),*limit=in.getEnd();iter!=limit;) {
	       uint64_t subject,predicate,object;
	       iter=TempFile::readId(iter,subject);
	       iter=TempFile::readId(iter,predicate);
	       iter=TempFile::readId(iter,object);
	       cout<<subject<<" "<<predicate<<" "<<object<<endl;
	   }
   }

   {
   	   MemoryMappedFile in;
   	   ensure(in.open(newStringList.getFile().c_str()));
   	   for (const char* iter=in.getBegin(),*limit=in.getEnd();iter!=limit;) {
   		   unsigned len;
   		   uint64_t id,type;
   		   const char* data=0;
   		   iter=TempFile::readId(iter,id);
   		   iter=TempFile::readString(iter,len,data);
   		   iter=TempFile::readId(iter,type);
   		   cout<<"old list: to strings: "<<id<<" "<<string(data,data+len)<<endl;
   	   }
   }

   {
   	   MemoryMappedFile in;
   	   ensure(in.open(newStringTable.getFile().c_str()));
   	   for (const char* iter=in.getBegin(),*limit=in.getEnd();iter!=limit;) {
   		   unsigned len;
   		   uint64_t id,type;
   		   const char* data=0;
   		   iter=TempFile::readId(iter,id);
   		   iter=TempFile::readString(iter,len,data);
   		   iter=TempFile::readId(iter,type);
   		   cout<<"to strings: "<<id<<" "<<string(data,data+len)<<endl;
   	   }
   }

}
//---------------------------------------------------------------------------
static void renameIdsBySorting(TempFile& stringIds,TempFile& facts,TempFile& newFacts,TempFile& stringTable,TempFile& newStringTable,IdStatistics& stat)
   // Rename the triple ids
{
	MemoryMappedFile map;
    ensure(map.open(stringIds.getFile().c_str()));
    vector<unsigned> oldidmapping;
    oldidmapping.resize(stat.maxOldId);

    {
	    for (const char* iter=map.getBegin(),*limit=map.getEnd();iter!=limit;) {
	 	   uint64_t from,to;
           iter=TempFile::readId(TempFile::readId(iter,from),to);
           oldidmapping[from]=to;
//           cerr<<"old old id mapping: "<<from<<" "<<to<<endl;
	    }
    }


    TempFile factsFromObject(stringIds.getFile().c_str());

	{
	   MemoryMappedFile in;
	   ensure(in.open(facts.getFile().c_str()));
	   for (const char* iter=in.getBegin(),*limit=in.getEnd();iter!=limit;) {
	       uint64_t subject,predicate,object;
	       iter=TempFile::readId(iter,subject);
	       iter=TempFile::readId(iter,predicate);
	       iter=TempFile::readId(iter,object);
	       factsFromObject.writeId(object);
	       factsFromObject.writeId(predicate);
	       factsFromObject.writeId(subject);
	   }
	}

	TempFile sortedFactsFromObject(stringIds.getFile().c_str());
    Sorter::sort(factsFromObject,sortedFactsFromObject,skipIdIdId,compare123,true);
    factsFromObject.discard();

	TempFile values(stringIds.getBaseFile());
	{
	   MemoryMappedFile in;
	   vector<unsigned> entries;
	   unsigned nextPredicate=0;
	   unsigned nextNonPredicate=0;
	   entries.resize(stat.maxId+1,0);
	   cout<<"triple count: "<<stat.maxId<<endl;
	   ensure(in.open(sortedFactsFromObject.getFile().c_str()));
       bool firstPredicate=true;

	   for (const char* iter=in.getBegin(),*limit=in.getEnd();iter!=limit;) {
	       uint64_t subject,predicate,object;
	       iter=TempFile::readId(iter,object);
	       iter=TempFile::readId(iter,predicate);
	       iter=TempFile::readId(iter,subject);

	       unsigned objId;
	       if ((entries[object]>0)&&(~entries[object]))
	    	   objId=entries[object];
	       else {
	    	   objId=entries[object]=((nextNonPredicate++)<<1)|1;
	       }

	       unsigned predId;
	       if (((entries[predicate]>0)||(predicate==0))&&(!(entries[predicate]&1))){
	    	   predId=entries[predicate];
	       	   if (predicate==0&&firstPredicate){
	       		   nextPredicate++;
	       		   firstPredicate=false;
	       	   }
	       }
	       else {
	    	   predId=entries[predicate]=((nextPredicate++)<<1);
	       }

	       unsigned subjId;
	       if ((entries[subject]>0)&&(~entries[subject]))
	    	   subjId=entries[subject];
	       else {
	    	   subjId=entries[subject]=((nextNonPredicate++)<<1)|1;
	       }

	       values.writeId(static_cast<uint64_t>(object));
	       values.writeId(static_cast<uint64_t>(objId));
	       values.writeId(static_cast<uint64_t>(predicate));
	       values.writeId(static_cast<uint64_t>(predId));
	       values.writeId(static_cast<uint64_t>(subject));
	       values.writeId(static_cast<uint64_t>(subjId));
//	       cerr<<"obj pred subj: "<<object<<" "<<predicate<<" "<<subject<<endl;
//	       cerr<<" to: "<<objId<<" "<<predId<<" "<<subjId<<endl;
	   }
   }

   TempFile sortedValues(stringIds.getBaseFile());
   Sorter::sort(values,sortedValues,skipIdId,compareId);
  // values.discard();

   /// debug
//   {
//	      MemoryMappedFile valuesmap;
//	      ensure(valuesmap.open(values.getFile().c_str()));
//	      for (const char* iter=valuesmap.getBegin(),*limit=valuesmap.getEnd();iter!=limit;) {
//	          uint64_t value,id;
//	          iter=TempFile::readId(iter,value);
//	          iter=TempFile::readId(iter,id);
//	          cerr<<" debug unsorted mapping: "<<value<<" "<<id<<endl;
//	      }
//  }

//   {
//	      MemoryMappedFile values;
//	      ensure(values.open(sortedValues.getFile().c_str()));
//	      for (const char* iter=values.getBegin(),*limit=values.getEnd();iter!=limit;) {
//	          uint64_t value,id;
//	          iter=TempFile::readId(iter,value);
//	          iter=TempFile::readId(iter,id);
//	          cerr<<" debug sorted mapping: "<<value<<" "<<id<<endl;
//	      }
//   }
   // Build the id map and the string list
   TempFile rawIdMap(stringIds.getBaseFile()),stringList(stringIds.getBaseFile());
   vector<unsigned> value2Id(stat.maxId+1);
   {
      MemoryMappedFile values;
      ensure(values.open(sortedValues.getFile().c_str()));
      uint64_t lastId=0; uint64_t lastValue=0;
      for (const char* iter=values.getBegin(),*limit=values.getEnd();iter!=limit;) {
         // Read the entry
         uint64_t value,id;
         iter=TempFile::readId(iter,value);
         iter=TempFile::readId(iter,id);
//         cerr<<" mapping: val to id "<<value<<"  "<<id<<endl;
         // A new one?
         if ((!lastValue)||(lastValue!=value)) {
            rawIdMap.writeId(id);
            rawIdMap.writeId(id);
            lastId=id;
            value2Id[value]=id;
            lastValue=value;
//            cerr<<"   now it is mapping "<<value<<" to id "<<id<<endl;
         } else {
            rawIdMap.writeId(lastId);
            rawIdMap.writeId(id);
            value2Id[value]=lastId;
//            cerr<<"   now it is mapping "<<value<<" to id "<<id<<endl;
         }
      }
   }
   // Sort the ID map
   TempFile idMap(stringIds.getBaseFile());
   Sorter::sort(rawIdMap,idMap,skipIdId,compareId);
   rawIdMap.discard();

   // Construct new ids
   TempFile newIds(stringIds.getBaseFile());
   {
      MemoryMappedFile in;
      ensure(in.open(idMap.getFile().c_str()));
      uint64_t lastId=0,newId=0;
      for (const char* iter=in.getBegin(),*limit=in.getEnd();iter!=limit;) {
         uint64_t firstId,currentId;
         iter=TempFile::readId(iter,firstId);
         iter=TempFile::readId(iter,currentId);
         if (firstId!=lastId) {
            ++newId;
            lastId=firstId;
         }
  //       cerr<<"writing current id: "<<currentId<<" "<<newId<<endl;
         newIds.writeId(currentId);
         newIds.writeId(newId);
      }
   }

   // sort the ids
   TempFile newStringIds(newIds.getBaseFile());
   Sorter::sort(newIds,newStringIds,skipIdId,compareValue);
   newIds.discard();

   vector<unsigned> idmapping(stat.maxOldId+1);
   {
      MemoryMappedFile in;
      ensure(in.open(newStringIds.getFile().c_str()));
      for (const char* iter=in.getBegin(),*limit=in.getEnd();iter!=limit;) {
         uint64_t from,to;
         iter=TempFile::readId(iter,from);
         iter=TempFile::readId(iter,to);
         idmapping[from]=to;
 //        cerr<<" idmapping: "<<from<<" "<<to<<endl;
      }

   }


   for (unsigned i=0; i < value2Id.size(); i++){
//	   cerr<<"mapping: "<<i<<" "<<value2Id[i]<<" "<<idmapping[value2Id[i]]<<endl;
	   value2Id[i]=idmapping[value2Id[i]];

   }
   // assign new ids
   TempFile newTripleIds(stringIds.getBaseFile());
   {
	   MemoryMappedFile in;
	   ensure(in.open(sortedFactsFromObject.getFile().c_str()));
	   for (const char* iter=in.getBegin(),*limit=in.getEnd();iter!=limit;) {
		   uint64_t subject,predicate,object;
       	   iter=TempFile::readId(iter,object);
		   iter=TempFile::readId(iter,predicate);
		   iter=TempFile::readId(iter,subject);
       	   uint64_t newsubj,newpred,newobj;
       	   newsubj=value2Id[subject];
       	   newobj=value2Id[object];
       	   newpred=value2Id[predicate];

//       	   cerr<<" obj, pred, subj: "<<object<<" "<<predicate<<" "<<subject<<endl;
//      	   cerr<<" to: "<<newobj<<" "<<newpred<<" "<<newsubj<<endl;

       	   newTripleIds.writeId(newsubj);
       	   newTripleIds.writeId(newpred);
       	   newTripleIds.writeId(newobj);
	   }
   }
   Sorter::sort(newTripleIds,newFacts,skipIdIdId,compare123,true);
   newTripleIds.discard();

   TempFile newStringList(newFacts.getBaseFile());
   {
	   MemoryMappedFile in;
	   ensure(in.open(stringTable.getFile().c_str()));
	   for (const char* iter=in.getBegin(),*limit=in.getEnd();iter!=limit;) {
		   unsigned len;
		   uint64_t id,type;
		   const char* data=0;
		   iter=TempFile::readId(iter,id);
		   iter=TempFile::readString(iter,len,data);
		   iter=TempFile::readId(iter,type);
		   //id=idmapping[id];
//		   cerr<<"writing to strings: "<<id<<" "<<oldidmapping[id]<<" "<<value2Id[oldidmapping[id]]<<" "<<string(data,data+len)<<endl;
		   newStringList.writeId(value2Id[oldidmapping[id]]);
		   newStringList.writeString(len,data);
		   newStringList.writeId(type);
	   }
   }

   Sorter::sort(newStringList,newStringTable,skipIdStringId,compareValue);

}
//---------------------------------------------------------------------------
namespace {
class FactsLoader : public DatabaseBuilder::FactsReader {
   protected:
   /// Map to the input
   MemoryMappedFile in;
   /// Points into the data
   const char* iter,*limit;

   /// Read an id
   static const char* readId(const char* d,unsigned& v) { uint64_t x; d=TempFile::readId(d,x); v=x; return d; }

   public:
   /// Constructor
   FactsLoader(TempFile& file) { file.close(); ensure(in.open(file.getFile().c_str())); iter=in.getBegin(); limit=in.getEnd(); }

   /// Reset
   void reset() { iter=in.getBegin(); }
};
class Load123 : public FactsLoader { public: Load123(TempFile& file) : FactsLoader(file) {} bool next(unsigned& v1,unsigned& v2,unsigned& v3) { if (iter!=limit) { iter=readId(readId(readId(iter,v1),v2),v3); return true; } else return false; } };
class Load132 : public FactsLoader { public: Load132(TempFile& file) : FactsLoader(file) {} bool next(unsigned& v1,unsigned& v2,unsigned& v3) { if (iter!=limit) { iter=readId(readId(readId(iter,v1),v3),v2); return true; } else return false; } };
class Load213 : public FactsLoader { public: Load213(TempFile& file) : FactsLoader(file) {} bool next(unsigned& v1,unsigned& v2,unsigned& v3) { if (iter!=limit) { iter=readId(readId(readId(iter,v2),v1),v3); return true; } else return false; } };
class Load231 : public FactsLoader { public: Load231(TempFile& file) : FactsLoader(file) {} bool next(unsigned& v1,unsigned& v2,unsigned& v3) { if (iter!=limit) { iter=readId(readId(readId(iter,v3),v1),v2); return true; } else return false; } };
class Load312 : public FactsLoader { public: Load312(TempFile& file) : FactsLoader(file) {} bool next(unsigned& v1,unsigned& v2,unsigned& v3) { if (iter!=limit) { iter=readId(readId(readId(iter,v2),v3),v1); return true; } else return false; } };
class Load321 : public FactsLoader { public: Load321(TempFile& file) : FactsLoader(file) {} bool next(unsigned& v1,unsigned& v2,unsigned& v3) { if (iter!=limit) { iter=readId(readId(readId(iter,v3),v2),v1); return true; } else return false; } };
}
//---------------------------------------------------------------------------
static void loadFacts(DatabaseBuilder& builder,TempFile& facts)
   // Load the facts
{
   cout << "Loading triples..." << endl;
   // Order 0
   {
      Load123 loader(facts);
      builder.loadFacts(0,loader);
   }
   // Order 1
   {
      TempFile sorted(facts.getBaseFile());
      Sorter::sort(facts,sorted,skipIdIdId,compare132);
      Load132 loader(sorted);
      builder.loadFacts(1,loader);
   }
   // Order 2
   {
      TempFile sorted(facts.getBaseFile());
      Sorter::sort(facts,sorted,skipIdIdId,compare321);
      Load321 loader(sorted);
      builder.loadFacts(2,loader);
   }
   // Order 3
   {
      TempFile sorted(facts.getBaseFile());
      Sorter::sort(facts,sorted,skipIdIdId,compare312);
      Load312 loader(sorted);
      builder.loadFacts(3,loader);
   }
   // Order 4
   {
      TempFile sorted(facts.getBaseFile());
      Sorter::sort(facts,sorted,skipIdIdId,compare213);
      Load213 loader(sorted);
      builder.loadFacts(4,loader);
   }
   // Order 5
   {
      TempFile sorted(facts.getBaseFile());
      Sorter::sort(facts,sorted,skipIdIdId,compare231);
      Load231 loader(sorted);
      builder.loadFacts(5,loader);
   }
}
//---------------------------------------------------------------------------
namespace {
//---------------------------------------------------------------------------
/// Load the strings from the table
class StringReader : public DatabaseBuilder::StringsReader {
   private:
   /// The input file
   MemoryMappedFile in;
   /// The output file
   TempFile out;
   /// Pointers to the data
   const char* iter,*limit;

   public:
   /// Constructor
   StringReader(TempFile& file) : out(file.getBaseFile()) { file.close(); ensure(in.open(file.getFile().c_str())); iter=in.getBegin(); limit=in.getEnd(); }

   /// Close the input
   void closeIn() { in.close(); }
   /// Get the output
   TempFile& getOut() { out.close(); return out; }

   /// Read the next entry
   bool next(unsigned& len,const char*& data,Type::ID& type,unsigned& subType);
   /// Remember string info
   void rememberInfo(unsigned page,unsigned ofs,unsigned hash);
};
//---------------------------------------------------------------------------
bool StringReader::next(unsigned& len,const char*& data,Type::ID& type,unsigned& subType)
   // Read the next entry
{
   if (iter==limit)
      return false;
   iter=TempFile::readString(TempFile::skipId(iter),len,data);
   uint64_t typeInfo;
   iter=TempFile::readId(iter,typeInfo);
   type=static_cast<Type::ID>(typeInfo&0xFF);
   subType=static_cast<unsigned>(typeInfo>>8);
   return true;
}
//---------------------------------------------------------------------------
void StringReader::rememberInfo(unsigned page,unsigned ofs,unsigned hash)
   // Remember string info
{
   out.writeId(hash);
   out.writeId(page);
   out.writeId(ofs);
}
//---------------------------------------------------------------------------
/// Read the string mapping
class StringMappingReader : public DatabaseBuilder::StringInfoReader
{
   private:
   /// The input
   MemoryMappedFile in;
   /// Points into the data
   const char* iter,*limit;

   public:
   /// Constructor
   StringMappingReader(TempFile& file) { file.close(); ensure(in.open(file.getFile().c_str())); iter=in.getBegin(); limit=in.getEnd(); }

   /// Read the next entry
   bool next(unsigned& v1,unsigned& v2);
};
//---------------------------------------------------------------------------
bool StringMappingReader::next(unsigned& v1,unsigned& v2)
   // Read the next entry
{
   if (iter==limit)
      return false;
   uint64_t i1,i2,i3;
   iter=TempFile::readId(TempFile::readId(TempFile::readId(iter,i1),i2),i3);
   v1=i2; v2=i3;
   return true;
}
//---------------------------------------------------------------------------
/// Read the string hashes
class StringHashesReader : public DatabaseBuilder::StringInfoReader
{
   private:
   /// The input
   MemoryMappedFile in;
   /// Points into the data
   const char* iter,*limit;

   public:
   /// Constructor
   StringHashesReader(TempFile& file) { file.close(); ensure(in.open(file.getFile().c_str())); iter=in.getBegin(); limit=in.getEnd(); }

   /// Read the next entry
   bool next(unsigned& v1,unsigned& v2);
};
//---------------------------------------------------------------------------
bool StringHashesReader::next(unsigned& v1,unsigned& v2)
   // Read the next entry
{
   if (iter==limit)
      return false;
   uint64_t i1,i2,i3;
   iter=TempFile::readId(TempFile::readId(TempFile::readId(iter,i1),i2),i3);
   v1=i1; v2=i2;
   return true;
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
static void loadStrings(DatabaseBuilder& builder,TempFile& stringTable)
   // Load the strings
{
   cout << "Loading strings..." << endl;

   // Load the raw strings
   StringReader reader(stringTable);
   builder.loadStrings(reader);
   reader.closeIn();

   // Load the strings mappings
   {
      StringMappingReader infoReader(reader.getOut());
      builder.loadStringMappings(infoReader);
   }

   // Load the hash->page mappings
   {
      TempFile sortedByHash(stringTable.getBaseFile());
      Sorter::sort(reader.getOut(),sortedByHash,skipIdIdId,compareValue);
      StringHashesReader infoReader(sortedByHash);
      builder.loadStringHashes(infoReader);
   }
}
//---------------------------------------------------------------------------
static void loadStatistics(DatabaseBuilder& builder,TempFile& facts)
   // Compute the statistics
{
   cout << "Computing statistics..." << endl;

   TempFile tmp(facts.getBaseFile());
   tmp.close();
   builder.computeExactStatistics(tmp.getFile().c_str());
}
//---------------------------------------------------------------------------
namespace {
//---------------------------------------------------------------------------
class SelectivityInfo: public DatabaseBuilder::SelectivityReader{
public:
	SelectivityInfo(TempFile& file){
		    file.flush();
			file.close();
			ensure(in.open(file.getFile().c_str()));
			iter = in.getBegin();
			limit = in.getEnd();
	};
	bool next(unsigned& node, PathSelectivitySegment::Direction& dir, unsigned& selectivity);
private:
	MemoryMappedFile in;
	const char* iter;
	const char* limit;
};
//---------------------------------------------------------------------------
bool SelectivityInfo::next(unsigned& node, PathSelectivitySegment::Direction& dir, unsigned& selectivity){
	if (iter == limit)
		return false;
	uint64_t i1, i2, i3;
   	iter = TempFile::readId(iter, i1);
   	iter = TempFile::readId(iter, i2);
   	iter = TempFile::readId(iter, i3);
   	node = i1;
	dir = static_cast<PathSelectivitySegment::Direction>(i2);
   	selectivity = i3;
	return true;
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
static void loadPathSelectivity(DatabaseBuilder& builder, TempFile& facts) {
	cout << "Computing path selectivity info..." << endl;
	TempFile labels(facts.getBaseFile());

	vector<unsigned> back_selectivity, forw_selectivity;
	builder.computePathSelectivity(back_selectivity, forw_selectivity);

	// write the selectivity info and sort it
	for (unsigned i=0; i < forw_selectivity.size(); i++){
		labels.writeId(i);
		labels.writeId(PathSelectivitySegment::Forward);
		labels.writeId(forw_selectivity[i]);
	}
	for (unsigned i=0; i < back_selectivity.size(); i++){
		labels.writeId(i);
		labels.writeId(PathSelectivitySegment::Backward);
		labels.writeId(back_selectivity[i]);
	}
	TempFile sortedStat(labels.getBaseFile());
    Sorter::sort(labels,sortedStat,skipIdIdId,compareValueDir);

    SelectivityInfo sccReader(sortedStat);

    builder.loadPathSelectivity(sccReader);
}
//---------------------------------------------------------------------------
static void loadDatabase(const char* name,TempFile& facts,TempFile& stringTable)
   // Load the database
{
   cout << "Loading database into " << name << "..." << endl;
   DatabaseBuilder builder(name);

   // Load the facts
   loadFacts(builder,facts);

   // Load the strings
   loadStrings(builder,stringTable);

   // Compute the statistics
   loadStatistics(builder,facts);

   // Compute path selectivity information
   loadPathSelectivity(builder, facts);
}
//---------------------------------------------------------------------------
int main(int argc,char* argv[])
{
   // Warn first
   if (smallAddressSpace())
      cerr << "Warning: Running RDF-3X on a 32 bit system is not supported and will fail for large data sets. Please use a 64 bit system instead!" << endl;

   // Greeting
   cerr << "RDF-3X turtle importer" << endl
        << "(c) 2008 Thomas Neumann. Web site: http://www.mpi-inf.mpg.de/~neumann/rdf3x" << endl;

   // Check the arguments
   if (argc<2) {
      cerr <<  "usage: " << argv[0] << " <database> [input]" << endl
           << "without input file data is read from stdin" << endl;
      return 1;
   }

   // Parse the input
   TempFile rawFacts(argv[1]),rawStrings(argv[1]);
   map<unsigned,unsigned> subTypes;
   if (argc>=3) {
      StringLookup lookup;
      for (int index=2;index<argc;index++) {
         ifstream in(argv[index]);
         if (!in.is_open()) {
            cerr << "Unable to open " << argv[2] << endl;
            return 1;
         }
//         tmpName<<argv[index]<<"_tmp.n3";
//         ofstream out(tmpName.str().c_str());
//         if (!sortInput(in,out,rawFacts))
//        	 return 1;
//         ifstream sortedin(tmpName.str().c_str());
         if (!parse(in,argv[index],lookup,rawFacts,rawStrings,subTypes))
            return 1;
//         remove(tmpName.str().c_str());
      }
   } else {
      StringLookup lookup;
      if (!parse(cin,"stdin",lookup,rawFacts,rawStrings,subTypes))
         return 1;
   }

   // Build the string dictionary
   TempFile stringTable(argv[1]),stringIds(argv[1]);
   buildDictionary(rawStrings,stringTable,stringIds,subTypes);

   // Resolve the ids
   TempFile facts(argv[1]);
   TempFile newStringTable(argv[1]);
   TempFile newFacts(argv[1]);
   IdStatistics stat;
   resolveIds(rawFacts,stringIds,facts,stat);
  // renameIds(stringIds,facts,newFacts,stringTable,newStringTable,stat);
//   renameIdsBySorting(stringIds,facts,newFacts,stringTable,newStringTable,stat);


   // And start the load
   loadDatabase(argv[1],facts,stringTable);
   stringIds.discard();
   stringTable.discard();

   cout << "Done." << endl;
}
//---------------------------------------------------------------------------
