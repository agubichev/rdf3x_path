#include "rts/database/DatabaseBuilder.hpp"
#include "infra/util/Hash.hpp"
#include <vector>
#include <iostream>
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
const unsigned pageSize = DatabaseBuilder::pageSize;
//---------------------------------------------------------------------------
/// A RDF triple
struct Triple {
   /// The values as IDs
   unsigned subject,predicate,object;
};
//---------------------------------------------------------------------------
/// A reader with putback capabilities
class PutbackReader {
   private:
   /// The real reader
   DatabaseBuilder::FactsReader& reader;
   /// The putback triple
   unsigned subject,predicate,object;
   /// Do we have a putback?
   bool hasPutback;

   public:
   /// Constructor
   PutbackReader(DatabaseBuilder::FactsReader& reader) : reader(reader),hasPutback(false) {}

   /// Get the next triple
   bool next(unsigned& subject,unsigned& predicate,unsigned& object);
   /// Put a triple back
   void putBack(unsigned subject,unsigned predicate,unsigned object);
};
//---------------------------------------------------------------------------
bool PutbackReader::next(unsigned& subject,unsigned& predicate,unsigned& object)
   // Get the next triple
{
   if (hasPutback) {
      subject=this->subject; predicate=this->predicate; object=this->object;
      hasPutback=false;
      return true;
   } else return reader.next(subject,predicate,object);
}
//---------------------------------------------------------------------------
void PutbackReader::putBack(unsigned subject,unsigned predicate,unsigned object)
   // Put a triple back
{
   this->subject=subject; this->predicate=predicate; this->object=object;
   hasPutback=true;
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
DatabaseBuilder::FactsReader::FactsReader()
   // Constructor
{
}
//---------------------------------------------------------------------------
DatabaseBuilder::FactsReader::~FactsReader()
   // Destructor
{
}
//---------------------------------------------------------------------------
DatabaseBuilder::StringsReader::StringsReader()
   // Constructor
{
}
//---------------------------------------------------------------------------
DatabaseBuilder::StringsReader::~StringsReader()
   // Destructor
{
}
//---------------------------------------------------------------------------
DatabaseBuilder::StringInfoReader::StringInfoReader()
   // Constructor
{
}
//---------------------------------------------------------------------------
DatabaseBuilder::StringInfoReader::~StringInfoReader()
   // Destructor
{
}
//---------------------------------------------------------------------------
DatabaseBuilder::DatabaseBuilder(const char* fileName)
   : out(fileName,ios::out|ios::trunc|ios::binary),dbFile(fileName),page(1)
   // Constructor
{
   // Check the output
   if (!out.is_open()) {
      cerr << "unable to create " << fileName << endl;
      throw;
   }

   // Clear the first page, it will be written later
   char buffer[pageSize];
   for (unsigned index=0;index<pageSize;index++)
      buffer[index]=0;
   out.write(buffer,pageSize);

   // Zero the directory
   memset(&directory,0,sizeof(directory));
}
//---------------------------------------------------------------------------
DatabaseBuilder::~DatabaseBuilder()
   // Destructor
{
}
//---------------------------------------------------------------------------
static void writePage(ofstream& out,unsigned page,const void* data)
   // Write a page to the file
{
   unsigned long long ofs=static_cast<unsigned long long>(page)*static_cast<unsigned long long>(pageSize);
   if (static_cast<unsigned long long>(out.tellp())!=ofs) {
      cout << "internal error: tried to write page " << page << " (ofs " << ofs << ") at position " << out.tellp() << endl;
      throw;
   }
   out.write(static_cast<const char*>(data),pageSize);
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
static unsigned bytes(unsigned v)
   // Compute the number of bytes required to encode a value
{
   if (v>=(1<<24))
      return 4; else
   if (v>=(1<<16))
      return 3; else
   if (v>=(1<<8)) return 2; else
      return 1;
}
//---------------------------------------------------------------------------
static unsigned writeDelta(unsigned char* buffer,unsigned ofs,unsigned value)
   // Write an integer with varying size
{
   if (value>=(1<<24)) {
      writeUint32(buffer+ofs,value);
      return ofs+4;
   } else if (value>=(1<<16)) {
      buffer[ofs]=value>>16;
      buffer[ofs+1]=(value>>8)&0xFF;
      buffer[ofs+2]=value&0xFF;
      return ofs+3;
   } else if (value>=(1<<8)) {
      buffer[ofs]=value>>8;
      buffer[ofs+1]=value&0xFF;
      return ofs+2;
   } else {
      buffer[ofs]=value;
      return ofs+1;
   }
}
//---------------------------------------------------------------------------
static unsigned bytes0(unsigned v)
   // Compute the number of bytes required to encode a value with 0 compression
{
   if (v>=(1<<24))
      return 4; else
   if (v>=(1<<16))
      return 3; else
   if (v>=(1<<8)) return 2;
   if (v>0)
      return 1; else
      return 0;
}
//---------------------------------------------------------------------------
static unsigned writeDelta0(unsigned char* buffer,unsigned ofs,unsigned value)
   // Write an integer with varying size with 0 compression
{
   if (value>=(1<<24)) {
      writeUint32(buffer+ofs,value);
      return ofs+4;
   } else if (value>=(1<<16)) {
      buffer[ofs]=value>>16;
      buffer[ofs+1]=(value>>8)&0xFF;
      buffer[ofs+2]=value&0xFF;
      return ofs+3;
   } else if (value>=(1<<8)) {
      buffer[ofs]=value>>8;
      buffer[ofs+1]=value&0xFF;
      return ofs+2;
   } else if (value>0) {
      buffer[ofs]=value;
      return ofs+1;
   } else return ofs;
}
//---------------------------------------------------------------------------
static unsigned packLeaves(ofstream& out,DatabaseBuilder::FactsReader& reader,vector<pair<Triple,unsigned> >& boundaries,unsigned page)
   // Pack the facts into leaves using prefix compression
{
   const unsigned headerSize = 4; // Next pointer
   unsigned char buffer[pageSize];
   unsigned bufferPos=headerSize;
   unsigned lastSubject=0,lastPredicate=0,lastObject=0;

   unsigned subject,predicate,object;
   while (reader.next(subject,predicate,object)) {
      // Try to pack it on the current page
      unsigned len;
      if (subject==lastSubject) {
         if (predicate==lastPredicate) {
            if (object==lastObject) {
               // Skipping a duplicate
               continue;
            } else {
               if ((object-lastObject)<128)
                  len=1; else
                  len=1+bytes(object-lastObject-128);
            }
         } else {
            len=1+bytes(predicate-lastPredicate)+bytes(object);
         }
      } else {
         len=1+bytes(subject-lastSubject)+bytes(predicate)+bytes(object);
      }

      // Tuple too big or first element on the page?
      if ((bufferPos==headerSize)||(bufferPos+len>pageSize)) {
         // Write the partial page
         if (bufferPos>headerSize) {
            writeUint32(buffer,page+1);
            for (unsigned index=bufferPos;index<pageSize;index++)
               buffer[index]=0;
            writePage(out,page,buffer);
            Triple t; t.subject=lastSubject; t.predicate=lastPredicate; t.object=lastObject;
            boundaries.push_back(pair<Triple,unsigned>(t,page));
            ++page;
         }
         // Write the first element fully
         bufferPos=headerSize;
         writeUint32(buffer+bufferPos,subject); bufferPos+=4;
         writeUint32(buffer+bufferPos,predicate); bufferPos+=4;
         writeUint32(buffer+bufferPos,object); bufferPos+=4;
      } else {
         // No, pack them
         if (subject==lastSubject) {
            if (predicate==lastPredicate) {
               if (object==lastObject) {
                  // Skipping a duplicate
                  continue;
               } else {
                  if ((object-lastObject)<128) {
                     buffer[bufferPos++]=object-lastObject;
                  } else {
                     buffer[bufferPos++]=0x80|(bytes(object-lastObject-128)-1);
                     bufferPos=writeDelta(buffer,bufferPos,object-lastObject-128);
                  }
               }
            } else {
               buffer[bufferPos++]=0x80|(bytes(predicate-lastPredicate)<<2)|(bytes(object)-1);
               bufferPos=writeDelta(buffer,bufferPos,predicate-lastPredicate);
               bufferPos=writeDelta(buffer,bufferPos,object);
            }
         } else {
            buffer[bufferPos++]=0xC0|((bytes(subject-lastSubject)-1)<<4)|((bytes(predicate)-1)<<2)|(bytes(object)-1);
            bufferPos=writeDelta(buffer,bufferPos,subject-lastSubject);
            bufferPos=writeDelta(buffer,bufferPos,predicate);
            bufferPos=writeDelta(buffer,bufferPos,object);
         }
      }

      // Update the values
      lastSubject=subject; lastPredicate=predicate; lastObject=object;
   }
   // Flush the last page
   writeUint32(buffer,0);
   for (unsigned index=bufferPos;index<pageSize;index++)
      buffer[index]=0;
   writePage(out,page,buffer);
   Triple t; t.subject=lastSubject; t.predicate=lastPredicate; t.object=lastObject;
   boundaries.push_back(pair<Triple,unsigned>(t,page));
   ++page;

   return page;
}
//---------------------------------------------------------------------------
static unsigned packInner(ofstream& out,const vector<pair<Triple,unsigned> >& data,vector<pair<Triple,unsigned> >& boundaries,unsigned page)
   // Create inner nodes
{
   const unsigned headerSize = 16; // marker+next+count+padding
   unsigned char buffer[pageSize];
   unsigned bufferPos=headerSize,bufferCount=0;

   for (vector<pair<Triple,unsigned> >::const_iterator iter=data.begin(),limit=data.end();iter!=limit;++iter) {
      // Do we have to start a new page?
      if ((bufferPos+16)>pageSize) {
         writeUint32(buffer,0xFFFFFFFF);
         writeUint32(buffer+4,page+1);
         writeUint32(buffer+8,bufferCount);
         writeUint32(buffer+12,0);
         for (unsigned index=bufferPos;index<pageSize;index++)
            buffer[index]=0;
         writePage(out,page,buffer);
         boundaries.push_back(pair<Triple,unsigned>((*(iter-1)).first,page));
         ++page;
         bufferPos=headerSize; bufferCount=0;
      }
      // Write the entry
      writeUint32(buffer+bufferPos,(*iter).first.subject); bufferPos+=4;
      writeUint32(buffer+bufferPos,(*iter).first.predicate); bufferPos+=4;
      writeUint32(buffer+bufferPos,(*iter).first.object); bufferPos+=4;
      writeUint32(buffer+bufferPos,(*iter).second); bufferPos+=4;
      bufferCount++;
   }
   // Write the least page
   writeUint32(buffer,0xFFFFFFFF);
   writeUint32(buffer+4,0);
   writeUint32(buffer+8,bufferCount);
   writeUint32(buffer+12,0);
   for (unsigned index=bufferPos;index<pageSize;index++)
      buffer[index]=0;
   writePage(out,page,buffer);
   boundaries.push_back(pair<Triple,unsigned>(data.back().first,page));
   ++page;

   return page;
}
//---------------------------------------------------------------------------
void DatabaseBuilder::loadFullFacts(unsigned ordering,FactsReader& reader)
   // Load the triples into the database
{
   // Write the leave nodes
   vector<pair<Triple,unsigned> > boundaries;
   directory.factStarts[ordering]=page;
   page=packLeaves(out,reader,boundaries,page);
   directory.factStatistics[ordering].pages=page-directory.factStarts[ordering];

   // Only one leaf node? Special case this
   if (boundaries.size()==1) {
      vector<pair<Triple,unsigned> > newBoundaries;
      page=packInner(out,boundaries,newBoundaries,page);
      directory.factIndices[ordering]=page-1;
      return;
   }

   // Write the inner nodes
   while (boundaries.size()>1) {
      vector<pair<Triple,unsigned> > newBoundaries;
      page=packInner(out,boundaries,newBoundaries,page);
      swap(boundaries,newBoundaries);
   }
   directory.factIndices[ordering]=page-1;
}
//---------------------------------------------------------------------------
static unsigned packAggregatedLeaves(ofstream& out,DatabaseBuilder::FactsReader& factsReader,vector<Triple>& boundaries,unsigned page)
   // Pack the aggregated facts into leaves using prefix compression
{
   const unsigned headerSize = 4; // Next pointer
   unsigned char buffer[pageSize];
   unsigned bufferPos=headerSize;
   unsigned lastSubject=0,lastPredicate=0;

   PutbackReader reader(factsReader);
   unsigned subject,predicate,object;
   while (reader.next(subject,predicate,object)) {
      // Count the duplicates
      unsigned nextSubject,nextPredicate,nextObject,count=1;
      while (reader.next(nextSubject,nextPredicate,nextObject)) {
         if ((nextSubject!=subject)||(nextPredicate!=predicate)) {
            reader.putBack(nextSubject,nextPredicate,nextObject);
            break;
         }
         if (nextObject==object)
            continue;
         object=nextObject;
         count++;
      }

      // Try to pack it on the current page
      unsigned len;
      if ((subject==lastSubject)&&(count<5)&&((predicate-lastPredicate)<32))
         len=1; else
         len=1+bytes0(subject-lastSubject)+bytes0(predicate)+bytes0(count-1);

      // Tuple too big or first element on the page?
      if ((bufferPos==headerSize)||(bufferPos+len>pageSize)) {
         // Write the partial page
         if (bufferPos>headerSize) {
            writeUint32(buffer,page+1);
            for (unsigned index=bufferPos;index<pageSize;index++)
               buffer[index]=0;
            writePage(out,page,buffer);
            Triple t; t.subject=lastSubject; t.predicate=lastPredicate; t.object=page;
            boundaries.push_back(t);
            ++page;
         }
         // Write the first element fully
         bufferPos=headerSize;
         writeUint32(buffer+bufferPos,subject); bufferPos+=4;
         writeUint32(buffer+bufferPos,predicate); bufferPos+=4;
         writeUint32(buffer+bufferPos,count); bufferPos+=4;
      } else {
         // No, pack them
         if ((subject==lastSubject)&&(count<5)&&((predicate-lastPredicate)<32)) {
            buffer[bufferPos++]=((count-1)<<5)|(predicate-lastPredicate);
         } else {
            buffer[bufferPos++]=0x80|((bytes0(subject-lastSubject)*25)+(bytes0(predicate)*5)+bytes0(count-1));
            bufferPos=writeDelta0(buffer,bufferPos,subject-lastSubject);
            bufferPos=writeDelta0(buffer,bufferPos,predicate);
            bufferPos=writeDelta0(buffer,bufferPos,count-1);
         }
      }

      // Update the values
      lastSubject=subject; lastPredicate=predicate;
   }
   // Flush the last page
   writeUint32(buffer,0);
   for (unsigned index=bufferPos;index<pageSize;index++)
      buffer[index]=0;
   writePage(out,page,buffer);
   Triple t; t.subject=lastSubject; t.predicate=lastPredicate; t.object=page;
   boundaries.push_back(t);
   ++page;

   return page;
}
//---------------------------------------------------------------------------
static unsigned packAggregatedInner(ofstream& out,const vector<Triple>& data,vector<Triple>& boundaries,unsigned page)
   // Create aggregated inner nodes
{
   const unsigned headerSize = 16; // marker+next+count+padding
   unsigned char buffer[pageSize];
   unsigned bufferPos=headerSize,bufferCount=0;

   for (vector<Triple>::const_iterator iter=data.begin(),limit=data.end();iter!=limit;++iter) {
      // Do we have to start a new page?
      if ((bufferPos+12)>pageSize) {
         writeUint32(buffer,0xFFFFFFFF);
         writeUint32(buffer+4,page+1);
         writeUint32(buffer+8,bufferCount);
         writeUint32(buffer+12,0);
         for (unsigned index=bufferPos;index<pageSize;index++)
            buffer[index]=0;
         writePage(out,page,buffer);
         Triple t=*(iter-1); t.object=page;
         boundaries.push_back(t);
         ++page;
         bufferPos=headerSize; bufferCount=0;
      }
      // Write the entry
      writeUint32(buffer+bufferPos,(*iter).subject); bufferPos+=4;
      writeUint32(buffer+bufferPos,(*iter).predicate); bufferPos+=4;
      writeUint32(buffer+bufferPos,(*iter).object); bufferPos+=4;
      bufferCount++;
   }
   // Write the least page
   writeUint32(buffer,0xFFFFFFFF);
   writeUint32(buffer+4,0);
   writeUint32(buffer+8,bufferCount);
   writeUint32(buffer+12,0);
   for (unsigned index=bufferPos;index<pageSize;index++)
      buffer[index]=0;
   writePage(out,page,buffer);
   Triple t=data.back(); t.object=page;
   boundaries.push_back(t);
   ++page;

   return page;
}
//---------------------------------------------------------------------------
void DatabaseBuilder::loadAggregatedFacts(unsigned ordering,FactsReader& reader)
   // Load the triples aggregated into the database
{
   // Write the leave nodes
   vector<Triple> boundaries;
   directory.aggregatedFactStarts[ordering]=page;
   page=packAggregatedLeaves(out,reader,boundaries,page);
   directory.factStatistics[ordering].aggregatedPages=page-directory.aggregatedFactStarts[ordering];

   // Only one leaf node? Special case this
   if (boundaries.size()==1) {
      vector<Triple> newBoundaries;
      page=packAggregatedInner(out,boundaries,newBoundaries,page);
      directory.aggregatedFactIndices[ordering]=page-1;
      return;
   }

   // Write the inner nodes
   while (boundaries.size()>1) {
      vector<Triple> newBoundaries;
      page=packAggregatedInner(out,boundaries,newBoundaries,page);
      swap(boundaries,newBoundaries);
   }
   directory.aggregatedFactIndices[ordering]=page-1;
}
//---------------------------------------------------------------------------
static unsigned packFullyAggregatedLeaves(ofstream& out,DatabaseBuilder::FactsReader& factsReader,vector<Triple>& boundaries,unsigned page)
   // Pack the fully aggregated facts into leaves using prefix compression
{
   const unsigned headerSize = 4; // Next pointer
   unsigned char buffer[pageSize];
   unsigned bufferPos=headerSize;
   unsigned lastSubject=0;

   PutbackReader reader(factsReader);
   unsigned subject,predicate,object;
   while (reader.next(subject,predicate,object)) {
      // Count
      unsigned nextSubject,nextPredicate,nextObject,count=1;
      while (reader.next(nextSubject,nextPredicate,nextObject)) {
         if (nextSubject!=subject) {
            reader.putBack(nextSubject,nextPredicate,nextObject);
            break;
         }
         if ((nextPredicate==predicate)&&(nextObject==object))
            continue;
         predicate=nextPredicate;
         object=nextObject;
         count++;
      }

      // Try to pack it on the current page
      unsigned len;
      if (((subject-lastSubject)<16)&&(count<=8))
         len=1; else
         len=1+bytes0(subject-lastSubject-1)+bytes0(count-1);

      // Tuple too big or first element on the page?
      if ((bufferPos==headerSize)||(bufferPos+len>pageSize)) {
         // Write the partial page
         if (bufferPos>headerSize) {
            writeUint32(buffer,page+1);
            for (unsigned index=bufferPos;index<pageSize;index++)
               buffer[index]=0;
            writePage(out,page,buffer);
            Triple t; t.subject=lastSubject; t.predicate=0; t.object=page;
            boundaries.push_back(t);
            ++page;
         }
         // Write the first element fully
         bufferPos=headerSize;
         writeUint32(buffer+bufferPos,subject); bufferPos+=4;
         writeUint32(buffer+bufferPos,count); bufferPos+=4;
      } else {
         // No, pack them
         if (((subject-lastSubject)<16)&&(count<=8)) {
            buffer[bufferPos++]=((count-1)<<4)|(subject-lastSubject);
         } else {
            buffer[bufferPos++]=0x80|((bytes0(subject-lastSubject-1)*5)+bytes0(count-1));
            bufferPos=writeDelta0(buffer,bufferPos,subject-lastSubject-1);
            bufferPos=writeDelta0(buffer,bufferPos,count-1);
         }
      }

      // Update the values
      lastSubject=subject;
   }
   // Flush the last page
   writeUint32(buffer,0);
   for (unsigned index=bufferPos;index<pageSize;index++)
      buffer[index]=0;
   writePage(out,page,buffer);
   Triple t; t.subject=lastSubject; t.predicate=0; t.object=page;
   boundaries.push_back(t);
   ++page;

   return page;
}
//---------------------------------------------------------------------------
static unsigned packFullyAggregatedInner(ofstream& out,const vector<Triple>& data,vector<Triple>& boundaries,unsigned page)
   // Create fully aggregated inner nodes
{
   const unsigned headerSize = 16; // marker+next+count+padding
   unsigned char buffer[pageSize];
   unsigned bufferPos=headerSize,bufferCount=0;

   for (vector<Triple>::const_iterator iter=data.begin(),limit=data.end();iter!=limit;++iter) {
      // Do we have to start a new page?
      if ((bufferPos+8)>pageSize) {
         writeUint32(buffer,0xFFFFFFFF);
         writeUint32(buffer+4,page+1);
         writeUint32(buffer+8,bufferCount);
         writeUint32(buffer+12,0);
         for (unsigned index=bufferPos;index<pageSize;index++)
            buffer[index]=0;
         writePage(out,page,buffer);
         Triple t=*(iter-1); t.object=page;
         boundaries.push_back(t);
         ++page;
         bufferPos=headerSize; bufferCount=0;
      }
      // Write the entry
      writeUint32(buffer+bufferPos,(*iter).subject); bufferPos+=4;
      writeUint32(buffer+bufferPos,(*iter).object); bufferPos+=4;
      bufferCount++;
   }
   // Write the least page
   writeUint32(buffer,0xFFFFFFFF);
   writeUint32(buffer+4,0);
   writeUint32(buffer+8,bufferCount);
   writeUint32(buffer+12,0);
   for (unsigned index=bufferPos;index<pageSize;index++)
      buffer[index]=0;
   writePage(out,page,buffer);
   Triple t=data.back(); t.object=page;
   boundaries.push_back(t);
   ++page;

   return page;
}
//---------------------------------------------------------------------------
void DatabaseBuilder::loadFullyAggregatedFacts(unsigned ordering,FactsReader& reader)
   // Load the triples fully aggregated into the database
{
   // Write the leave nodes
   vector<Triple> boundaries;
   directory.fullyAggregatedFactStarts[ordering]=page;
   page=packFullyAggregatedLeaves(out,reader,boundaries,page);

   // Only one leaf node? Special case this
   if (boundaries.size()==1) {
      vector<Triple> newBoundaries;
      page=packFullyAggregatedInner(out,boundaries,newBoundaries,page);
      directory.fullyAggregatedFactIndices[ordering]=page-1;
      return;
   }

   // Write the inner nodes
   while (boundaries.size()>1) {
      vector<Triple> newBoundaries;
      page=packFullyAggregatedInner(out,boundaries,newBoundaries,page);
      swap(boundaries,newBoundaries);
   }
   directory.fullyAggregatedFactIndices[ordering]=page-1;
}
//---------------------------------------------------------------------------
void DatabaseBuilder::loadFacts(unsigned order,FactsReader& reader)
   // Loads the facts in a given order
{
   // Load the full facts first
   loadFullFacts(order,reader);

   // Load the aggregated facts
   reader.reset();
   loadAggregatedFacts(order,reader);

   // Load the fully aggregated facts
   if ((order&1)==0) {
      reader.reset();
      loadFullyAggregatedFacts(order/2,reader);
   }

   // Compute the tuple statistics
   reader.reset();
   unsigned subject,predicate,object;
   if (!reader.next(subject,predicate,object)) {
      directory.factStatistics[order].groups1=0;
      directory.factStatistics[order].groups2=0;
      directory.factStatistics[order].cardinality=0;
   } else {
      directory.factStatistics[order].groups1=1;
      directory.factStatistics[order].groups2=1;
      directory.factStatistics[order].cardinality=1;
      unsigned nextSubject,nextPredicate,nextObject;
      while (reader.next(nextSubject,nextPredicate,nextObject)) {
         if (nextSubject!=subject) {
            directory.factStatistics[order].groups1++;
            directory.factStatistics[order].groups2++;
            directory.factStatistics[order].cardinality++;
            subject=nextSubject; predicate=nextPredicate; object=nextObject;
         } else if (nextPredicate!=predicate) {
            directory.factStatistics[order].groups2++;
            directory.factStatistics[order].cardinality++;
            predicate=nextPredicate; object=nextObject;
         } else if (nextObject!=object) {
            directory.factStatistics[order].cardinality++;
            object=nextObject;
         }
      }
   }
}
//---------------------------------------------------------------------------
void DatabaseBuilder::loadStrings(StringsReader& reader)
   // Load the raw strings (must be in id order)
{
   // Prepare the buffer
   const unsigned headerSize = 8; // next+count
   unsigned char buffer[pageSize];
   unsigned bufferPos=headerSize,bufferCount=0;
   directory.stringStart=page;

   // Read the strings
   unsigned len; const char* data;
   unsigned id=0;
   while (reader.next(len,data)) {
      // Is the page full?
      if (bufferPos+12+len>pageSize) {
         for (unsigned index=bufferPos;index<pageSize;index++)
            buffer[index]=0;
         writeUint32(buffer,page+1);
         writeUint32(buffer+4,bufferCount);
         writePage(out,page,buffer);
         ++page;
         bufferPos=headerSize; bufferCount=0;
      }
      // Check the len, handle an overlong string
      if (bufferPos+12+len>pageSize) {
         // Compute the required number of pages
         unsigned lenInPages=(len+bufferPos+12+pageSize-1)/pageSize;

         // Write the first page
         unsigned hash=Hash::hash(data,len);
         writeUint32(buffer,page+lenInPages);
         writeUint32(buffer+4,1);
         writeUint32(buffer+bufferPos,id);
         writeUint32(buffer+bufferPos+4,hash);
         writeUint32(buffer+bufferPos+8,len);
         memcpy(buffer+bufferPos+12,data,pageSize-(bufferPos+12));
         writePage(out,page,buffer);
         reader.rememberInfo(page,(bufferPos<<16)|(0xFFFF),hash);
         ++id;
         ++page;

         // Write all intermediate pages
         const char* dataIter=data;
         unsigned iterLen=len;
         dataIter+=pageSize-(bufferPos+12);
         iterLen-=pageSize-(bufferPos+12);
         while (iterLen>pageSize) {
            writePage(out,page,dataIter);
            ++page;
            dataIter+=pageSize;
            iterLen-=pageSize;
         }

         // Write the last page
         if (iterLen) {
            memcpy(buffer,dataIter,iterLen);
            for (unsigned index=iterLen;index<pageSize;index++)
               buffer[index]=0;
            writePage(out,page,buffer);
            ++page;
         }

         continue;
      }

      // Hash the current string...
      unsigned hash=Hash::hash(data,len);

      // ...store it...
      writeUint32(buffer+bufferPos,id); bufferPos+=4;
      writeUint32(buffer+bufferPos,hash); bufferPos+=4;
      writeUint32(buffer+bufferPos,len); bufferPos+=4;
      unsigned ofs=bufferPos;
      for (unsigned index=0;index<len;index++)
         buffer[bufferPos++]=data[index];
      ++bufferCount;

      // ...and remember its position
      reader.rememberInfo(page,(ofs<<16)|(len),hash);
      ++id;
   }
   // Flush the last page
   for (unsigned index=bufferPos;index<pageSize;index++)
      buffer[index]=0;
   writeUint32(buffer,0);
   writeUint32(buffer+4,bufferCount);
   writePage(out,page,buffer);
   ++page;
}
//---------------------------------------------------------------------------
void DatabaseBuilder::loadStringMappings(DatabaseBuilder::StringInfoReader& reader)
   // Load the string mappings (must be in id order)
{
   // Prepare the buffer
   unsigned char buffer[pageSize];
   unsigned bufferPos=0;
   directory.stringMapping=page;

   // Dump the page number
   unsigned stringPage,stringOfsLen;
   while (reader.next(stringPage,stringOfsLen)) {
      // Is the page full?
      if (bufferPos==pageSize) {
         writePage(out,page,buffer);
         ++page;
         bufferPos=0;
      }
      // Write the page number and ofs/len
      writeUint32(buffer+bufferPos,stringPage); bufferPos+=4;
      writeUint32(buffer+bufferPos,stringOfsLen); bufferPos+=4;
   }
   // Write the last page
   for (unsigned index=bufferPos;index<pageSize;index++)
      buffer[index]=0;
   writePage(out,page,buffer);
   ++page;
}
//---------------------------------------------------------------------------
static unsigned writeStringLeaves(ofstream& out,DatabaseBuilder::StringInfoReader& reader,vector<pair<unsigned,unsigned> >& boundaries,unsigned page)
   // Write the leaf nodes of the string index
{
   // Prepare the buffer
   const unsigned headerSize = 8; // next+count
   unsigned char buffer[pageSize];
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
      if ((bufferPos+8*pages.size())>pageSize) {
         // Too big for any page?
         if ((headerSize+8*pages.size())>pageSize) {
            cout << "error: too many hash collisions in string table, chaining currently not implemented." << endl;
            throw;
         }
         // Write the current page
         for (unsigned index=bufferPos;index<pageSize;index++)
            buffer[index]=0;
         writeUint32(buffer,page+1);
         writeUint32(buffer+4,bufferCount);
         writePage(out,page,buffer);
         boundaries.push_back(pair<unsigned,unsigned>(previousHash,page));
         ++page;
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
   for (unsigned index=bufferPos;index<pageSize;index++)
      buffer[index]=0;
   writeUint32(buffer,0);
   writeUint32(buffer+4,bufferCount);
   writePage(out,page,buffer);
   boundaries.push_back(pair<unsigned,unsigned>(previousHash,page));
   ++page;

   return page;
}
//---------------------------------------------------------------------------
static unsigned writeStringInner(ofstream& out,const vector<pair<unsigned,unsigned> >& data,vector<pair<unsigned,unsigned> >& boundaries,unsigned page)
   // Write inner nodes
{
   const unsigned headerSize = 16; // marker+next+count+padding
   unsigned char buffer[pageSize];
   unsigned bufferPos=headerSize,bufferCount=0;

   for (vector<pair<unsigned,unsigned> >::const_iterator iter=data.begin(),limit=data.end();iter!=limit;++iter) {
      // Do we have to start a new page?
      if ((bufferPos+8)>pageSize) {
         writeUint32(buffer,0xFFFFFFFF);
         writeUint32(buffer+4,page+1);
         writeUint32(buffer+8,bufferCount);
         writeUint32(buffer+12,0);
         for (unsigned index=bufferPos;index<pageSize;index++)
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
   for (unsigned index=bufferPos;index<pageSize;index++)
      buffer[index]=0;
   writePage(out,page,buffer);
   boundaries.push_back(pair<unsigned,unsigned>(data.back().first,page));
   ++page;

   return page;
}
//---------------------------------------------------------------------------
void DatabaseBuilder::loadStringHashes(StringInfoReader& reader)
   // Write the string index
{
   // Write the leaf nodes
   vector<pair<unsigned,unsigned> > boundaries;
   page=writeStringLeaves(out,reader,boundaries,page);

   // Only one leaf node? Special case this
   if (boundaries.size()==1) {
      vector<pair<unsigned,unsigned> > newBoundaries;
      page=writeStringInner(out,boundaries,newBoundaries,page);
      directory.stringIndex=page-1;
      return;
   }

   // Write the inner nodes
   while (boundaries.size()>1) {
      vector<pair<unsigned,unsigned> > newBoundaries;
      page=writeStringInner(out,boundaries,newBoundaries,page);
      swap(boundaries,newBoundaries);
   }
   directory.stringIndex=page-1;
}
//---------------------------------------------------------------------------
void DatabaseBuilder::finishLoading()
   // Finish the load phase, write the directory
{
   // Prepare empty pages for statistics
   unsigned char buffer[pageSize];
   for (unsigned index=0;index<pageSize;index++)
      buffer[index]=0;
   for (unsigned index=0;index<6;index++) {
      writePage(out,page,buffer);
      directory.statistics[index]=page;
      ++page;
   }
   for (unsigned index=0;index<2;index++) {
      writePage(out,page,buffer);
      directory.pathStatistics[index]=page;
      ++page;
   }

   // Write the directory
   unsigned bufferPos = 0;

   // Magic
   writeUint32(buffer+bufferPos,('R'<<24)|('D'<<16)|('F'<<8)); bufferPos+=4;
   // Format version
   writeUint32(buffer+bufferPos,1); bufferPos+=4;

   // Write the facts entries
   for (unsigned index=0;index<6;index++) {
      writeUint32(buffer+bufferPos,directory.factStarts[index]); bufferPos+=4;
      writeUint32(buffer+bufferPos,directory.factIndices[index]); bufferPos+=4;
      writeUint32(buffer+bufferPos,directory.aggregatedFactStarts[index]); bufferPos+=4;
      writeUint32(buffer+bufferPos,directory.aggregatedFactIndices[index]); bufferPos+=4;
      writeUint32(buffer+bufferPos,directory.factStatistics[index].pages); bufferPos+=4;
      writeUint32(buffer+bufferPos,directory.factStatistics[index].aggregatedPages); bufferPos+=4;
      writeUint32(buffer+bufferPos,directory.factStatistics[index].groups1); bufferPos+=4;
      writeUint32(buffer+bufferPos,directory.factStatistics[index].groups2); bufferPos+=4;
      writeUint32(buffer+bufferPos,directory.factStatistics[index].cardinality); bufferPos+=4;
   }
   for (unsigned index=0;index<3;index++) {
      writeUint32(buffer+bufferPos,directory.fullyAggregatedFactStarts[index]); bufferPos+=4;
      writeUint32(buffer+bufferPos,directory.fullyAggregatedFactIndices[index]); bufferPos+=4;
   }

   // Write the string entries
   writeUint32(buffer+bufferPos,directory.stringStart); bufferPos+=4;
   writeUint32(buffer+bufferPos,directory.stringMapping); bufferPos+=4;
   writeUint32(buffer+bufferPos,directory.stringIndex); bufferPos+=4;

   // Write the statistics
   for (unsigned index=0;index<6;index++) {
      writeUint32(buffer+bufferPos,directory.statistics[index]); bufferPos+=4;
   }
   for (unsigned index=0;index<2;index++) {
      writeUint32(buffer+bufferPos,directory.pathStatistics[index]); bufferPos+=4;
   }

   // Pad the page and write it to the beginning of the file
   for (unsigned index=bufferPos;index<pageSize;index++)
      buffer[index]=0;
   out.seekp(0,ios_base::beg);
   writePage(out,0,buffer);

   out.flush();
   out.close();
}
//---------------------------------------------------------------------------
