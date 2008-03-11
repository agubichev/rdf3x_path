#include "infra/util/Hash.hpp"
#include "rts/database/Database.hpp"
#include <fstream>
#include <iostream>
#include <vector>
#include <algorithm>
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
/// The desired page size
const unsigned pageSize = 16384;
//---------------------------------------------------------------------------
void buildStatisticsPage(Database& db,Database::DataOrder order,unsigned char* page);
//---------------------------------------------------------------------------
namespace {
//---------------------------------------------------------------------------
/// The database directory
struct Directory
{
   /// Statistics for a facts table
   struct FactsStatistics {
      /// The number of pages
      unsigned pages;
      /// The number of aggregated pages
      unsigned aggregatedPages;
      /// The number of level 1 groups
      unsigned groups1;
      /// The number of level 2 groups
      unsigned groups2;
      /// The number of tuples
      unsigned cardinality;
   };
   /// Begin of the facts tables in all orderings
   unsigned factStarts[6];
   /// Root of the fact indices in all orderings
   unsigned factIndices[6];
   /// Begin of the aggregated facts tables in all orderings
   unsigned aggregatedFactStarts[6];
   /// Root of the aggregatedfact indices in all orderings
   unsigned aggregatedFactIndices[6];
   /// Begin of the fully aggregated facts tables in all orderings
   unsigned fullyAggregatedFactStarts[3];
   /// Root of the fully aggregated facts tables in all orderings
   unsigned fullyAggregatedFactIndices[3];
   /// The statistics
   FactsStatistics factStatistics[6];
   /// Begin of the string table
   unsigned stringStart;
   /// Begin of the string mapping
   unsigned stringMapping;
   /// Root of the string index
   unsigned stringIndex;
   /// Pages with statistics
   unsigned statistics[6];
};
//---------------------------------------------------------------------------
/// A RDF tripple
struct Triple {
   /// The values as IDs
   unsigned subject,predicate,object;
};
//---------------------------------------------------------------------------
/// Order a RDF tripple lexicographically
struct OrderTriple {
   bool operator()(const Triple& a,const Triple& b) const {
      return (a.subject<b.subject)||
             ((a.subject==b.subject)&&((a.predicate<b.predicate)||
             ((a.predicate==b.predicate)&&(a.object<b.object))));
   }
};
//---------------------------------------------------------------------------
void writePage(ofstream& out,unsigned page,const void* data)
   // Write a page to the file
{
   if (static_cast<unsigned>(out.tellp())!=(page*pageSize)) {
      std::cout << "internal error: tried to write page " << page << " (ofs " << (page*pageSize) << ") at position " << out.tellp() << std::endl;
      throw;
   }
   out.write(static_cast<const char*>(data),pageSize);
}
//---------------------------------------------------------------------------
bool readFacts(vector<Triple>& facts,const char* fileName)
   // Read the facts table
{
   ifstream in(fileName);
   if (!in.is_open()) {
      cout << "unable to open " << fileName << endl;
      return false;
   }

   facts.clear();
   while (true) {
      Triple t;
      in >> t.subject >> t.predicate >> t.object;
      if (!in.good()) break;
      facts.push_back(t);
   }

   return true;
}
//---------------------------------------------------------------------------
void writeUint32(unsigned char* target,unsigned value)
   // Write a 32bit value
{
   target[0]=value>>24;
   target[1]=(value>>16)&0xFF;
   target[2]=(value>>8)&0xFF;
   target[3]=value&0xFF;
}
//---------------------------------------------------------------------------
unsigned bytes(unsigned v)
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
unsigned writeDelta(unsigned char* buffer,unsigned ofs,unsigned value)
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
unsigned bytes0(unsigned v)
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
unsigned writeDelta0(unsigned char* buffer,unsigned ofs,unsigned value)
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
unsigned packLeaves(ofstream& out,const vector<Triple>& facts,vector<pair<Triple,unsigned> >& boundaries,unsigned page)
   // Pack the facts into leaves using prefix compression
{
   const unsigned headerSize = 4; // Next pointer
   unsigned char buffer[pageSize];
   unsigned bufferPos=headerSize;
   unsigned lastSubject=0,lastPredicate=0,lastObject=0;

   for (vector<Triple>::const_iterator iter=facts.begin(),limit=facts.end();iter!=limit;++iter) {
      // Access the current tripple
      unsigned subject=(*iter).subject,predicate=(*iter).predicate,object=(*iter).object;

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
unsigned packInner(ofstream& out,const vector<pair<Triple,unsigned> >& data,vector<pair<Triple,unsigned> >& boundaries,unsigned page)
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
unsigned packFacts(ofstream& out,Directory& directory,unsigned ordering,const vector<Triple>& facts,unsigned page)
   // Pack the facts using prefix compression
{
   // Write the leave nodes
   vector<pair<Triple,unsigned> > boundaries;
   directory.factStarts[ordering]=page;
   page=packLeaves(out,facts,boundaries,page);
   directory.factStatistics[ordering].pages=page-directory.factStarts[ordering];

   // Only one leaf node? Special case this
   if (boundaries.size()==1) {
      vector<pair<Triple,unsigned> > newBoundaries;
      page=packInner(out,boundaries,newBoundaries,page);
      directory.factIndices[ordering]=page-1;
      return page;
   }

   // Write the inner nodes
   while (boundaries.size()>1) {
      vector<pair<Triple,unsigned> > newBoundaries;
      page=packInner(out,boundaries,newBoundaries,page);
      swap(boundaries,newBoundaries);
   }
   directory.factIndices[ordering]=page-1;
   return page;
}
//---------------------------------------------------------------------------
unsigned packAggregatedLeaves(ofstream& out,const vector<Triple>& facts,vector<Triple>& boundaries,unsigned page)
   // Pack the aggregated facts into leaves using prefix compression
{
   const unsigned headerSize = 4; // Next pointer
   unsigned char buffer[pageSize];
   unsigned bufferPos=headerSize;
   unsigned lastSubject=0,lastPredicate=0;

   for (vector<Triple>::const_iterator iter=facts.begin(),limit=facts.end();iter!=limit;) {
      // Access the current tripple
      unsigned subject=(*iter).subject,predicate=(*iter).predicate;
      unsigned object=(*iter).object,count=1;
      for (++iter;iter!=limit;++iter) {
         if (((*iter).subject!=subject)||((*iter).predicate!=predicate))
            break;
         if ((*iter).object==object)
            continue;
         object=(*iter).object;
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
unsigned packAggregatedInner(ofstream& out,const vector<Triple>& data,vector<Triple>& boundaries,unsigned page)
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
unsigned packAggregatedFacts(ofstream& out,Directory& directory,unsigned ordering,const vector<Triple>& facts,unsigned page)
   // Pack the aggregated facts using prefix compression
{
   // Write the leave nodes
   vector<Triple> boundaries;
   directory.aggregatedFactStarts[ordering]=page;
   page=packAggregatedLeaves(out,facts,boundaries,page);
   directory.factStatistics[ordering].aggregatedPages=page-directory.aggregatedFactStarts[ordering];

   // Only one leaf node? Special case this
   if (boundaries.size()==1) {
      vector<Triple> newBoundaries;
      page=packAggregatedInner(out,boundaries,newBoundaries,page);
      directory.aggregatedFactIndices[ordering]=page-1;
      return page;
   }

   // Write the inner nodes
   while (boundaries.size()>1) {
      vector<Triple> newBoundaries;
      page=packAggregatedInner(out,boundaries,newBoundaries,page);
      swap(boundaries,newBoundaries);
   }
   directory.aggregatedFactIndices[ordering]=page-1;
   return page;
}
//---------------------------------------------------------------------------
unsigned packFullyAggregatedLeaves(ofstream& out,const vector<Triple>& facts,vector<Triple>& boundaries,unsigned page)
   // Pack the fully aggregated facts into leaves using prefix compression
{
   const unsigned headerSize = 4; // Next pointer
   unsigned char buffer[pageSize];
   unsigned bufferPos=headerSize;
   unsigned lastSubject=0;

   for (vector<Triple>::const_iterator iter=facts.begin(),limit=facts.end();iter!=limit;) {
      // Access the current tripple
      unsigned subject=(*iter).subject,predicate=(*iter).predicate;
      unsigned object=(*iter).object,count=1;
      for (++iter;iter!=limit;++iter) {
         if ((*iter).subject!=subject)
            break;
         if (((*iter).predicate==predicate)&&((*iter).object==object))
            continue;
         predicate=(*iter).predicate;
         object=(*iter).object;
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
         if ((subject==lastSubject)&&((subject-lastSubject)<16)&&(count<=8)) {
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
unsigned packFullyAggregatedInner(ofstream& out,const vector<Triple>& data,vector<Triple>& boundaries,unsigned page)
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
unsigned packFullyAggregatedFacts(ofstream& out,Directory& directory,unsigned ordering,const vector<Triple>& facts,unsigned page)
   // Pack the fully aggregated facts using prefix compression
{
   // Write the leave nodes
   vector<Triple> boundaries;
   directory.fullyAggregatedFactStarts[ordering]=page;
   page=packFullyAggregatedLeaves(out,facts,boundaries,page);

   // Only one leaf node? Special case this
   if (boundaries.size()==1) {
      vector<Triple> newBoundaries;
      page=packFullyAggregatedInner(out,boundaries,newBoundaries,page);
      directory.fullyAggregatedFactIndices[ordering]=page-1;
      return page;
   }

   // Write the inner nodes
   while (boundaries.size()>1) {
      vector<Triple> newBoundaries;
      page=packFullyAggregatedInner(out,boundaries,newBoundaries,page);
      swap(boundaries,newBoundaries);
   }
   directory.fullyAggregatedFactIndices[ordering]=page-1;
   return page;
}
//---------------------------------------------------------------------------
unsigned dumpFacts(ofstream& out,Directory& directory,vector<Triple>& facts,unsigned page)
   // Dump all 6 orderings into the database
{
   // Produce the different orderings
   for (unsigned index=0;index<6;index++) {
      cout << "Dumping ordering " << (index+1) << endl;

      // Change the values to fit the desired order
      switch (index) {
         case 0: // subject,predicate,object
            break;
         case 1: // subject,object,predicate
            for (vector<Triple>::iterator iter=facts.begin(),limit=facts.end();iter!=limit;++iter)
               std::swap((*iter).object,(*iter).predicate);
            break;
         case 2: // object,predicate,subject
            for (vector<Triple>::iterator iter=facts.begin(),limit=facts.end();iter!=limit;++iter)
               std::swap((*iter).object,(*iter).subject);
            break;
         case 3: // object,subject,predicate
            for (vector<Triple>::iterator iter=facts.begin(),limit=facts.end();iter!=limit;++iter) {
               std::swap((*iter).object,(*iter).subject);
               std::swap((*iter).object,(*iter).predicate);
            }
            break;
         case 4: // predicate,subject,object
            for (vector<Triple>::iterator iter=facts.begin(),limit=facts.end();iter!=limit;++iter)
               std::swap((*iter).subject,(*iter).predicate);
            break;
         case 5: // predicate,object,subject
            for (vector<Triple>::iterator iter=facts.begin(),limit=facts.end();iter!=limit;++iter) {
               std::swap((*iter).subject,(*iter).predicate);
               std::swap((*iter).object,(*iter).predicate);
            }
            break;
      }

      // Sort the facts accordingly
      sort(facts.begin(),facts.end(),OrderTriple());

      // Dump them in the table
      page=packFacts(out,directory,index,facts,page);

      // Dump the aggregated facts in another table
      page=packAggregatedFacts(out,directory,index,facts,page);

      // Dump the fully aggregated facts
      if ((index&1)==0)
         page=packFullyAggregatedFacts(out,directory,index/2,facts,page);

      // Compute the tuple statistics
      if (facts.begin()==facts.end()) {
         directory.factStatistics[index].groups1=0;
         directory.factStatistics[index].groups2=0;
         directory.factStatistics[index].cardinality=0;
      } else {
         vector<Triple>::const_iterator iter=facts.begin(),limit=facts.end();
         unsigned subject=(*iter).subject,predicate=(*iter).predicate,object=(*iter).object;
         directory.factStatistics[index].groups1=1;
         directory.factStatistics[index].groups2=1;
         directory.factStatistics[index].cardinality=1;
         for (++iter;iter!=limit;++iter) {
            if ((*iter).subject!=subject) {
               directory.factStatistics[index].groups1++;
               directory.factStatistics[index].groups2++;
               directory.factStatistics[index].cardinality++;
               subject=(*iter).subject; predicate=(*iter).predicate; object=(*iter).object;
            } else if ((*iter).predicate!=predicate) {
               directory.factStatistics[index].groups2++;
               directory.factStatistics[index].cardinality++;
               predicate=(*iter).predicate; object=(*iter).object;
            } else if ((*iter).object!=object) {
               directory.factStatistics[index].cardinality++;
               object=(*iter).object;
            }
         }
      }


      // Change the values back to the original order
      switch (index) {
         case 0: // subject,predicate,object
            break;
         case 1: // subject,object,predicate
            for (vector<Triple>::iterator iter=facts.begin(),limit=facts.end();iter!=limit;++iter)
               std::swap((*iter).object,(*iter).predicate);
            break;
         case 2: // object,predicate,subject
            for (vector<Triple>::iterator iter=facts.begin(),limit=facts.end();iter!=limit;++iter)
               std::swap((*iter).object,(*iter).subject);
            break;
         case 3: // object,subject,predicate
            for (vector<Triple>::iterator iter=facts.begin(),limit=facts.end();iter!=limit;++iter) {
               std::swap((*iter).object,(*iter).predicate);
               std::swap((*iter).object,(*iter).subject);
            }
            break;
         case 4: // predicate,subject,object
            for (vector<Triple>::iterator iter=facts.begin(),limit=facts.end();iter!=limit;++iter)
               std::swap((*iter).subject,(*iter).predicate);
            break;
         case 5: // predicate,object,subject
            for (vector<Triple>::iterator iter=facts.begin(),limit=facts.end();iter!=limit;++iter) {
               std::swap((*iter).object,(*iter).predicate);
               std::swap((*iter).subject,(*iter).predicate);
            }
            break;
      }
   }
   return page;
}
//---------------------------------------------------------------------------
/// A string description
struct StringEntry {
   /// The id of the string
   unsigned id;
   /// The page the string is put on
   unsigned page;
   /// Offset and len
   unsigned ofsLen;
   /// The hash value of the string
   unsigned hash;
};
//---------------------------------------------------------------------------
/// Order a string entry by id
struct OrderStringById {
   bool operator()(const StringEntry& a,const StringEntry& b) const { return a.id<b.id; }
};
//---------------------------------------------------------------------------
/// Order a string entry by hash
struct OrderStringByHash {
   bool operator()(const StringEntry& a,const StringEntry& b) const { return a.hash<b.hash; }
};
//---------------------------------------------------------------------------
unsigned readAndPackStrings(ofstream& out,Directory& directory,vector<StringEntry>& strings,const char* fileName,unsigned page)
   // Read the facts table and pack it into the output file
{
   ifstream in(fileName);
   if (!in.is_open()) {
      cout << "unable to open " << fileName << endl;
      return false;
   }

   // Prepare the buffer
   const unsigned headerSize = 8; // next+count
   unsigned char buffer[pageSize];
   unsigned bufferPos=headerSize,bufferCount=0;
   directory.stringStart=page;

   // Scan the strings and dump them
   strings.clear();
   string s;
   while (true) {
      unsigned id;
      in >> id;
      in.get();
      getline(in,s);
      if (!in.good()) break;
      while (s.length()&&((s[s.length()-1]=='\r')||(s[s.length()-1]=='\n')))
         s=s.substr(0,s.length()-1);

      // Is the page full?
      if (bufferPos+12+s.length()>pageSize) {
         for (unsigned index=bufferPos;index<pageSize;index++)
            buffer[index]=0;
         writeUint32(buffer,page+1);
         writeUint32(buffer+4,bufferCount);
         writePage(out,page,buffer);
         ++page;
         bufferPos=headerSize; bufferCount=0;
      }
      // Hash the current string...
      unsigned hash=Hash::hash(s);

      // ...store it...
      writeUint32(buffer+bufferPos,id); bufferPos+=4;
      writeUint32(buffer+bufferPos,hash); bufferPos+=4;
      writeUint32(buffer+bufferPos,s.length()); bufferPos+=4;
      unsigned ofs=bufferPos;
      for (unsigned index=0;index<s.length();index++)
         buffer[bufferPos++]=s[index];
      ++bufferCount;

      // ...and remember its position
      StringEntry e;
      e.id=id;
      e.page=page;
      e.ofsLen=(ofs<<16)|(s.length());
      e.hash=hash;
      strings.push_back(e);
   }
   // Flush the last page
   for (unsigned index=bufferPos;index<pageSize;index++)
      buffer[index]=0;
   writeUint32(buffer,0);
   writeUint32(buffer+4,bufferCount);
   writePage(out,page,buffer);
   ++page;

   return page;
}
//---------------------------------------------------------------------------
unsigned writeStringMapping(ofstream& out,Directory& directory,vector<StringEntry>& strings,unsigned page)
   // Write the string mapping
{
   // Sort the strings by id
   sort(strings.begin(),strings.end(),OrderStringById());

   // Prepare the buffer
   unsigned char buffer[pageSize];
   unsigned bufferPos=0;
   directory.stringMapping=page;

   // Dump the page number
   unsigned nextId=0;
   bool warned=false;
   for (vector<StringEntry>::const_iterator iter=strings.begin(),limit=strings.end();iter!=limit;++iter) {
      // Is the page full?
      if (bufferPos==pageSize) {
         writePage(out,page,buffer);
         ++page;
         bufferPos=0;
      }
      // Do we have to write dummy entries?
      if ((*iter).id>nextId++) {
         if (!warned) {
            cout << "warning: sparse string ids detected, can be very inefficient!" << endl;
            warned=true;
         }
         writeUint32(buffer+bufferPos,0); bufferPos+=4;
         writeUint32(buffer+bufferPos,0); bufferPos+=4;
         --iter;
         continue;
      }
      // Write the page number and ofs/len
      writeUint32(buffer+bufferPos,(*iter).page); bufferPos+=4;
      writeUint32(buffer+bufferPos,(*iter).ofsLen); bufferPos+=4;
   }
   // Write the last page
   for (unsigned index=bufferPos;index<pageSize;index++)
      buffer[index]=0;
   writePage(out,page,buffer);
   ++page;

   return page;
}
//---------------------------------------------------------------------------
unsigned writeStringLeaves(ofstream& out,const vector<StringEntry>& strings,vector<pair<unsigned,unsigned> >& boundaries,unsigned page)
   // Write the leaf nodes of the string index
{
   // Prepare the buffer
   const unsigned headerSize = 8; // next+count
   unsigned char buffer[pageSize];
   unsigned bufferPos=headerSize,bufferCount=0;

   // Scan the strings
   for (vector<StringEntry>::const_iterator iter=strings.begin(),limit=strings.end(),next;iter!=limit;) {
      // Find the next hash value
      for (next=iter;next!=limit;++next)
         if ((*iter).hash!=(*next).hash)
            break;
      // Too big for the current page?
      if ((bufferPos+8*(next-iter))>pageSize) {
         // Too big for any page?
         if ((headerSize+8*(next-iter))>pageSize) {
            cout << "error: too many hash collisions in string table, chaining currently not implemented." << endl;
            return 0;
         }
         // Write the current page
         for (unsigned index=bufferPos;index<pageSize;index++)
            buffer[index]=0;
         writeUint32(buffer,page+1);
         writeUint32(buffer+4,bufferCount);
         writePage(out,page,buffer);
         boundaries.push_back(pair<unsigned,unsigned>((*(iter-1)).hash,page));
         ++page;
         bufferPos=headerSize; bufferCount=0;
      }
      // Write the chain
      for (;iter!=next;++iter) {
         writeUint32(buffer+bufferPos,(*iter).hash); bufferPos+=4;
         writeUint32(buffer+bufferPos,(*iter).page); bufferPos+=4;
         bufferCount++;
      }
   }

   // Flush the last page
   for (unsigned index=bufferPos;index<pageSize;index++)
      buffer[index]=0;
   writeUint32(buffer,0);
   writeUint32(buffer+4,bufferCount);
   writePage(out,page,buffer);
   boundaries.push_back(pair<unsigned,unsigned>(strings.back().hash,page));
   ++page;

   return page;
}
//---------------------------------------------------------------------------
unsigned writeStringInner(ofstream& out,const vector<pair<unsigned,unsigned> >& data,vector<pair<unsigned,unsigned> >& boundaries,unsigned page)
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
unsigned writeStringIndex(ofstream& out,Directory& directory,vector<StringEntry>& strings,unsigned page)
   // Write the string index
{
   // Sort the strings by hash value
   sort(strings.begin(),strings.end(),OrderStringByHash());

   // Write the leaf nodes
   vector<pair<unsigned,unsigned> > boundaries;
   page=writeStringLeaves(out,strings,boundaries,page);

   // Only one leaf node? Special case this
   if (boundaries.size()==1) {
      vector<pair<unsigned,unsigned> > newBoundaries;
      page=writeStringInner(out,boundaries,newBoundaries,page);
      directory.stringIndex=page-1;
      return page;
   }

   // Write the inner nodes
   while (boundaries.size()>1) {
      vector<pair<unsigned,unsigned> > newBoundaries;
      page=writeStringInner(out,boundaries,newBoundaries,page);
      swap(boundaries,newBoundaries);
   }
   directory.stringIndex=page-1;
   return page;
}
//---------------------------------------------------------------------------
void writeDirectory(ofstream& out,Directory& directory)
   // Write the directory page
{
   unsigned char buffer[pageSize];
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
      writeUint32(buffer+bufferPos,directory.statistics[6]); bufferPos+=4;
   }

   // Pad the page and write it to the beginning of the file
   for (unsigned index=bufferPos;index<pageSize;index++)
      buffer[index]=0;
   out.seekp(0,ios_base::beg);
   writePage(out,0,buffer);
}
//---------------------------------------------------------------------------
bool buildDatabase(const char* dbFile,const char* factsFile,const char* stringsFile,Directory& directory)
   // Build the initial database
{
   // Prepare the output
   ofstream out(dbFile,ios::out|ios::trunc|ios::binary);
   {
      // Clear the first page, it will be written later
      char buffer[pageSize];
      for (unsigned index=0;index<pageSize;index++)
         buffer[index]=0;
      out.write(buffer,pageSize);
   }

   // Process the facts
   unsigned page=1;
   {
      // Read the facts table
      cout << "Reading the facts table..." << endl;
      vector<Triple> facts;
      if (!readFacts(facts,factsFile))
         return false;

      // Produce the different orderings
      page=dumpFacts(out,directory,facts,page);
   }

   // Process the strings
   {
      // Read the strings table
      cout << "Reading the strings table..." << endl;
      vector<StringEntry> strings;
      if ((page=readAndPackStrings(out,directory,strings,stringsFile,page))==0)
         return 1;

      // Write the string mapping
      cout << "Writing the string mapping..." << endl;
      page=writeStringMapping(out,directory,strings,page);

      // Write the string index
      cout << "Writing the string index..." << endl;
      page=writeStringIndex(out,directory,strings,page);
   }

   // Prepare empty pages for statistics
   {
      char buffer[pageSize];
      for (unsigned index=0;index<pageSize;index++)
         buffer[index]=0;
      for (unsigned index=0;index<6;index++) {
         writePage(out,page,buffer);
         directory.statistics[index]=page;
         ++page;
      }
   }

   // Finally write the directory page
   writeDirectory(out,directory);

   return true;
}
//---------------------------------------------------------------------------
bool buildDatabaseStatistics(const char* dbFile,const Directory& directory)
   // Build the database statistics
{
   // Open the database again
   Database db;
   if (!db.open(dbFile)) {
      cout << "Unable to open " << dbFile << endl;
      return false;
   }

   // Compute the individual statistics
   cout << "Computing statistics..." << endl;
   unsigned char statisticPages[6][pageSize];
   for (unsigned index=0;index<6;index++) {
      cout << "Building statistic " << (index+1) << "..." << endl;
      buildStatisticsPage(db,static_cast<Database::DataOrder>(index),statisticPages[index]);
   }

   // Close the database
   db.close();

   // And patch the statistics
   ofstream out(dbFile,ios::in|ios::out|ios::ate|ios::binary);
   if (!out.is_open()) {
      cout << "Unable to write " << dbFile << endl;
      return false;
   }
   out.seekp(directory.statistics[0]*pageSize,ios::beg);
   for (unsigned index=0;index<6;index++)
      writePage(out,directory.statistics[index],statisticPages[index]);

   return true;
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
int main(int argc,char* argv[])
{
   if (argc!=4) {
      cout << "usage: " << argv[0] << " <facts> <strings> <target>" << endl;
      return 1;
   }

   // Build the initial database
   Directory directory;
   if (!buildDatabase(argv[3],argv[1],argv[2],directory))
      return 1;

   // Compute the missing statistics
   if (!buildDatabaseStatistics(argv[3],directory))
      return 1;
}
//---------------------------------------------------------------------------
