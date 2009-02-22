#include "rts/database/Database.hpp"
#include "rts/buffer/BufferManager.hpp"
#include "rts/database/DatabasePartition.hpp"
#include "rts/partition/FilePartition.hpp"
#include "rts/segment/AggregatedFactsSegment.hpp"
#include "rts/segment/DictionarySegment.hpp"
#include "rts/segment/ExactStatisticsSegment.hpp"
#include "rts/segment/FactsSegment.hpp"
#include "rts/segment/FullyAggregatedFactsSegment.hpp"
#include "rts/segment/StatisticsSegment.hpp"
#include "rts/segment/PathStatisticsSegment.hpp"
#include <cassert>
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
/// Active buffer size. This is only a hint!
static const unsigned bufferSize = 16*1024*1024;
//---------------------------------------------------------------------------
Database::Database()
   : file(0),bufferManager(0),partition(0),dictionary(0),exactStatistics(0)
   // Constructor
{
   for (unsigned index=0;index<6;index++) {
      facts[index]=0;
      aggregatedFacts[index]=0;
      statistics[index]=0;
   }
   for (unsigned index=0;index<3;index++)
      fullyAggregatedFacts[index]=0;
   for (unsigned index=0;index<2;index++)
      pathStatistics[index]=0;
}
//---------------------------------------------------------------------------
Database::~Database()
   // Destructor
{
   close();
}
//---------------------------------------------------------------------------
static void writeUint64(unsigned char* writer,uint64_t value)
   // Write a 64bit integer value
{
   writer[0]=static_cast<unsigned char>(value>>56);
   writer[1]=static_cast<unsigned char>(value>>48);
   writer[2]=static_cast<unsigned char>(value>>40);
   writer[3]=static_cast<unsigned char>(value>>32);
   writer[4]=static_cast<unsigned char>(value>>24);
   writer[5]=static_cast<unsigned char>(value>>16);
   writer[6]=static_cast<unsigned char>(value>>8);
   writer[7]=static_cast<unsigned char>(value>>0);
}
//---------------------------------------------------------------------------
bool Database::create(const char* fileName)
   // Create a new database
{
   // Try to create the partition
   file=new FilePartition();
   if (!file->create(fileName))
      return false;
   unsigned start,len;
   if (!file->grow(4,start,len))
      return false;
   assert((start==0)&&(len==4));
   bufferManager=new BufferManager(bufferSize);
   partition=new DatabasePartition(*bufferManager,*file);

   // Create the inventory segments
   partition->create();

   // Format the root page
   {
      Partition::PageInfo pageInfo;
      unsigned char* page=static_cast<unsigned char*>(file->buildPage(0,pageInfo));

      // Magic
      page[0]='R'; page[1]='D'; page[2]='F'; page[3]=0;
      page[4]=0;   page[5]=0;   page[6]=0;   page[7]=2;

      // Root SN
      rootSN=1;
      writeUint64(page+8,rootSN);
      writeUint64(page+BufferReference::pageSize-8,rootSN);

      // Start LSN
      startLSN=0;
      writeUint64(page+16,startLSN);

      file->flushWrittenPage(pageInfo);
      file->finishWrittenPage(pageInfo);
   }
   file->flush();

   return true;
}
//---------------------------------------------------------------------------
static unsigned readUint32(const unsigned char* data) { return (data[0]<<24)|(data[1]<<16)|(data[2]<<8)|data[3]; }
//---------------------------------------------------------------------------
static unsigned long long readUint64(const unsigned char* data)
{
   unsigned long long result=0;
   for (unsigned index=0;index<8;index++)
      result=(result<<8)|static_cast<unsigned long long>(data[7-index]);
   return result;
}
//---------------------------------------------------------------------------
bool Database::open(const char* fileName,bool readOnly)
   // Open a database
{
   close();

   // Try to open the database
   bufferManager=new BufferManager(bufferSize);
   file=new FilePartition();
   partition=new DatabasePartition(*bufferManager,*file);
   if (file->open(fileName,readOnly)) {
      // Read the directory page
      BufferReference directory(BufferRequest(*bufferManager,*file,0));
      const unsigned char* page=static_cast<const unsigned char*>(directory.getPage());

      // Check the header
      if ((readUint32(page)==(('R'<<24)|('D'<<16)|('F'<<8)))&&(readUint32(page+4)==1)) {
         // Read the page infos
         unsigned factStarts[6],factIndices[6],aggregatedFactStarts[6],aggregatedFactIndices[6];
         unsigned fullyAggregatedFactStarts[3],fullyAggregatedFactIndices[3];
         unsigned pageCounts[6],aggregatedPageCounts[6],groups1[6],groups2[6],cardinalities[6];
         for (unsigned index=0;index<6;index++) {
            factStarts[index]=readUint32(page+8+index*36);
            factIndices[index]=readUint32(page+8+index*36+4);
            aggregatedFactStarts[index]=readUint32(page+8+index*36+8);
            aggregatedFactIndices[index]=readUint32(page+8+index*36+12);
            pageCounts[index]=readUint32(page+8+index*36+16);
            aggregatedPageCounts[index]=readUint32(page+8+index*36+20);
            groups1[index]=readUint32(page+8+index*36+24);
            groups2[index]=readUint32(page+8+index*36+28);
            cardinalities[index]=readUint32(page+8+index*36+32);
         }
         for (unsigned index=0;index<3;index++) {
            fullyAggregatedFactStarts[index]=readUint32(page+224+index*8);
            fullyAggregatedFactIndices[index]=readUint32(page+224+index*8+4);
         }
         unsigned stringStart=readUint32(page+248);
         unsigned stringMapping=readUint32(page+252);
         unsigned stringIndex=readUint32(page+256);
         unsigned statisticsPages[6];
         for (unsigned index=0;index<6;index++)
            statisticsPages[index]=readUint32(page+260+4*index);
         unsigned pathStatisticsPages[2];
         for (unsigned index=0;index<2;index++)
            pathStatisticsPages[index]=readUint32(page+284+4*index);
         unsigned exactStatisticsPages[6];
         for (unsigned index=0;index<6;index++)
            exactStatisticsPages[index]=readUint32(page+292+4*index);
         unsigned long long exactStatisticsJoinCounts[9];
         for (unsigned index=0;index<9;index++)
            exactStatisticsJoinCounts[index]=readUint64(page+316+8*index);

         // Construct the segments
         for (unsigned index=0;index<6;index++) {
            facts[index]=new FactsSegment(*partition,factStarts[index],factIndices[index],pageCounts[index],groups1[index],groups2[index],cardinalities[index]);
            aggregatedFacts[index]=new AggregatedFactsSegment(*partition,aggregatedFactStarts[index],aggregatedFactIndices[index],aggregatedPageCounts[index],groups1[index],groups2[index]);
            statistics[index]=new StatisticsSegment(*partition,statisticsPages[index]);
         }
         for (unsigned index=0;index<3;index++)
            fullyAggregatedFacts[index]=new FullyAggregatedFactsSegment(*partition,fullyAggregatedFactStarts[index],fullyAggregatedFactIndices[index],fullyAggregatedFactIndices[index]-fullyAggregatedFactStarts[index],groups1[index*2]);
         for (unsigned index=0;index<2;index++)
            pathStatistics[index]=new PathStatisticsSegment(*partition,pathStatisticsPages[index]);
         exactStatistics=new ExactStatisticsSegment(*partition,*this,exactStatisticsPages[0],exactStatisticsPages[1],exactStatisticsPages[2],exactStatisticsPages[3],exactStatisticsPages[4],exactStatisticsPages[5],exactStatisticsJoinCounts[0],exactStatisticsJoinCounts[1],exactStatisticsJoinCounts[2],exactStatisticsJoinCounts[3],exactStatisticsJoinCounts[4],exactStatisticsJoinCounts[5],exactStatisticsJoinCounts[6],exactStatisticsJoinCounts[7],exactStatisticsJoinCounts[8]);
         dictionary=new DictionarySegment(*partition,stringStart,stringMapping,stringIndex);

         return true;
      }
   }

   // Failure, cleanup
   delete bufferManager;
   bufferManager=0;
   return false;
}
//---------------------------------------------------------------------------
void Database::close()
   // Close the current database
{
   for (unsigned index=0;index<6;index++) {
      delete facts[index];
      facts[index]=0;
      delete aggregatedFacts[index];
      aggregatedFacts[index]=0;
      delete statistics[index];
      statistics[index]=0;
   }
   for (unsigned index=0;index<3;index++) {
      delete fullyAggregatedFacts[index];
      fullyAggregatedFacts[index]=0;
   }
   for (unsigned index=0;index<2;index++) {
      delete pathStatistics[index];
      pathStatistics[index]=0;
   }
   delete exactStatistics;
   exactStatistics=0;
   delete dictionary;
   dictionary=0;
   delete partition;
   partition=0;
   delete bufferManager;
   bufferManager=0;
   delete file;
   file=0;
}
//---------------------------------------------------------------------------
FactsSegment& Database::getFacts(DataOrder order)
   // Get the facts
{
   return *(facts[order]);
}
//---------------------------------------------------------------------------
AggregatedFactsSegment& Database::getAggregatedFacts(DataOrder order)
   // Get the facts
{
   return *(aggregatedFacts[order]);
}
//---------------------------------------------------------------------------
FullyAggregatedFactsSegment& Database::getFullyAggregatedFacts(DataOrder order)
   // Get fully aggregated fcats
{
   return *(fullyAggregatedFacts[order/2]);
}
//---------------------------------------------------------------------------
StatisticsSegment& Database::getStatistics(DataOrder order)
   // Get fact statistics
{
   return *(statistics[order]);
}
//---------------------------------------------------------------------------
PathStatisticsSegment& Database::getPathStatistics(bool stars)
   // Get path statistics
{
   return *(pathStatistics[stars]);
}
//---------------------------------------------------------------------------
ExactStatisticsSegment& Database::getExactStatistics()
   // Get the exact statistics
{
   return *exactStatistics;
}
//---------------------------------------------------------------------------
DictionarySegment& Database::getDictionary()
   // Get the dictionary
{
   return *dictionary;
}
//---------------------------------------------------------------------------
