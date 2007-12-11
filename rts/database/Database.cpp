#include "rts/database/Database.hpp"
#include "rts/buffer/BufferManager.hpp"
#include "rts/segment/DictionarySegment.hpp"
#include "rts/segment/FactsSegment.hpp"
//---------------------------------------------------------------------------
Database::Database()
   : bufferManager(0),dictionary(0)
   // Constructor
{
   for (unsigned index=0;index<6;index++)
      facts[index]=0;
}
//---------------------------------------------------------------------------
Database::~Database()
   // Destructor
{
   close();
}
//---------------------------------------------------------------------------
static unsigned readUint32(const unsigned char* data) { return (data[0]<<24)|(data[1]<<16)|(data[2]<<8)|data[3]; }
//---------------------------------------------------------------------------
bool Database::open(const char* fileName)
   // Open a database
{
   close();

   // Try to open the database
   bufferManager=new BufferManager();
   if (bufferManager->open(fileName)&&(bufferManager->getPageCount()>0)) {
      // Read the directory page
      BufferReference directory;
      bufferManager->readExclusive(directory,0);
      const unsigned char* page=static_cast<const unsigned char*>(directory.getPage());

      // Check the header
      if ((readUint32(page)==(('R'<<24)|('D'<<16)|('F'<<8)))&&(readUint32(page+4)==1)) {
         // Read the page infos
         unsigned factStarts[6],factIndices[6];
         for (unsigned index=0;index<6;index++) {
            factStarts[index]=readUint32(page+8+index*8);
            factIndices[index]=readUint32(page+8+index*8+4);
         }
         unsigned stringStart=readUint32(page+56);
         unsigned stringMapping=readUint32(page+60);
         unsigned stringIndex=readUint32(page+64);

         // Construct the segments
         for (unsigned index=0;index<6;index++)
            facts[index]=new FactsSegment(*bufferManager,factStarts[index],factIndices[index]);
         dictionary=new DictionarySegment(*bufferManager,stringStart,stringMapping,stringIndex);

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
   }
   delete dictionary;
   dictionary=0;
   delete bufferManager;
   bufferManager=0;
}
//---------------------------------------------------------------------------
DictionarySegment& Database::getDictionary()
   // Get the dictionary
{
   return *dictionary;
}
//---------------------------------------------------------------------------
