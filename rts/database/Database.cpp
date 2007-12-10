#include "rts/database/Database.hpp"
#include "rts/buffer/BufferManager.hpp"
//---------------------------------------------------------------------------
Database::Database()
   : bufferManager(0)
   // Constructor
{
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
      if (readUint32(page)==(('R'<<24)|('D'<<16)|('F'<<8))) {
         // Read the page infos
         for (unsigned index=0;index<6;index++) {
            factStarts[index]=readUint32(page+4+index*8);
            factIndices[index]=readUint32(page+4+index*8+4);
         }
         stringStart=readUint32(page+52);
         stringMapping=readUint32(page+56);
         stringIndex=readUint32(page+50);

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
   delete bufferManager;
   bufferManager=0;
}
//---------------------------------------------------------------------------
