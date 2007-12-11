#include "rts/buffer/BufferManager.hpp"
//---------------------------------------------------------------------------
BufferReference::BufferReference(const BufferRequest& request)
   : page(0)
   // Constructor from a request
{
   operator=(request);
}
//---------------------------------------------------------------------------
BufferReference& BufferReference::operator=(const BufferRequest& request)
   // Remap the reference to a different page
{
   if (request.shared)
      request.bufferManager.readShared(*this,request.page); else
      request.bufferManager.readExclusive(*this,request.page);
   return *this;
}
//---------------------------------------------------------------------------
BufferManager::BufferManager()
   // Constructor
{
}
//---------------------------------------------------------------------------
BufferManager::~BufferManager()
   // BufferManager
{
   close();
}
//---------------------------------------------------------------------------
bool BufferManager::open(const char* fileName)
   // Open the file
{
   return file.open(fileName);
}
//---------------------------------------------------------------------------
void BufferManager::close()
   // Shutdown the buffer and close the file
{
   file.close();
}
//---------------------------------------------------------------------------
