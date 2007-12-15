#include "rts/buffer/BufferManager.hpp"
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
