#include "rts/buffer/BufferManager.hpp"
#include <algorithm>
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
void BufferManager::prefetchPages(unsigned start,unsigned stop)
   // Prefetch a number of pages
{
   // Sanitize the input
   if (start>stop)
      std::swap(start,stop);
   if (start>=getPageCount())
      return;
   if (stop>=getPageCount())
      stop=getPageCount()-1;

   // Prefetch it
   file.prefetch(file.getBegin()+(start*pageSize),file.getBegin()+(stop*pageSize)+pageSize-1);
}
//---------------------------------------------------------------------------
