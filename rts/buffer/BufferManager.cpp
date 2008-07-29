#include "rts/buffer/BufferManager.hpp"
#include <algorithm>
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
