#include "rts/buffer/BufferReference.hpp"
#include "rts/buffer/BufferManager.hpp"
//---------------------------------------------------------------------------
// RDF-3X
// (c) 2009 Thomas Neumann. Web site: http://www.mpi-inf.mpg.de/~neumann/rdf3x
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
//---------------------------------------------------------------------------
BufferReference::BufferReference()
   : frame(0)
   // Constructor
{
}
//---------------------------------------------------------------------------
BufferReference::BufferReference(const BufferRequest& request)
   : frame(0)
   // Constructor from a request
{
   operator=(request);
}
//---------------------------------------------------------------------------
BufferReference::~BufferReference()
   // Destructor
{
   reset();
}
//---------------------------------------------------------------------------
BufferReference& BufferReference::operator=(const BufferRequest& request)
   // Remap the reference to a different page
{
   reset();

   if (request.shared) {
      // XXX avoid the const_cast by splitting BufferReference
      frame=const_cast<BufferFrame*>(request.bufferManager.readPageShared(request.partition,request.page));
   } else {
      // XXX avoid the const_cast by splitting BufferReference
      frame=const_cast<BufferFrame*>(request.bufferManager.readPageExclusive(request.partition,request.page));
   }

   return *this;
}
//---------------------------------------------------------------------------
void BufferReference::reset()
   // Reset the reference
{
   if (frame) {
      frame->getBufferManager()->unfixPage(frame);
      frame=0;
   }
}
//---------------------------------------------------------------------------
const void* BufferReference::getPage() const
   // Access the page
{
   return frame->pageData();
}
//---------------------------------------------------------------------------
unsigned BufferReference::pageNo() const
   // Get the page number
{
   return frame->getPageNo();
}
//---------------------------------------------------------------------------
