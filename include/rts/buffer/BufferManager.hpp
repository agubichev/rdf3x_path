#ifndef H_rts_buffer_BufferManager
#define H_rts_buffer_BufferManager
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
#include "infra/osdep/MemoryMappedFile.hpp"
//---------------------------------------------------------------------------
class BufferManager;
//---------------------------------------------------------------------------
/// A request to access a buffer page. Used by segments to "return" references.
struct BufferRequest
{
   /// The buffer manager
   BufferManager& bufferManager;
   /// The requested page
   unsigned page;
   /// Shared access?
   bool shared;

   /// Constructor
   BufferRequest(BufferManager& bufferManager,unsigned page,bool shared) : bufferManager(bufferManager),page(page),shared(shared) {}
};
//---------------------------------------------------------------------------
/// A reference to a page in the database buffer.
/// The page remains accessible during the lifetime of the BufferReference object.
class BufferReference
{
   private:
   /// The page reference
   const void* page;

   /// No copying of references
   BufferReference(const BufferReference&);
   void operator=(const BufferReference&);

   /// The buffer manager can change a reference
   friend class BufferManager;

   public:
   /// Constructor
   BufferReference() : page(0) {}
   /// Constructor from a request
   inline BufferReference(const BufferRequest& request);
   /// Note: Currently there is no destructor as all pages are accessible all the time. This might change!

   /// Remap the reference to a different page
   inline BufferReference& operator=(const BufferRequest& request);
   /// Reset the reference
   void reset() { page=0; }

   /// Access the page
   const void* getPage() const { return page; }
};
//---------------------------------------------------------------------------
/// A database buffer backed by a file
class BufferManager
{
   public:
   /// The size of a page
   static const unsigned pageSize = 16384;
   /// A page buffer
   struct PageBuffer { char data[pageSize]; };

   private:
   /// The file
   MemoryMappedFile file;

   BufferManager(const BufferManager&);
   void operator=(const BufferManager&);

   public:
   /// Constructor
   BufferManager();
   /// BufferManager
   ~BufferManager();

   /// Open the file
   bool open(const char* fileName);
   /// Shutdown the buffer and close the file
   void close();

   /// The number of pages
   unsigned getPageCount() const { return (file.getEnd()-file.getBegin())/pageSize; }
   /// Access a page
   void readShared(BufferReference& ref,unsigned page) { ref.page=reinterpret_cast<const PageBuffer*>(file.getBegin())+page; }
   /// Access a page
   void readExclusive(BufferReference& ref,unsigned page) { ref.page=reinterpret_cast<const PageBuffer*>(file.getBegin())+page; }
   /// Prefetch a number of pages
   void prefetchPages(unsigned start,unsigned stop);

   /// Get the ID of a reference
   unsigned getPageId(const BufferReference& ref) const { return static_cast<const PageBuffer*>(ref.page)-reinterpret_cast<const PageBuffer*>(file.getBegin()); }
};
//---------------------------------------------------------------------------
BufferReference::BufferReference(const BufferRequest& request)
   : page(0)
   // Constructor from a request
{
#ifdef SUPPORT_READWRITE
   if (request.shared)
      request.bufferManager.readShared(*this,request.page); else
      request.bufferManager.readExclusive(*this,request.page);
#else
   request.bufferManager.readShared(*this,request.page);
#endif
}
//---------------------------------------------------------------------------
BufferReference& BufferReference::operator=(const BufferRequest& request)
   // Remap the reference to a different page
{
#ifdef SUPPORT_READWRITE
   if (request.shared)
      request.bufferManager.readShared(*this,request.page); else
      request.bufferManager.readExclusive(*this,request.page);
#else
   request.bufferManager.readShared(*this,request.page);
#endif
   return *this;
}
//---------------------------------------------------------------------------
#endif

