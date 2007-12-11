#ifndef H_rts_buffer_BufferManager
#define H_rts_buffer_BufferManager
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
   BufferReference(const BufferRequest& request);
   /// Note: Currently there is no destructor as all pages are accessible all the time. This might change!

   /// Remap the reference to a different page
   BufferReference& operator=(const BufferRequest& request);
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
   void readShared(BufferReference& ref,unsigned page) { ref.page=file.getBegin()+(page*pageSize); }
   /// Access a page
   void readExclusive(BufferReference& ref,unsigned page) { ref.page=file.getBegin()+(page*pageSize); }
};
//---------------------------------------------------------------------------
#endif
