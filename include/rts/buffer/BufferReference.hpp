#ifndef H_rts_buffer_BufferReference
#define H_rts_buffer_BufferReference
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
class BufferManager;
class BufferFrame;
class Partition;
//---------------------------------------------------------------------------
/// A request to access a buffer page. Used by segments to "return" references.
struct BufferRequest
{
   /// The buffer manager
   BufferManager& bufferManager;
   /// The partition
   Partition& partition;
   /// The requested page
   unsigned page;
   /// Shared access?
   bool shared;

   /// Constructor
   BufferRequest(BufferManager& bufferManager,Partition& partition,unsigned page,bool shared) : bufferManager(bufferManager),partition(partition),page(page),shared(shared) {}
};
//---------------------------------------------------------------------------
/// A reference to a page in the database buffer.
/// The page remains accessible during the lifetime of the BufferReference object.
class BufferReference
{
   public:
   /// The size of a page
   static const unsigned pageSize = 16384;
   /// A page buffer
   struct PageBuffer { char data[pageSize]; };

   private:
   /// The buffer frame
   BufferFrame* frame;

   /// No copying of references
   BufferReference(const BufferReference&);
   void operator=(const BufferReference&);

   /// The buffer manager can change a reference
   friend class BufferManager;

   public:
   /// Constructor
   BufferReference();
   /// Constructor from a request
   BufferReference(const BufferRequest& request);
   /// Destructor
   ~BufferReference();

   /// Remap the reference to a different page
   BufferReference& operator=(const BufferRequest& request);
   /// Reset the reference
   void reset();

   /// Access the page
   const void* getPage() const;
   /// Get the page number
   unsigned pageNo() const;
};
//---------------------------------------------------------------------------
#endif
