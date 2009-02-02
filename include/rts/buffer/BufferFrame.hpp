#ifndef H_rts_buffer_BufferFrame
#define H_rts_buffer_BufferFrame
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
#include "rts/partition/Partition.hpp"
#include "infra/osdep/MemoryMappedFile.hpp"
#include "infra/osdep/Latch.hpp"
//---------------------------------------------------------------------------
class BufferManager;
class Transaction;
//---------------------------------------------------------------------------
/// A buffer frame. References some part of the database
class BufferFrame
{
   private:
   /// Possible states
   enum State { Empty, Read, Write, WriteDirty };

   /// The buffer manager
   BufferManager* buffer;
   /// The lack
   Latch latch;
   /// Does someone currently try to lock the frame?
   unsigned intentionLock;
   /// The associated data
   void* data;
   /// The partition
   Partition* partition;
   /// Page info used by the partition
   Partition::PageInfo pageInfo;
   /// The page
   unsigned pageNo;
   /// The log sequence number
   uint64_t lsn;
   /// THe state
   State state;

   /// Grant the buffer manager access
   friend class BufferManager;
   /// The transaction has to change the LSN
   friend class Transaction;

   BufferFrame(const BufferFrame&);
   void operator=(const BufferFrame&);

   public:
   /// Constructor
   BufferFrame();

   /// The associated buffer manager
   BufferManager* getBufferManager() const { return buffer; }
   /// The page data
   const void* pageData() const { return data; }
   /// The page data
   void* pageData() { return data; }
   /// The page number
   unsigned getPageNo() const { return pageNo; }
   /// The paritition
   Partition* getPartition() const { return partition; }
   /// Mark as modified
   BufferFrame* update() const;

   /// The LSN
   uint64_t getLSN() const { return lsn; }
};
//---------------------------------------------------------------------------
#endif
