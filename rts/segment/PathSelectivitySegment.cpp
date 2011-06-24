#include "rts/segment/PathSelectivitySegment.hpp"
#include "infra/osdep/MemoryMappedFile.hpp"
#include "infra/util/fastlz.hpp"
#include "rts/database/Database.hpp"
#include "rts/database/DatabaseBuilder.hpp"
#include "rts/database/DatabasePartition.hpp"
#include "rts/segment/AggregatedFactsSegment.hpp"
#include "rts/segment/FactsSegment.hpp"
#include "rts/segment/FullyAggregatedFactsSegment.hpp"
#include "rts/segment/BTree.hpp"
#include "infra/util/Hash.hpp"
#include <algorithm>
#include <vector>
#include <cstring>
#include <cassert>
#include <iostream>
#include <set>
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
using namespace std;
//---------------------------------------------------------------------------
// Info slots
static const unsigned slotTableStart = 0;
static const unsigned slotIndexRoot = 1;
static const unsigned slotLabelIndexRoot = 2;
static const unsigned slotLabelCount = 3;
//---------------------------------------------------------------------------
PathSelectivitySegment::SelectivitySource::~SelectivitySource()
   // Destructor
{
}
//---------------------------------------------------------------------------
PathSelectivitySegment::PathSelectivitySegment(DatabasePartition& partition)
   : Segment(partition)
   // Constructor
{
}
//---------------------------------------------------------------------------
Segment::Type PathSelectivitySegment::getType() const
   // Get the type
{
   return Segment::Type_PathSelectivity;
}
//---------------------------------------------------------------------------
void PathSelectivitySegment::refreshInfo()
   // Refresh segment info stored in the partition
{
	Segment::refreshInfo();
	indexRoot=getSegmentData(slotIndexRoot);
	indexLabelsRoot=getSegmentData(slotLabelIndexRoot);
}
//---------------------------------------------------------------------------
class PathSelectivitySegment::HashIndexImplementation
{
	public:
	/// The size of an inner key
	static const unsigned innerKeySize = 8;
	/// An inner key
	struct InnerKey {
		/// The node id
		unsigned node;
		/// The scan direction
		PathSelectivitySegment::Direction dir;

		/// Constructor
		InnerKey() : node(0), dir(static_cast<PathSelectivitySegment::Direction>(0)) {}
		/// Constructor
		InnerKey(unsigned n, PathSelectivitySegment::Direction dir) : node(n), dir(dir) {}

		/// Compare
		bool operator==(const InnerKey& o) const { return ((node==o.node) && (dir==o.dir)); }
		/// Compare
		bool operator<(const InnerKey& o) const {return ((node<o.node)||((node==o.node)&&(dir<o.dir)));}
		void print() const{;}
	};

	/// Read an inner key
	static void readInnerKey(InnerKey& key,const unsigned char* ptr) {
		key.node=Segment::readUint32Aligned(ptr);
		key.dir=static_cast<PathSelectivitySegment::Direction>(Segment::readUint32Aligned(ptr+4));
	}
	/// Write an inner key
	static void writeInnerKey(unsigned char* ptr,const InnerKey& key) {
		Segment::writeUint32Aligned(ptr,key.node);
		Segment::writeUint32Aligned(ptr+4,key.dir);
	}

	/// A leaf entry
	struct LeafEntry {
	    /// The node
	    unsigned node;
	    /// Direction of the scan
	    PathSelectivitySegment::Direction dir;
	    /// Selectivity
	    unsigned selectivity;
	    /// Compare
	    bool operator==(const LeafEntry& o) const { return (node==o.node && dir==o.dir); }
	    /// Compare
	    bool operator<(const LeafEntry& o) const {return ((node<o.node)||((node==o.node)&&(dir<o.dir))||((dir==o.dir)&&(selectivity<o.selectivity))||(selectivity<o.selectivity));}
	    /// Compare
	    bool operator<(const InnerKey& o) const {return ((node<o.node)||((node==o.node)&&(dir<o.dir))); }
	    void print(){ ;}
	};
	/// A leaf entry source
	class LeafEntrySource {
	    private:
	    /// The real source
	    PathSelectivitySegment::SelectivitySource& source;
	    public:
	    /// Constructor
	    LeafEntrySource(PathSelectivitySegment::SelectivitySource& source) : source(source) {}
	    /// Read the next entry
	    bool next(LeafEntry& l) { return source.next(l.node,l.dir,l.selectivity); }
	};

	/// Derive an inner key
	static InnerKey deriveInnerKey(const LeafEntry& e) { return InnerKey(e.node,e.dir); }
	/// Read the first leaf entry
	static void readFirstLeafEntryKey(InnerKey& key,const unsigned char* ptr) {
	   key.node=Segment::readUint32Aligned(ptr);
	   key.dir=static_cast<PathSelectivitySegment::Direction>(Segment::readUint32Aligned(ptr+4));
	}

	private:
	/// The segment
	PathSelectivitySegment& segment;

	public:
	/// Constructor
	explicit HashIndexImplementation(PathSelectivitySegment& segment) : segment(segment) {}
	/// Get the segment
	Segment& getSegment() const { return segment; }
	/// Read a specific page
	BufferRequest readShared(unsigned page) const { return segment.readShared(page); }
	/// Read a specific page
	BufferRequestExclusive readExclusive(unsigned page) const { return segment.readExclusive(page); }
	/// Allocate a new page
	bool allocPage(BufferReferenceModified& page) { return segment.allocPage(page); }
	/// Get the root page
	unsigned getRootPage() const { return segment.indexRoot; }
	/// Set the root page
	void setRootPage(unsigned page);
	/// Store info about the leaf pages
	void updateLeafInfo(unsigned /*firstLeaf*/,unsigned /*leafCount*/) {}

	/// Pack leaf entries
	static unsigned packLeafEntries(unsigned char* writer,unsigned char* limit,vector<LeafEntry>::const_iterator entriesStart,vector<LeafEntry>::const_iterator entriesLimit);
	/// Unpack leaf entries
	static void unpackLeafEntries(vector<LeafEntry>& entries,const unsigned char* reader,const unsigned char* limit);
};
//---------------------------------------------------------------------------
void PathSelectivitySegment::HashIndexImplementation::setRootPage(unsigned page)
   // Set the root page
{
   segment.indexRoot=page;
   segment.setSegmentData(slotIndexRoot,segment.indexRoot);
}
//---------------------------------------------------------------------------
unsigned PathSelectivitySegment::HashIndexImplementation::packLeafEntries(unsigned char* writer,unsigned char* writerLimit,vector<PathSelectivitySegment::HashIndexImplementation::LeafEntry>::const_iterator entriesStart,vector<PathSelectivitySegment::HashIndexImplementation::LeafEntry>::const_iterator entriesLimit)
   // Store the hash/page pairs
{
   // Too small?
   if ((writerLimit-writer)<4){
      return 0;
   }

   // Compute the output len
   unsigned maxLen=((writerLimit-writer)-4)/(3*4);
   unsigned inputLen=entriesLimit-entriesStart;
   unsigned len=min(maxLen,inputLen);
   // Write the count
   Segment::writeUint32Aligned(writer,len); writer+=4;

   // Store the entries
   for (unsigned index=0;index<len;++index,++entriesStart) {
      Segment::writeUint32Aligned(writer,(*entriesStart).node); writer+=4;
      Segment::writeUint32Aligned(writer,static_cast<unsigned int>((*entriesStart).dir)); writer+=4;
      Segment::writeUint32Aligned(writer,(*entriesStart).selectivity); writer+=4;
   }

   // Pad the remaining space
   memset(writer,0,writerLimit-writer);

   return len;
}
//---------------------------------------------------------------------------
void PathSelectivitySegment::HashIndexImplementation::unpackLeafEntries(vector<PathSelectivitySegment::HashIndexImplementation::LeafEntry>& entries,const unsigned char* reader,const unsigned char* /*limit*/)
   // Read the hash/page pairs
{
   // Read the len
   unsigned len=Segment::readUint32Aligned(reader); reader+=4;
   // Read the entries
   entries.resize(len);
   for (unsigned index=0;index<len;index++) {
      entries[index].node=Segment::readUint32Aligned(reader); reader+=4;
      entries[index].dir=static_cast<PathSelectivitySegment::Direction>(Segment::readUint32Aligned(reader)); reader+=4;
      entries[index].selectivity=Segment::readUint32Aligned(reader); reader+=4;
   }
}
//---------------------------------------------------------------------------
/// Index node -> direction, selectivity
class PathSelectivitySegment::HashIndex : public BTree<HashIndexImplementation>
{
   public:
   /// Constructor
   explicit HashIndex(PathSelectivitySegment& segment) : BTree<HashIndexImplementation>(segment) {}

   /// Size of the leaf header (used for scans)
   using BTree<HashIndexImplementation>::leafHeaderSize;
};
//---------------------------------------------------------------------------
void PathSelectivitySegment::loadSelectivitySource(SelectivitySource& reader){
	HashIndex::LeafEntrySource source(reader);

//	PathSelectivitySegment::Direction d;
//	unsigned a,b;
//	while (reader.next(a,d,b))
//		cerr<<a<<" "<<d<<" "<<b<<endl;

	HashIndex(*this).performBulkload(source);
}
//---------------------------------------------------------------------------
bool PathSelectivitySegment::lookupSelectivity(unsigned id, PathSelectivitySegment::Direction d, unsigned& sel)
   // Lookup the selectivity for given id and direction
{
	// Find the leaf page
	BufferReference ref;
	if (!HashIndex(*this).findLeaf(ref,HashIndex::InnerKey(id,d))){
		return false;
	}
	// A leaf node. Perform a binary search on the exact value.
	const unsigned char* page=static_cast<const unsigned char*>(ref.getPage());
	unsigned left=0,right=readUint32Aligned(page+HashIndex::leafHeaderSize);
	while (left!=right) {
		unsigned middle=(left+right)/2;
		unsigned idAtMiddle=readUint32Aligned(page+HashIndex::leafHeaderSize+4+3*4*middle);
		if (id>idAtMiddle) {
			left=middle+1;
		} else if (id<idAtMiddle) {
			right=middle;
		} else {
			// We found a match.
			left=middle;
			while (left && readUint32Aligned(page+HashIndex::leafHeaderSize+4+3*4*(left-1))==id)
				--left;
			break;
		}
	}
	// Unsuccessful search?
	if (left==right)
		return false;

	PathSelectivitySegment::Direction dir1,dir2;


	dir1 = static_cast<PathSelectivitySegment::Direction>(readUint32Aligned(page+HashIndex::leafHeaderSize+4+(3*4*left)+4));
	dir2 = static_cast<PathSelectivitySegment::Direction>(readUint32Aligned(page+HashIndex::leafHeaderSize+4+(3*4*left)+4+12));

//	cerr<<"dir1, dir2: "<<dir1<<" "<<" "<<dir2<<endl;
//	cerr<<"id1, id2: "<<readUint32Aligned(page+HashIndex::leafHeaderSize+4+3*4*left)<<" "<<readUint32Aligned(page+HashIndex::leafHeaderSize+4+3*4*(left+1))<<endl;
	if (dir1==d && readUint32Aligned(page+HashIndex::leafHeaderSize+4+3*4*left) == id)
		sel=readUint32Aligned(page+HashIndex::leafHeaderSize+4+(3*4*left)+8);
	else if (readUint32Aligned(page+HashIndex::leafHeaderSize+4+3*4*(left+1)) == id)
		sel=readUint32Aligned(page+HashIndex::leafHeaderSize+4+3*4*(left+1)+8);


	//we reached the end of the page
	return false;
}
