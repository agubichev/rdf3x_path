#ifndef H_rts_segment_PathSelectivitySegment
#define H_rts_segment_PathSelectivitySegment
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
#include "rts/segment/Segment.hpp"
#include "rts/database/Database.hpp"
//---------------------------------------------------------------------------
/// Precomputed information for reachability queries
class PathSelectivitySegment: public Segment {
	public:
	/// The segment id
	static const Segment::Type ID = Segment::Type_PathSelectivity;

	// direction of the scan
	enum Direction {Forward, Backward};

	/// Constructor
	PathSelectivitySegment(DatabasePartition& partition);
	Segment::Type getType() const;

	/// Lookup selectivity for a given node and scan direction
	bool lookupSelectivity(unsigned id, Direction d, unsigned& sel);


	/// A source for selectivity info
	class SelectivitySource {
	   public:
	   /// Destructor
	   virtual ~SelectivitySource();

	   /// Get the next entry
	   virtual bool next(unsigned& node,Direction& dir, unsigned& sel) = 0;
	};


	class HashIndexImplementation;
	class HashIndex;
	private:
	/// The root of the index b-tree
	unsigned indexRoot;
	unsigned indexLabelsRoot;

	/// Refresh segment info stored in the partition
	void refreshInfo();
    /// load information about path selectivity
	void loadSelectivitySource(SelectivitySource& reader);

	friend class DatabaseBuilder;

};
//---------------------------------------------------------------------------
#endif
