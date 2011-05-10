#ifndef H_rts_segment_PathSelectivity
#define H_rts_segment_PathSelectivity
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
#include <vector>
//---------------------------------------------------------------------------
/// Precomputed information for path scan selectivity
class PathSelectivity {
public:

	/// Constructor
	PathSelectivity() {};
	/// Compute path scan selectivity and write in to disk
	void computeSelectivity(Database& db, std::vector<unsigned>& back_selectivity, std::vector<unsigned>& forw_selectivity);
private:


};

#endif
