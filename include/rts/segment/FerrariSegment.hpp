#ifndef H_rts_segment_FerrariSegment
#define H_rts_segment_FerrariSegment
//---------------------------------------------------------------------------
// RDF-3X
// (c) 2013 Andrey Gubichev. Web site: http://www.mpi-inf.mpg.de/~neumann/rdf3x
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
//---------------------------------------------------------------------------
#include "rts/segment/Segment.hpp"
//---------------------------------------------------------------------------
class AggregatedFactsSegment;
class Database;
class DatabaseBuilder;
class Graph;
//---------------------------------------------------------------------------
/// FERRARI reachability index segment
class FerrariSegment : public Segment
{

private:
   friend class DatabaseBuilder;
   /// Position of the directory
   unsigned directoryPage;

   // compute the index
   void computeFerrari(Database& db);
   // serialize the graph
   unsigned packGraph(Graph* g);

   FerrariSegment(const FerrariSegment&);
   void operator=(const FerrariSegment&);
   /// Refresh segment info stored in the partition
   void refreshInfo();
   public:
   FerrariSegment(DatabasePartition& partition);
   /// Get type
   Type getType() const;
   class Dumper;

};

#endif
