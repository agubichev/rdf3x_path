#ifndef H_rts_segment_PredicateSetSegment
#define H_rts_segment_PredicateSetSegment
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
#include "rts/segment/Segment.hpp"
//---------------------------------------------------------------------------
/// Statistics about sets of predicates occuring for a subject
class PredicateSetSegment : public Segment
{
   public:
   /// A predset
   struct PredSet;

   private:
   /// The data
   struct Data;

   /// THe data
   Data* data;

   /// Refresh segment info stored in the partition
   void refreshInfo();

   PredicateSetSegment(const PredicateSetSegment&);
   void operator=(const PredicateSetSegment&);

   friend class DatabaseBuilder;

   public:
   /// Constructor
   explicit PredicateSetSegment(DatabasePartition& partition);
   /// Destructor
   ~PredicateSetSegment();

   /// Get the type
   Type getType() const;

   /// Compute the predicate sets (after loading)
   void computePredicateSets();
};
//---------------------------------------------------------------------------
#endif
