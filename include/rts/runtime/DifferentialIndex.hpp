#ifndef H_rts_runtime_DifferentialIndex
#define H_rts_runtime_DifferentialIndex
//---------------------------------------------------------------------------
#include "infra/osdep/Latch.hpp"
#include <set>
#include <vector>
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
class Database;
//---------------------------------------------------------------------------
/// Index for all transient updates
class DifferentialIndex
{
   public:
   /// A regular triple
   struct Triple {
      /// Entries
      unsigned subject,predicate,object;
   };
   /// A versioned triple
   struct VersionedTriple {
      /// Entries
      unsigned value1,value2,value3;
      /// Versions
      unsigned created,deleted;

      /// Constructor
      VersionedTriple(unsigned value1,unsigned value2,unsigned value3,unsigned created,unsigned deleted) : value1(value1),value2(value2),value3(value3),created(created),deleted(deleted) {}
      
      /// Compare
      bool operator<(const VersionedTriple& v) const { return (value1<v.value1)||((value1==v.value1)&&((value2<v.value2)||((value2==v.value2)&&((value3<v.value3)||((value3==v.value3)&&(created<v.created)))))); }
   };

   private:
   /// The underlying database
   Database& db;
   /// Triples
   std::set<VersionedTriple> triples[6];
   /// The latch
   Latch latch;

   public:
   /// Constructor
   explicit DifferentialIndex(Database& db);
   /// Destructor
   ~DifferentialIndex();

   /// Load new triples
   void load(const std::vector<Triple>& triples);

   /// Synchronize with the underlying database
   void sync();
};
//---------------------------------------------------------------------------
#endif
