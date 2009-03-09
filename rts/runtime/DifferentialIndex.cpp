#include "rts/runtime/DifferentialIndex.hpp"
#include "rts/database/Database.hpp"
#include "rts/segment/FactsSegment.hpp"
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
using namespace std;
//---------------------------------------------------------------------------
DifferentialIndex::DifferentialIndex(Database& db)
   : db(db)
   // Constructor
{
}
//---------------------------------------------------------------------------
DifferentialIndex::~DifferentialIndex()
   // Destructor
{
}
//---------------------------------------------------------------------------
void DifferentialIndex::load(const vector<Triple>& mewTriples)
   // Load new triples
{
   static const unsigned created = 0;
   
   latch.lockExclusive();
   
   // SPO
   for (vector<Triple>::const_iterator iter=mewTriples.begin(),limit=mewTriples.end();iter!=limit;++iter)
      triples[0].insert(VersionedTriple((*iter).subject,(*iter).predicate,(*iter).object,created,~0u));
   // SOP
   for (vector<Triple>::const_iterator iter=mewTriples.begin(),limit=mewTriples.end();iter!=limit;++iter)
      triples[1].insert(VersionedTriple((*iter).subject,(*iter).object,(*iter).predicate,created,~0u));
   // PSO
   for (vector<Triple>::const_iterator iter=mewTriples.begin(),limit=mewTriples.end();iter!=limit;++iter)
      triples[2].insert(VersionedTriple((*iter).predicate,(*iter).subject,(*iter).object,created,~0u));
   // POS
   for (vector<Triple>::const_iterator iter=mewTriples.begin(),limit=mewTriples.end();iter!=limit;++iter)
      triples[3].insert(VersionedTriple((*iter).predicate,(*iter).object,(*iter).subject,created,~0u));
   // OSP
   for (vector<Triple>::const_iterator iter=mewTriples.begin(),limit=mewTriples.end();iter!=limit;++iter)
      triples[4].insert(VersionedTriple((*iter).object,(*iter).subject,(*iter).predicate,created,~0u));
   // OPS
   for (vector<Triple>::const_iterator iter=mewTriples.begin(),limit=mewTriples.end();iter!=limit;++iter)
      triples[5].insert(VersionedTriple((*iter).object,(*iter).predicate,(*iter).subject,created,~0u));

   latch.unlock();
}
//---------------------------------------------------------------------------
namespace {
//---------------------------------------------------------------------------
/// Loads triples for consumption
class TriplesLoader : public FactsSegment::Source
{
   private:
   /// The range
   set<DifferentialIndex::VersionedTriple>::iterator iter,limit;

   public:
   /// Constructor
   TriplesLoader(set<DifferentialIndex::VersionedTriple>::iterator iter,set<DifferentialIndex::VersionedTriple>::iterator limit) : iter(iter),limit(limit) {}

   /// Get the next triple
   bool next(unsigned& value1,unsigned& value2,unsigned& value3);
   /// Mark the last entry as duplicate
   void markAsDuplicate();
};
//---------------------------------------------------------------------------
bool TriplesLoader::next(unsigned& value1,unsigned& value2,unsigned& value3)
   // Get the next triple
{
   if (iter==limit)
      return false;
   
   value1=(*iter).value1;
   value2=(*iter).value2;
   value3=(*iter).value3;
   ++iter;
   
   return true;
}
//---------------------------------------------------------------------------
void TriplesLoader::markAsDuplicate()
   // Mark the last entry as duplicate
{
   set<DifferentialIndex::VersionedTriple>::iterator last=iter;
   --last;
   const_cast<DifferentialIndex::VersionedTriple&>(*last).deleted=0;
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
void DifferentialIndex::sync()
   // Synchronize with the underlying database
{
   for (unsigned index=0;index<6;index++) {
      TriplesLoader loader(triples[index].begin(),triples[index].end());
      db.getFacts(static_cast<Database::DataOrder>(index)).update(loader);
      triples[index].clear();
   }
}
//---------------------------------------------------------------------------
