#include "rts/runtime/BulkOperation.hpp"
#include "rts/database/Database.hpp"
#include "rts/segment/DictionarySegment.hpp"
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
BulkOperation::BulkOperation(DifferentialIndex& differentialIndex)
   : differentialIndex(differentialIndex)
   // Constructor
{
}
//---------------------------------------------------------------------------
BulkOperation::~BulkOperation()
   // Destructor
{
}
//---------------------------------------------------------------------------
unsigned BulkOperation::mapString(const std::string& value)
   // Map a string
{
   // Local?
   if (string2id.count(value))
      return string2id[value];

   // Already in db?
   unsigned id;
   if (differentialIndex.getDatabase().getDictionary().lookup(value,id))
      return id;

   // Create a temporary id
   id=(~0u)-id2string.size();
   string2id[value]=id;
   id2string.push_back(value);

   return id;
}
//---------------------------------------------------------------------------
void BulkOperation::insert(const std::string& subject,const std::string& predicate,const std::string& object)
   // Add a triple
{
   DifferentialIndex::Triple t;
   t.subject=mapString(subject);
   t.predicate=mapString(predicate);
   t.object=mapString(object);
   triples.push_back(t);
}
//---------------------------------------------------------------------------
void BulkOperation::commit()
   // Commit
{
   // Resolve all temporary ids
   vector<unsigned> realIds;
   differentialIndex.mapStrings(id2string,realIds);
   unsigned tempStart=(~0u)-realIds.size();
   for (vector<DifferentialIndex::Triple>::iterator iter=triples.begin(),limit=triples.end();iter!=limit;++iter) {
      if ((*iter).subject>tempStart) (*iter).subject=realIds[(~0u)-(*iter).subject];
      if ((*iter).predicate>tempStart) (*iter).predicate=realIds[(~0u)-(*iter).predicate];
      if ((*iter).object>tempStart) (*iter).object=realIds[(~0u)-(*iter).object];
   }
   realIds.clear();

   // Load the triples
   differentialIndex.load(triples);

   // And release
   id2string.clear();
   string2id.clear();
   triples.clear();
}
//---------------------------------------------------------------------------
void BulkOperation::abort()
   // Abort
{
   id2string.clear();
   string2id.clear();
   triples.clear();
}
//---------------------------------------------------------------------------
