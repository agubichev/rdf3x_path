#ifndef H_rts_runtime_BulkOperation
#define H_rts_runtime_BulkOperation
//---------------------------------------------------------------------------
#include "rts/runtime/DifferentialIndex.hpp"
#include <map>
#include <string>
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
/// A bulk operation/transaction
class BulkOperation
{
   private:
   /// The differential index
   DifferentialIndex& differentialIndex;
   /// The triples
   std::vector<DifferentialIndex::Triple> triples;
   /// The temporary dictionary
   std::map<std::string,unsigned> string2id;
   /// The temporary dictionary
   std::vector<std::string> id2string;

   /// Map a string
   unsigned mapString(const std::string& value);

   public:
   /// Constructor
   explicit BulkOperation(DifferentialIndex& differentialIndex);
   /// Destructor
   ~BulkOperation();

   /// Add a triple
   void insert(const std::string& subject,const std::string& predicate,const std::string& object);

   /// Commit
   void commit();
   /// Abort
   void abort();
};
//---------------------------------------------------------------------------
#endif
