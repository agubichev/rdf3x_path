#include "rts/segment/PredicateSetSegment.hpp"
#include "rts/database/DatabasePartition.hpp"
#include "rts/segment/AggregatedFactsSegment.hpp"
#include <set>
#include <iostream>
#include <fstream>
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
/// A predicate set
struct PredicateSetSegment::PredSet
{
   /// A set entry
   struct Entry {
      /// The predicate
      unsigned predicate;
      /// The number of occurances
      unsigned count;
   };

   /// The number of distinct subjects with this predicate combination
   unsigned subjects;
   /// The predicates (sorted by predicate)
   vector<Entry> predicates;

   /// Compare by occuring predicates
   bool operator<(const PredSet& other) const;
};
//---------------------------------------------------------------------------
bool PredicateSetSegment::PredSet::operator<(const PredSet& other) const
   // Compare by occuring predicates
{
   if (predicates.size()<other.predicates.size()) return true;
   if (predicates.size()>other.predicates.size()) return false;

   for (vector<Entry>::const_iterator iter=predicates.begin(),limit=predicates.end(),iter2=other.predicates.begin();iter!=limit;++iter,++iter2) {
      unsigned p1=(*iter).predicate,p2=(*iter2).predicate;
      if (p1<p2) return true;
      if (p1>p2) return false;
   }
   return false;
}
//---------------------------------------------------------------------------
/// The data
struct PredicateSetSegment::Data
{
   /// The sets
   vector<PredSet> predSets;
};
//---------------------------------------------------------------------------
PredicateSetSegment::PredicateSetSegment(DatabasePartition& partition)
   : Segment(partition),data(new Data())
   // Constructor
{
}
//---------------------------------------------------------------------------
PredicateSetSegment::~PredicateSetSegment()
   // Destructor
{
   delete data;
}
//---------------------------------------------------------------------------
Segment::Type PredicateSetSegment::getType() const
   // Get the type
{
   return Segment::Type_PredicateSet;
}
//---------------------------------------------------------------------------
void PredicateSetSegment::refreshInfo()
   // Refresh segment info stored in the partition
{
}
//---------------------------------------------------------------------------
static void addPredSet(set<PredicateSetSegment::PredSet>& predSets,PredicateSetSegment::PredSet& predSet)
   // Add a subject to the predicate set
{
   set<PredicateSetSegment::PredSet>::iterator pos=predSets.find(predSet);
   if (pos!=predSets.end()) {
      PredicateSetSegment::PredSet& p=const_cast<PredicateSetSegment::PredSet&>(*pos);
      p.subjects+=predSet.subjects;
      for (vector<PredicateSetSegment::PredSet::Entry>::iterator iter=p.predicates.begin(),limit=p.predicates.end(),iter2=predSet.predicates.begin();iter!=limit;++iter,++iter2)
         (*iter).count+=(*iter2).count;
   } else {
      predSets.insert(predSet);
   }
}
//---------------------------------------------------------------------------
void PredicateSetSegment::computePredicateSets()
   // Compute the predicate sets (after loading)
{
   // Collect all predicate sets
   set<PredSet> predSets;
#if 1
   {
      AggregatedFactsSegment::Scan scan;
      if (scan.first(*getPartition().lookupSegment<AggregatedFactsSegment>(DatabasePartition::Tag_SP))) {
         PredSet predSet; predSet.subjects=1;
         unsigned current=~0u;
         do {
            // Did the subject change? Then start a new set
            if (scan.getValue1()!=current) {
               if (!predSet.predicates.empty())
                  addPredSet(predSets,predSet);
               predSet.predicates.clear();
               current=scan.getValue1();
            }
            // Remember the predicate
            PredSet::Entry e; e.predicate=scan.getValue2(); e.count=scan.getCount();
            predSet.predicates.push_back(e);
         } while (scan.next());
         // Store the last set
         if (!predSet.predicates.empty())
            addPredSet(predSets,predSet);
      }
   }
   {
      ofstream out("predsets.dump");
      out << predSets.size() << endl;
      for (set<PredSet>::const_iterator iter=predSets.begin(),limit=predSets.end();iter!=limit;++iter) {
         out << (*iter).subjects << " " << (*iter).predicates.size();
         for (vector<PredSet::Entry>::const_iterator iter2=(*iter).predicates.begin(),limit2=(*iter).predicates.end();iter2!=limit2;++iter2) {
            out << " " << (*iter2).predicate << " " << (*iter2).count;
         }
         out << endl;
      }
   }
#else
#endif
   cout << "Found " << predSets.size() << " predicate sets" << endl;
}
//---------------------------------------------------------------------------
