#include "rts/segment/PredicateSetSegment.hpp"
#include "rts/database/DatabasePartition.hpp"
#include "rts/segment/AggregatedFactsSegment.hpp"
#include <algorithm>
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

   /// Check for subset relationship of the contained predicates
   bool subsetOf(const PredSet& other) const;
   /// Transfer predicates to a subset
   void transferTo(PredSet& target);
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
bool PredicateSetSegment::PredSet::subsetOf(const PredSet& other) const
   // Check for subset relationship of the contained predicates
{
   if (predicates.size()>other.predicates.size()) return false;

   vector<Entry>::const_iterator iter2=other.predicates.begin(),limit2=other.predicates.end();
   for (vector<Entry>::const_iterator iter=predicates.begin(),limit=predicates.end();iter!=limit;++iter) {
      // Examine the next value
      if (iter2==limit2) return false;
      unsigned v=(*iter).predicate;

      // Fits directly?
      if (v==((*iter2).predicate)) {
         ++iter2;
         continue;
      }

      // Perform binary search
      vector<Entry>::const_iterator left=iter2+1,right=limit2;
      while (left!=right) {
         vector<Entry>::const_iterator middle=left+((right-left)/2);
         unsigned v2=(*middle).predicate;
         if (v<v2) {
            right=middle;
         } else if (v>v2) {
            left=middle+1;
         } else {
            left=middle;
            break;
         }
      }
      if ((left==limit2)||((*left).predicate!=v))
         return false;
      iter2=left+1;
   }
   return true;
}
//---------------------------------------------------------------------------
void PredicateSetSegment::PredSet::transferTo(PredSet& target)
   // Transfer predicates to a subset
{
   target.subjects+=subjects;
   vector<Entry>::iterator writer=target.predicates.begin(),writerLimit=target.predicates.end();
   if (writer==writerLimit) return;
   for (unsigned index=0,limit=predicates.size();index<limit;++index) {
      if (predicates[index].predicate==(*writer).predicate) {
         (*writer).count+=predicates[index].count;
         predicates.erase(predicates.begin()+index);
         --index; --limit;
         if ((++writer)==writerLimit)
            break;
      }
   }
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
#if 0
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
#endif
//---------------------------------------------------------------------------
namespace {
//---------------------------------------------------------------------------
struct OrderBySubjects { bool operator()(const PredicateSetSegment::PredSet* a,const PredicateSetSegment::PredSet* b) { return a->subjects>b->subjects; } };
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
void PredicateSetSegment::computePredicateSets()
   // Compute the predicate sets (after loading)
{
   // Collect all predicate sets
   set<PredSet> predSets;
#if 0
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
      ofstream out("bin/predsets.dump");
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
   {
      ifstream in("bin/predsets.dump");
      unsigned size = 0;
      in >> size;
      for (unsigned index=0;index<size;index++) {
         PredSet p; unsigned count;
         in >> p.subjects >> count;
         p.predicates.resize(count);
         for (unsigned index2=0;index2<count;index2++)
            in >> p.predicates[index2].predicate >> p.predicates[index2].count;
         predSets.insert(p);
      }
   }
#endif
   cout << "Found " << predSets.size() << " predicate sets" << endl;

   // Simplify if needed
   static const unsigned maxSize = 10000;
   if (predSets.size()>maxSize) {
      // Sort all sets
      vector<PredSet*> sets;
      sets.reserve(predSets.size());
      for (set<PredSet>::const_iterator iter=predSets.begin(),limit=predSets.end();iter!=limit;++iter)
         sets.push_back(const_cast<PredSet*>(&(*iter)));
      sort(sets.begin(),sets.end(),OrderBySubjects());

      // And merge the small ones
      for (unsigned index=maxSize,limit=sets.size();index<limit;++index) {
         PredSet remaining=*sets[index];
         while (!remaining.predicates.empty()) {
            // Find the largest subset
            PredSet* bestMatch=0;
            for (vector<PredSet*>::const_iterator iter=sets.begin(),limit=iter+maxSize;iter!=limit;++iter)
               if (((*iter)->predicates.size()<remaining.predicates.size())&&
                   ((!bestMatch)||((*iter)->predicates.size()>bestMatch->predicates.size()))&&
                   ((*iter)->subsetOf(remaining)))
                  bestMatch=*iter;

            // None found?
            if (!bestMatch) break;

            // Transfer
            remaining.transferTo(*bestMatch);
         }
      }

      // Keep only the common pred sets
      set<PredSet> newPredSets;
      for (vector<PredSet*>::const_iterator iter=sets.begin(),limit=iter+maxSize;iter!=limit;++iter)
         newPredSets.insert(**iter);
      predSets.swap(newPredSets);
   }

   {
      ofstream out("bin/predsets2.dump");
      out << predSets.size() << endl;
      for (set<PredSet>::const_iterator iter=predSets.begin(),limit=predSets.end();iter!=limit;++iter) {
         out << (*iter).subjects << " " << (*iter).predicates.size();
         for (vector<PredSet::Entry>::const_iterator iter2=(*iter).predicates.begin(),limit2=(*iter).predicates.end();iter2!=limit2;++iter2) {
            out << " " << (*iter2).predicate << " " << (*iter2).count;
         }
         out << endl;
      }
   }
}
//---------------------------------------------------------------------------
