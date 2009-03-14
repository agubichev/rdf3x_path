#include "rts/runtime/PredicateLockManager.hpp"
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
PredicateLockManager::Box::Box(unsigned subjectMin,unsigned subjectMax,unsigned predicateMin,unsigned predicateMax,unsigned objectMin,unsigned objectMax)
   : subjectMin(subjectMin),subjectMax(subjectMax),predicateMin(predicateMin),predicateMax(predicateMax),objectMin(objectMin),objectMax(objectMax)
   // Constructor
{
}
//---------------------------------------------------------------------------
bool PredicateLockManager::Box::intersects(const Box& box) const
   // Intersects?
{
   return (subjectMin<=box.subjectMax)&&(subjectMax>=box.subjectMin)&&
          (predicateMin<=box.predicateMax)&&(predicateMax>=box.objectMin)&&
          (objectMin<=box.objectMax)&&(objectMax>=box.objectMin);
}
//---------------------------------------------------------------------------
PredicateLockManager::PredicateLockManager()
   // Constructor
{
}
//---------------------------------------------------------------------------
PredicateLockManager::~PredicateLockManager()
   // Destructor
{
}
//---------------------------------------------------------------------------
bool PredicateLockManager::lock(unsigned transaction,const Box& box,bool exclusive)
   // Lock a region
{
   // Check for conflicting locks
   auto_lock lock(mutex);
   for (vector<Lock>::const_iterator iter=locks.begin(),limit=locks.end();iter!=limit;++iter)
      if ((*iter).box.intersects(box)) {
         // Same transaction?
         if ((*iter).transaction==transaction)
            continue;
         // Compatible?
         if ((!exclusive)&&(!(*iter).exclusive))
            continue;
         // Already committed?
         if (committed.count((*iter).transaction))
            continue;
         // No, fail. XXX implement waiting
         return false;
      }

   // Create a new lock
   locks.push_back(Lock(box,transaction,exclusive));

   // Remember the transaction
   active.insert(transaction);

   return true;
}
//---------------------------------------------------------------------------
void PredicateLockManager::finished(unsigned transaction)
   // A transaction finished
{
   auto_lock lock(mutex);

   // Oldest active transaction?
   if (transaction==(*active.begin())) {
      active.erase(transaction);
      while (!committed.empty()) {
         unsigned first=*committed.begin();
         if (first>transaction) break;
         committed.erase(first);
      }
      for (vector<Lock>::iterator iter=locks.begin(),limit=locks.end();iter!=limit;)
         if ((*iter).transaction<=transaction) {
            iter=locks.erase(iter);
            limit=locks.end();
         } else {
            ++iter;
         }
   } else {
      // No, just remember it
      committed.insert(transaction);
   }
}
//---------------------------------------------------------------------------
