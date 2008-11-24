#include "rts/operator/ResultsPrinter.hpp"
#include "rts/database/Database.hpp"
#include "rts/runtime/Runtime.hpp"
#include "rts/segment/DictionarySegment.hpp"
#include <iostream>
#include <map>
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
using namespace std;
//---------------------------------------------------------------------------
ResultsPrinter::ResultsPrinter(Database& db,Operator* input,const vector<Register*>& output,DuplicateHandling duplicateHandling,unsigned limit,bool silent)
   : output(output),input(input),dictionary(db.getDictionary()),duplicateHandling(duplicateHandling),limit(limit),silent(silent)
   // Constructor
{
}
//---------------------------------------------------------------------------
ResultsPrinter::~ResultsPrinter()
   // Destructor
{
   delete input;
}
//---------------------------------------------------------------------------
namespace {
//---------------------------------------------------------------------------
/// A wrapper to avoid duplicating the strings in memory
struct CacheEntry {
   /// The string boundaries
   const char* start,*stop;

   /// Constructor
   CacheEntry() : start(0),stop(0) {}
   /// Print it
   void print() const;
};
//---------------------------------------------------------------------------
void CacheEntry::print() const
   // Print it
{
   for (const char* iter=start,*limit=stop;iter!=limit;++iter)
      cout << *iter;
}
//---------------------------------------------------------------------------
static void printResult(map<unsigned,CacheEntry>& stringCache,vector<unsigned>::const_iterator start,vector<unsigned>::const_iterator stop)
   // Print a result row
{
   if (start==stop) return;
   if (!~(*start))
      cout << "NULL"; else
      stringCache[*start].print();
   for (++start;start!=stop;++start) {
      cout << ' ';
      if (!~(*start))
         cout << "NULL"; else
         stringCache[*start].print();
   }
}
//---------------------------------------------------------------------------
};
//---------------------------------------------------------------------------
unsigned ResultsPrinter::first()
   // Produce the first tuple
{
   // Empty input?
   unsigned count;
   if ((count=input->first())==0) {
      if (!silent)
         cout << "<empty result>" << endl;
      return true;
   }

   // Collect the values
   vector<unsigned> results;
   map<unsigned,CacheEntry> stringCache;
   unsigned minCount=(duplicateHandling==ShowDuplicates)?2:1;
   unsigned entryCount=0;
   do {
      if (count<minCount) continue;
      results.push_back(count);
      for (vector<Register*>::const_iterator iter=output.begin(),limit=output.end();iter!=limit;++iter) {
         unsigned id=(*iter)->value;
         results.push_back(id);
         if (~id) stringCache[id];
      }
      if ((++entryCount)>=this->limit) break;
   } while ((count=input->next())!=0);

   // Lookup the strings
   for (map<unsigned,CacheEntry>::iterator iter=stringCache.begin(),limit=stringCache.end();iter!=limit;++iter) {
      CacheEntry& c=(*iter).second;
      dictionary.lookupById((*iter).first,c.start,c.stop);
   }

   // Skip printing the results?
   if (silent)
      return 1;

   // Expand duplicates?
   unsigned columns=output.size();
   if (duplicateHandling==ExpandDuplicates) {
      for (vector<unsigned>::const_iterator iter=results.begin(),limit=results.end();iter!=limit;) {
         unsigned count=*iter; ++iter;
         for (unsigned index=0;index<count;index++) {
            printResult(stringCache,iter,iter+columns);
            cout << endl;
         }
         iter+=columns;
      }
   } else {
      // No, reduced, count, or duplicates
      for (vector<unsigned>::const_iterator iter=results.begin(),limit=results.end();iter!=limit;) {
         unsigned count=*iter; ++iter;
         printResult(stringCache,iter,iter+columns);
         if (duplicateHandling!=ReduceDuplicates)
            cout << " " << count;
         cout << endl;
         iter+=columns;
      }
   }

   return 1;
}
//---------------------------------------------------------------------------
unsigned ResultsPrinter::next()
   // Produce the next tuple
{
   return false;
}
//---------------------------------------------------------------------------
void ResultsPrinter::print(DictionarySegment& dict,unsigned level)
   // Print the operator tree. Debugging only.
{
   indent(level); cout << "<ResultsPrinter";
   for (vector<Register*>::const_iterator iter=output.begin(),limit=output.end();iter!=limit;++iter) {
      cout << " "; printRegister(dict,*iter);
   }
   cout << endl;
   input->print(dict,level+1);
   indent(level); cout << ">" << endl;
}
//---------------------------------------------------------------------------
void ResultsPrinter::addMergeHint(Register* /*reg1*/,Register* /*reg2*/)
   // Add a merge join hint
{
   // Do not propagate as we break the pipeline
}
//---------------------------------------------------------------------------
void ResultsPrinter::getAsyncInputCandidates(Scheduler& scheduler)
   // Register parts of the tree that can be executed asynchronous
{
   input->getAsyncInputCandidates(scheduler);
}
//---------------------------------------------------------------------------
