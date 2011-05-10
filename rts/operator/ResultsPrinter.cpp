#include "rts/operator/ResultsPrinter.hpp"
#include "rts/operator/PlanPrinter.hpp"
#include "rts/database/Database.hpp"
#include "rts/runtime/DifferentialIndex.hpp"
#include "rts/runtime/Runtime.hpp"
#include "rts/runtime/TemporaryDictionary.hpp"
#include "rts/segment/DictionarySegment.hpp"
#include "infra/osdep/Timestamp.hpp"
#include <iostream>
#include <map>
#include <set>
#include <list>
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
ResultsPrinter::ResultsPrinter(Runtime& runtime,Operator* input,const CodeGen::Output& output,DuplicateHandling duplicateHandling,unsigned limit,bool silent)
   : Operator(1),output(output),input(input),runtime(runtime),dictionary(runtime.getDatabase().getDictionary()),duplicateHandling(duplicateHandling),outputMode(DefaultOutput),limit(limit),silent(silent)
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
   /// The type
   Type::ID type;
   /// The sub-type
   unsigned subType;

   /// Constructor
   CacheEntry() : start(0),stop(0) {}
   /// Print the raw value
   void printValue(bool escape) const;
   /// Print it
   void print(const map<unsigned,CacheEntry>& stringCache,bool escape) const;
};
//---------------------------------------------------------------------------
void CacheEntry::printValue(bool escape) const
   // Print the raw value
{
   if (escape) {
      for (const char* iter=start,*limit=stop;iter!=limit;++iter) {
         char c=*iter;
         if ((c==' ')||(c=='\n')||(c=='\\'))
            cout << '\\';
         cout << c;
      }
   } else {
      for (const char* iter=start,*limit=stop;iter!=limit;++iter)
         cout << *iter;
   }
}
//---------------------------------------------------------------------------
void CacheEntry::print(const map<unsigned,CacheEntry>& stringCache,bool escape) const
   // Print it
{
   switch (type) {
      case Type::URI: cout << '<'; printValue(escape); cout << '>'; break;
      case Type::Literal: cout << '"'; printValue(escape); cout << '"'; break;
      case Type::CustomLanguage: cout << '"'; printValue(escape); cout << "\"@"; (*stringCache.find(subType)).second.printValue(escape); break;
      case Type::CustomType: cout << '"'; printValue(escape); cout << "\"^^<"; (*stringCache.find(subType)).second.printValue(escape); cout << ">"; break;
      case Type::String: cout << '"'; printValue(escape); cout << "\"^^<http://www.w3.org/2001/XMLSchema#string>"; break;
      case Type::Integer: cout << '"'; printValue(escape); cout << "\"^^<http://www.w3.org/2001/XMLSchema#integer>"; break;
      case Type::Decimal: cout << '"'; printValue(escape); cout << "\"^^<http://www.w3.org/2001/XMLSchema#decimal>"; break;
      case Type::Double: cout << '"'; printValue(escape); cout << "\"^^<http://www.w3.org/2001/XMLSchema#double>"; break;
      case Type::Boolean: cout << '"'; printValue(escape); cout << "\"^^<http://www.w3.org/2001/XMLSchema#boolean>"; break;
   }
}
//---------------------------------------------------------------------------
template<class T> static void printResult(map<unsigned,CacheEntry>& stringCache,typename T::const_iterator start,typename T::const_iterator stop,bool escape)
   // Print a result row
{
   if (start==stop) return;
   if (!~(*start))
      cout << "NULL"; else
      stringCache[*start].print(stringCache,escape);
   for (++start;start!=stop;++start) {
      cout << ' ';
      if (!~(*start))
         cout << "NULL"; else
         stringCache[*start].print(stringCache,escape);
   }
}
//---------------------------------------------------------------------------
};
//---------------------------------------------------------------------------
unsigned ResultsPrinter::first()
   // Produce the first tuple
{
   observedOutputCardinality=1;
   // Empty input?
   unsigned count;
   if ((count=input->first())==0) {
      if ((!silent)&&(outputMode!=Embedded))
         cout << "<empty result>" << endl;
      return 1;
   }

   // Collect the values
   vector<unsigned> results;
   vector<list<unsigned> > pathresults;
   map<unsigned,CacheEntry> stringCache;
   unsigned minCount=(duplicateHandling==ShowDuplicates)?2:1;
   unsigned entryCount=0;
   Timestamp t1;
   do {
      if (count<minCount) continue;
      results.push_back(count);

	  for (vector<Register*>::const_iterator iter=output.valueoutput.begin(),limit=output.valueoutput.end();iter!=limit;++iter) {
         unsigned id=(*iter)->value;
         results.push_back(id);
         if (~id) stringCache[id];
      }

      for (vector<VectorRegister*>::const_iterator iter=output.pathoutput.begin(),limit=output.pathoutput.end();iter!=limit;++iter){
    	  list<unsigned>& path=(*iter)->value;
    	  pathresults.push_back(path);
    	  for (list<unsigned>::iterator itlist=path.begin();itlist!=path.end();itlist++)
    		  if (~(*itlist)) stringCache[*itlist];
      }
      if ((++entryCount)>=this->limit) break;
   } while ((count=input->next())!=0);

   Timestamp t2;
   cerr<<"time for computing: "<<t2-t1<<" ms"<<endl;
   cerr<<"string cache size: "<<stringCache.size()<<endl;

   // Lookup the strings
   set<unsigned> subTypes;
   TemporaryDictionary* tempDict=runtime.hasTemporaryDictionary()?(&runtime.getTemporaryDictionary()):0;
   DifferentialIndex* diffIndex=runtime.hasDifferentialIndex()?(&runtime.getDifferentialIndex()):0;
   for (map<unsigned,CacheEntry>::iterator iter=stringCache.begin(),limit=stringCache.end();iter!=limit;++iter) {
      CacheEntry& c=(*iter).second;
      if (tempDict)
         tempDict->lookupById((*iter).first,c.start,c.stop,c.type,c.subType); else
      if (diffIndex)
         diffIndex->lookupById((*iter).first,c.start,c.stop,c.type,c.subType); else
         dictionary.lookupById((*iter).first,c.start,c.stop,c.type,c.subType);
      if (Type::hasSubType(c.type))
         subTypes.insert(c.subType);
   }
   for (set<unsigned>::const_iterator iter=subTypes.begin(),limit=subTypes.end();iter!=limit;++iter) {
      CacheEntry& c=stringCache[*iter];
      if (tempDict)
         tempDict->lookupById(*iter,c.start,c.stop,c.type,c.subType); else
      if (diffIndex)
         diffIndex->lookupById(*iter,c.start,c.stop,c.type,c.subType); else
         dictionary.lookupById(*iter,c.start,c.stop,c.type,c.subType);
   }
   Timestamp t3;
   cerr<<"looking up results: "<<t3-t2<<" ms"<<endl;

   // Skip printing the results?
   if (silent)
      return 1;


   // Expand duplicates?
   unsigned columns=output.valueoutput.size();
   if (duplicateHandling==ExpandDuplicates) {
	  // output without paths
	  if (output.pathoutput.size() == 0){
		  for (vector<unsigned>::const_iterator iter=results.begin(),limit=results.end();iter!=limit;) {
			  unsigned count=*iter; ++iter;
			  for (unsigned index=0;index<count;index++) {
				  printResult<vector<unsigned> >(stringCache,iter,iter+columns,(outputMode==Embedded));
				  cout << endl;
			  }
			  iter+=columns;
		  }
	  }
	  // we need to combine paths and single values
	  else {
		  vector<unsigned>::const_iterator valueiter = results.begin();
		  vector<list<unsigned> >::const_iterator pathiter = pathresults.begin();
		  while (valueiter != results.end()){
			  unsigned i=0;
			  valueiter++;
			  while (i < output.order.size()){
				  unsigned count = i;
				  while (output.order[i]==0 && i<output.order.size()) i++;
				  if (count != i){
					  printResult<vector<unsigned> >(stringCache, valueiter, valueiter+(i-count),(outputMode==Embedded));
					  valueiter+=(i-count);
				  }
				  count = i;

				  while (output.order[i]==1 && i<output.order.size()) i++;

				  if (count != i){
					  for (unsigned j=0; j<i-count; j++){
						  cout<<" (";
						  printResult<list<unsigned> >(stringCache,pathiter->begin(),pathiter->end(),(outputMode==Embedded));
						  pathiter++;
						  cout<<") ";
					  }
				  }
			  }
			  //end of one tuple
			  cout<<endl;
		  }
	  }
   } else {
      // No, reduced, count, or duplicates
	  // output without paths
	  if (output.pathoutput.size() == 0){
		  for (vector<unsigned>::const_iterator iter=results.begin(),limit=results.end();iter!=limit;) {
			  unsigned count=*iter; ++iter;
			  printResult<vector<unsigned> >(stringCache,iter,iter+columns,(outputMode==Embedded));
			  if (duplicateHandling!=ReduceDuplicates)
				  cout << " " << count;
			  cout << endl;
			  iter+=columns;
		  }
	  }
	  else {

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
void ResultsPrinter::print(PlanPrinter& out)
   // Print the operator tree. Debugging only.
{

   out.beginOperator("ResultsPrinter",expectedOutputCardinality,observedOutputCardinality);
   out.addMaterializationAnnotation(output.valueoutput);
   out.addPathMaterializationAnnotation(output.pathoutput);

   input->print(out);
   out.endOperator();
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
