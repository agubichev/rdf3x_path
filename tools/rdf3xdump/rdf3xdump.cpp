#include "rts/database/Database.hpp"
#include "rts/operator/IndexScan.hpp"
#include "rts/runtime/Runtime.hpp"
#include "rts/segment/DictionarySegment.hpp"
#include <iostream>
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
static void writeURI(const char* start,const char* stop)
   // Write a URI
{
   cout << "<";
   for (const char* iter=start;iter<stop;++iter) {
      char c=*iter;
      switch (c) {
         case '\t': cout << "\\t"; break;
         case '\n': cout << "\\n"; break;
         case '\r': cout << "\\r"; break;
         case '>': cout << "\\>"; break;
         case '\\': cout << "\\\\"; break;
         default: cout << c; break;
      }
   }
   cout << ">";
}
//---------------------------------------------------------------------------
static void writeLiteral(const char* start,const char* stop)
   // Write a literal
{
   cout << "\"";
   for (const char* iter=start;iter<stop;++iter) {
      char c=*iter;
      switch (c) {
         case '\t': cout << "\\t"; break;
         case '\n': cout << "\\n"; break;
         case '\r': cout << "\\r"; break;
         case '\"': cout << "\\\""; break;
         case '\\': cout << "\\\\"; break;
         default: cout << c; break;
      }
   }
   cout << "\"";
}
//---------------------------------------------------------------------------
static void dumpSubject(DictionarySegment& dic,unsigned id)
   // Write a subject entry
{
   const char* start,*stop;
   if (!dic.lookupById(id,start,stop)) {
      cerr << "consistency error: encountered unknown id " << id << endl;
      throw;
   }
   writeURI(start,stop);
}
//---------------------------------------------------------------------------
static void dumpPredicate(DictionarySegment& dic,unsigned id)
   // Write a predicate entry
{
   const char* start,*stop;
   if (!dic.lookupById(id,start,stop)) {
      cerr << "consistency error: encountered unknown id " << id << endl;
      throw;
   }
   writeURI(start,stop);
}
//---------------------------------------------------------------------------
static bool isBlankNode(const char* start,const char* stop)
   // Looks like a blank node? XXX store in dictionary
{
   return (start+2>stop)&&(start[0]=='_')&&(start[1]==':');
}
//---------------------------------------------------------------------------
static bool isURI(const char* start,const char* stop)
   // Looks like a URI? XXX store in dictionary
{
   const char* limit=stop-5;
   if (limit>start+10) limit=start+10;
   for (;start<limit;++start) {
      char c=*start;
      if (c==' ') break;
      if (c==':')
         return (start[1]=='/')&&(start[2]=='/');
   }
   return false;
}
//---------------------------------------------------------------------------
static void dumpObject(DictionarySegment& dic,unsigned id)
   // Write an object entry
{
   const char* start,*stop;
   if (!dic.lookupById(id,start,stop)) {
      cerr << "consistency error: encountered unknown id " << id << endl;
      throw;
   }
   // Blank node or URI?
   if (isBlankNode(start,stop)||isURI(start,stop)) {
      writeURI(start,stop);
      return;
   }
   // No, a literal value
   writeLiteral(start,stop);
}
//---------------------------------------------------------------------------
int main(int argc,char* argv[])
{
   bool rawDump=false;
   if ((argc==3)&&(string(argv[2])=="--raw"))
     rawDump=true;

   // Greeting
   if (!rawDump)
   cerr << "RDF-3X turtle exporter" << endl
        << "(c) 2008 Thomas Neumann. Web site: http://www.mpi-inf.mpg.de/~neumann/rdf3x" << endl;

   // Check the arguments
   if (argc<2) {
      cerr << "usage: " << argv[0] << " <database>" << endl;
      return 1;
   }

   // Open the database
   Database db;
   if (!db.open(argv[1])) {
      cerr << "unable to open database " << argv[1] << endl;
      return 1;
   }

   // Raw dump?
   if (rawDump) {
      // Dump the facts
      unsigned maxId=0;
      {
         FactsSegment::Scan scan;
         if (scan.first(db.getFacts(Database::Order_Subject_Predicate_Object))) do {
            cout << scan.getValue1() << " " << scan.getValue2() << " " << scan.getValue3() << std::endl;
            maxId=max(maxId,scan.getValue1());
            maxId=max(maxId,scan.getValue2());
            maxId=max(maxId,scan.getValue3());
         } while (scan.next());
      }
      // Dump the strings
      {
         const char* start,*stop;
         DictionarySegment& dic=db.getDictionary();
         for (unsigned id=0;(id<=maxId)&&dic.lookupById(id,start,stop);++id) {
            cerr << id << " ";
	    for (const char* iter=start;iter!=stop;++iter) {
	       char c=*iter;
	       if (c<=0) {
	          unsigned v=(c&0xFF);
		  const char hex[]="0123456789ABCDEF";
		  cerr << "\\x" << hex[v>>4] << hex[v&0xF];
	       } else switch (c) {
	          case '\\': cerr << "\\\\"; break;
		  case '\n': cerr << "\\n"; break;
		  case '\r': cerr << "\\r"; break;
	          default: cerr << c; break;
	       }
	    }
	    cerr << endl;
         }
      }
      return 0;
   }

   // Dump the database
   DictionarySegment& dic=db.getDictionary();
   Register subject,predicate,object;
   subject.reset(); predicate.reset(); object.reset();
   IndexScan* scan=IndexScan::create(db,Database::Order_Subject_Predicate_Object,&subject,false,&predicate,false,&object,false);
   if (scan->first()) {
      // Write the first triple
      dumpSubject(dic,subject.value);
      cout << " ";
      dumpPredicate(dic,predicate.value);
      cout << " ";
      dumpObject(dic,object.value);
      unsigned lastSubject=subject.value,lastPredicate=predicate.value,lastObject=object.value;

      // And all others
      while (scan->next()) {
         if (subject.value==lastSubject) {
            if (predicate.value==lastPredicate) {
               cout << " , ";
               dumpObject(dic,object.value);
            } else {
               cout << ";" << endl << "  ";
               dumpPredicate(dic,predicate.value);
               cout << " ";
               dumpObject(dic,object.value);
            }
         } else {
            cout << "." << endl;
            dumpSubject(dic,subject.value);
            cout << " ";
            dumpPredicate(dic,predicate.value);
            cout << " ";
            dumpObject(dic,object.value);
         }
         lastSubject=subject.value; lastPredicate=predicate.value; lastObject=object.value;
      }
      // Termination
      cout << "." << endl;
   }
   delete scan;
}
//---------------------------------------------------------------------------
