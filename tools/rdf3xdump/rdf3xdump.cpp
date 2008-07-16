#include "rts/database/Database.hpp"
#include "rts/operator/IndexScan.hpp"
#include "rts/runtime/Runtime.hpp"
#include "rts/segment/DictionarySegment.hpp"
#include <iostream>
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
static void dumpSubject(DictionarySegment& dic,unsigned id)
   // Write a subject entry
{
   const char* start,*stop;
   if (!dic.lookupById(id,start,stop)) {
      cerr << "consistency error: encountered unknown id " << id << endl;
      throw;
   }
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
static void dumpPredicate(DictionarySegment& dic,unsigned id)
   // Write a predicate entry
{
   const char* start,*stop;
   if (!dic.lookupById(id,start,stop)) {
      cerr << "consistency error: encountered unknown id " << id << endl;
      throw;
   }
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
static void dumpObject(DictionarySegment& dic,unsigned id)
   // Write an object entry
{
   const char* start,*stop;
   if (!dic.lookupById(id,start,stop)) {
      cerr << "consistency error: encountered unknown id " << id << endl;
      throw;
   }
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
int main(int argc,char* argv[])
{
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

   // Dump the database
   DictionarySegment& dic=db.getDictionary();
   Register subject,predicate,object;
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
