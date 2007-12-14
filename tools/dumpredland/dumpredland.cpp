#include "infra/util/Hash.hpp"
#include <fstream>
#include <iostream>
#include <map>
#include <string>
#include <vector>
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
static bool parseEntry(string::const_iterator& iter,string::const_iterator limit,string& result,unsigned remaining)
   // Parse an entry
{
   result="";

   // Skip comma
   if ((iter!=limit)&&((*iter)==',')) {
      ++iter;
      while ((iter!=limit)&&((*iter)==' '))
        ++iter;
   }

   // Empty?
   if (iter==limit)
      return !remaining;
   if (!remaining)
      return false;

   // Handle the different separators
   if ((*iter)=='[') {
      ++iter;
      while (iter!=limit) {
         if ((*iter)==']') {
            string::const_iterator iter2=iter+1;
            string dummy;
            if (parseEntry(iter2,limit,dummy,remaining-1))
               break;
         }
         result+=*(iter++);
      }
      if (iter!=limit)
         ++iter;
      return true;
   } else if ((*iter)=='\"') {
      ++iter;
      while (iter!=limit) {
         if ((*iter)=='\"') {
            string::const_iterator iter2=iter+1;
            string dummy;
            if (parseEntry(iter2,limit,dummy,remaining-1))
               break;
         }
         result+=*(iter++);
      }
      if (iter!=limit)
         ++iter;
      return true;
   } else if ((*iter)=='(') {
      ++iter;
      while ((iter!=limit)&&((*iter)!=')'))
         result+=*(iter++);
      if (iter!=limit)
         ++iter;
      return true;
   } else return false;
}
//---------------------------------------------------------------------------
static bool collectStrings(ofstream& out,map<string,unsigned>& stringMap,map<unsigned,unsigned>& hashMap,const string& fileName,bool firstPass)
   // Collect all strings
{
   ifstream in(fileName.c_str());
   if (!in.is_open()) {
      cout << "warning: unable to open " << fileName << endl;
      return false;
   }

   string s;
   unsigned line=0;
   unsigned nextId=stringMap.size();
   static const unsigned recentlySeenSize = 1<<17;
   vector<string> recentlySeen;
   if (firstPass)
      recentlySeen.resize(recentlySeenSize);
   while (true) {
      getline(in,s);
      if (!in.good()) break;

      // Progress indicator
      if ((++line)>=100000) {
         cout << ".";
         cout.flush();
         line=0;
      }

      // Ignore block delimiters
      if ((s=="[[")||(s=="]]")) continue;

      // Find the boundaries
      string::const_iterator iter=s.begin(),limit=s.end();
      while ((iter!=limit)&&((*iter)!='{')) iter++;
      if (iter!=limit) ++iter;
      if (iter!=limit) --limit;
      while ((iter!=limit)&&((*limit)!='}')) limit--;
      if (iter==limit) {
         cout << "warning: invalid line '" << s << "' in " << fileName << endl;
         continue;
      }

      // Parse the entries
      string subject,predicate,object;
      bool ok=true;
      if (!parseEntry(iter,limit,subject,3)) ok=false;
      if (!parseEntry(iter,limit,predicate,2)) ok=false;
      if (!parseEntry(iter,limit,object,1)) ok=false;
      if (!ok) {
         cout << "warning: invalid entries in line '" << s << "' in " << fileName << endl;
         continue;
      }

      // And write
      if (firstPass) {
         if (!stringMap.count(predicate)) {
            out << nextId << " " << predicate << endl;
            stringMap[predicate]=nextId++;
         }
         for (unsigned index=0;index<2;index++) {
            const string& s=index?object:subject;
            unsigned hash=Hash::hash(s);
            unsigned slot=hash%recentlySeenSize;
            if (hashMap.count(hash)) {
               if (recentlySeen[slot]!=s)
                  hashMap[hash]=0;
            } else {
               hashMap[hash]=1;
            }
            recentlySeen[slot]=s;
         }
      } else {
         for (unsigned index=0;index<2;index++) {
            const string& s=index?object:subject;
            unsigned hash=Hash::hash(s);
            if (hashMap.count(hash)) {
               if ((hashMap[hash]==0)&&(!stringMap.count(s))) {
                  out << nextId << " " << s << endl;
                  stringMap[s]=nextId++;
               }
            } else {
               out << nextId << " " << s << endl;
               hashMap[hash]=nextId++;
            }
         }
      }
   }
   cout << endl;

   // Remember the collisisons
   if (firstPass) {
      vector<unsigned> collisions;
      for (map<unsigned,unsigned>::const_iterator iter=hashMap.begin(),limit=hashMap.end();iter!=limit;++iter)
         if (!(*iter).second)
            collisions.push_back((*iter).first);
      hashMap.clear();
      for (vector<unsigned>::const_iterator iter=collisions.begin(),limit=collisions.end();iter!=limit;++iter)
         hashMap[*iter]=0;
   }

   return true;
}
//---------------------------------------------------------------------------
static unsigned mapString(map<string,unsigned>& stringMap,map<unsigned,unsigned>& hashMap,const string& s)
   // Map a string to an id
{
   if (stringMap.count(s)) {
      return stringMap[s];
   } else {
      unsigned hash=Hash::hash(s);
      if (hashMap.count(hash))
         return hashMap[hash];
      cout << "Bug! '" << s << "' not found in string table!" << endl;
      return 0;
   }
}
//---------------------------------------------------------------------------
static bool dumpFile(ofstream& out,map<string,unsigned>& stringMap,map<unsigned,unsigned>& hashMap,const string& fileName)
   // Dump the file into the facts table
{
   ifstream in(fileName.c_str());
   if (!in.is_open()) {
      cout << "warning: unable to open " << fileName << endl;
      return false;
   }

   string s;
   unsigned line=0;
   while (true) {
      getline(in,s);
      if (!in.good()) break;

      // Progress indicator
      if ((++line)>=100000) {
         cout << ".";
         cout.flush();
         line=0;
      }

      // Ignore block delimiters
      if ((s=="[[")||(s=="]]")) continue;

      // Find the boundaries
      string::const_iterator iter=s.begin(),limit=s.end();
      while ((iter!=limit)&&((*iter)!='{')) iter++;
      if (iter!=limit) ++iter;
      if (iter!=limit) --limit;
      while ((iter!=limit)&&((*limit)!='}')) limit--;
      if (iter==limit) {
         cout << "warning: invalid line '" << s << "' in " << fileName << endl;
         continue;
      }

      // Parse the entries
      string subject,predicate,object;
      bool ok=true;
      if (!parseEntry(iter,limit,subject,3)) ok=false;
      if (!parseEntry(iter,limit,predicate,2)) ok=false;
      if (!parseEntry(iter,limit,object,1)) ok=false;
      if (!ok) {
         cout << "warning: invalid entries in line '" << s << "' in " << fileName << endl;
         continue;
      }

      // And write
      out << mapString(stringMap,hashMap,subject) << "\t" << mapString(stringMap,hashMap,predicate) << "\t" << mapString(stringMap,hashMap,object) << endl;
   }
   cout << endl;

   return true;
}
//---------------------------------------------------------------------------
int main(int argc,char* argv[])
{
   if (argc!=2) {
      cout << "usage: " << argv[0] << " [redland-dump]" << endl;
      return 1;
   }

   // Collect the predicates and build the string map
   map<string,unsigned> stringMap;
   map<unsigned,unsigned> hashMap;
   {
      ofstream out("strings");
      cout << "Collecting predicates..." << endl;
      if (!collectStrings(out,stringMap,hashMap,argv[1],true))
         return 1;
      cout << "Building the string map..." << endl;
      if (!collectStrings(out,stringMap,hashMap,argv[1],false))
         return 1;
   }

   // Create the facts table
   {
      ofstream out("facts");
      cout << "Dumping facts..." << endl;
      if (!dumpFile(out,stringMap,hashMap,argv[1]))
         return 1;
   }
}
//---------------------------------------------------------------------------
