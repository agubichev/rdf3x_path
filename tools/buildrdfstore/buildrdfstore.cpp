#include "rts/database/DatabaseBuilder.hpp"
#include <fstream>
#include <iostream>
#include <vector>
#include <algorithm>
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
namespace {
//---------------------------------------------------------------------------
/// A RDF triple
struct Triple {
   /// The values as IDs
   unsigned subject,predicate,object;
};
//---------------------------------------------------------------------------
/// Order a RDF triple lexicographically
struct OrderTriple {
   bool operator()(const Triple& a,const Triple& b) const {
      return (a.subject<b.subject)||
             ((a.subject==b.subject)&&((a.predicate<b.predicate)||
             ((a.predicate==b.predicate)&&(a.object<b.object))));
   }
};
//---------------------------------------------------------------------------
bool readFacts(vector<Triple>& facts,const char* fileName)
   // Read the facts table
{
   ifstream in(fileName);
   if (!in.is_open()) {
      cout << "unable to open " << fileName << endl;
      return false;
   }

   facts.clear();
   while (true) {
      Triple t;
      in >> t.subject >> t.predicate >> t.object;
      if (!in.good()) break;
      facts.push_back(t);
   }

   return true;
}
//---------------------------------------------------------------------------
/// Loader for facts
class FactsLoader : public DatabaseBuilder::FactsReader
{
   private:
   /// The facts
   vector<Triple>::const_iterator iter,limit,start;

   public:
   /// Constructor
   FactsLoader(const vector<Triple>& facts) : iter(facts.begin()),limit(facts.end()),start(iter) {}

   /// Get the next fact
   bool next(unsigned& subject,unsigned& predicate,unsigned& object);
   /// Reset the reader
   void reset();
};
//---------------------------------------------------------------------------
bool FactsLoader::next(unsigned& subject,unsigned& predicate,unsigned& object)
   // Get the next fact
{
   if (iter==limit)
      return false;
   subject=(*iter).subject;
   predicate=(*iter).predicate;
   object=(*iter).object;
   ++iter;
   return true;
}
//---------------------------------------------------------------------------
void FactsLoader::reset()
   // Reset the reader
{
   iter=start;
}
//---------------------------------------------------------------------------
void dumpFacts(DatabaseBuilder& builder,vector<Triple>& facts)
   // Dump all 6 orderings into the database
{
   // Produce the different orderings
   for (unsigned index=0;index<6;index++) {
      cout << "Dumping ordering " << (index+1) << endl;

      // Change the values to fit the desired order
      switch (index) {
         case 0: // subject,predicate,object
            break;
         case 1: // subject,object,predicate
            for (vector<Triple>::iterator iter=facts.begin(),limit=facts.end();iter!=limit;++iter)
               std::swap((*iter).object,(*iter).predicate);
            break;
         case 2: // object,predicate,subject
            for (vector<Triple>::iterator iter=facts.begin(),limit=facts.end();iter!=limit;++iter)
               std::swap((*iter).object,(*iter).subject);
            break;
         case 3: // object,subject,predicate
            for (vector<Triple>::iterator iter=facts.begin(),limit=facts.end();iter!=limit;++iter) {
               std::swap((*iter).object,(*iter).subject);
               std::swap((*iter).object,(*iter).predicate);
            }
            break;
         case 4: // predicate,subject,object
            for (vector<Triple>::iterator iter=facts.begin(),limit=facts.end();iter!=limit;++iter)
               std::swap((*iter).subject,(*iter).predicate);
            break;
         case 5: // predicate,object,subject
            for (vector<Triple>::iterator iter=facts.begin(),limit=facts.end();iter!=limit;++iter) {
               std::swap((*iter).subject,(*iter).predicate);
               std::swap((*iter).object,(*iter).predicate);
            }
            break;
      }

      // Sort the facts accordingly
      sort(facts.begin(),facts.end(),OrderTriple());

      // And load them
      FactsLoader loader(facts);
      builder.loadFacts(index,loader);

      // Change the values back to the original order
      switch (index) {
         case 0: // subject,predicate,object
            break;
         case 1: // subject,object,predicate
            for (vector<Triple>::iterator iter=facts.begin(),limit=facts.end();iter!=limit;++iter)
               std::swap((*iter).object,(*iter).predicate);
            break;
         case 2: // object,predicate,subject
            for (vector<Triple>::iterator iter=facts.begin(),limit=facts.end();iter!=limit;++iter)
               std::swap((*iter).object,(*iter).subject);
            break;
         case 3: // object,subject,predicate
            for (vector<Triple>::iterator iter=facts.begin(),limit=facts.end();iter!=limit;++iter) {
               std::swap((*iter).object,(*iter).predicate);
               std::swap((*iter).object,(*iter).subject);
            }
            break;
         case 4: // predicate,subject,object
            for (vector<Triple>::iterator iter=facts.begin(),limit=facts.end();iter!=limit;++iter)
               std::swap((*iter).subject,(*iter).predicate);
            break;
         case 5: // predicate,object,subject
            for (vector<Triple>::iterator iter=facts.begin(),limit=facts.end();iter!=limit;++iter) {
               std::swap((*iter).object,(*iter).predicate);
               std::swap((*iter).subject,(*iter).predicate);
            }
            break;
      }
   }
}
//---------------------------------------------------------------------------
/// A string description
struct StringEntry {
   /// The page the string is put on
   unsigned page;
   /// Offset and len
   unsigned ofsLen;
   /// The hash value of the string
   unsigned hash;
};
//---------------------------------------------------------------------------
/// Order a string entry by hash
struct OrderStringByHash {
   bool operator()(const StringEntry& a,const StringEntry& b) const { return a.hash<b.hash; }
};
//---------------------------------------------------------------------------
/// Read strings from a file
class StringReader : public DatabaseBuilder::StringsReader {
   private:
   /// The input
   ifstream& in;
   /// The remebered data
   vector<StringEntry>& strings;
   /// The next id
   unsigned id;
   /// The next string
   std::string s;

   public:
   /// Constructor
   StringReader(ifstream& in,vector<StringEntry>& strings) : in(in),strings(strings),id(0) {}

   /// Read the next string
   bool next(unsigned& len,const char*& data);
   /// Remember a string position and hash
   void rememberInfo(unsigned page,unsigned ofs,unsigned hash);
};
//---------------------------------------------------------------------------
bool StringReader::next(unsigned& len,const char*& data)
   // Read the next string
{
   unsigned nextId;
   in >> nextId;
   in.get();
   getline(in,s);
   if (!in.good()) return false;

   if (id!=nextId) {
      cerr << "error: got id " << nextId << ", expected " << id << endl << "strings must be sorted 0,1,2,..." << endl;
      throw;
   } else {
      id++;
   }
   while (s.length()&&((s[s.length()-1]=='\r')||(s[s.length()-1]=='\n')))
      s=s.substr(0,s.length()-1);
   len=s.size();
   data=s.c_str();
   return true;
}
//---------------------------------------------------------------------------
void StringReader::rememberInfo(unsigned page,unsigned ofs,unsigned hash)
   // Remember a string position and hash
{
   StringEntry s;
   s.page=page; s.ofsLen=ofs; s.hash=hash;
   strings.push_back(s);
}
//---------------------------------------------------------------------------
/// Read the mapping information
class StringMappingReader : public DatabaseBuilder::StringInfoReader
{
   private:
   /// The range
   vector<StringEntry>::const_iterator iter,limit;

   public:
   /// Constructor
   StringMappingReader(const vector<StringEntry>& strings) : iter(strings.begin()),limit(strings.end()) {}

   /// Read the next entry
   bool next(unsigned& v1,unsigned& v2);
};
//---------------------------------------------------------------------------
bool StringMappingReader::next(unsigned& v1,unsigned& v2)
   // Read the next enty
{
   if (iter==limit)
      return false;
   v1=(*iter).page;
   v2=(*iter).ofsLen;
   ++iter;
   return true;
}
//---------------------------------------------------------------------------
/// Read the hash information
class StringHashReader : public DatabaseBuilder::StringInfoReader
{
   private:
   /// The range
   vector<StringEntry>::const_iterator iter,limit;

   public:
   /// Constructor
   StringHashReader(const vector<StringEntry>& strings) : iter(strings.begin()),limit(strings.end()) {}

   /// Read the next entry
   bool next(unsigned& v1,unsigned& v2);
};
//---------------------------------------------------------------------------
bool StringHashReader::next(unsigned& v1,unsigned& v2)
   // Read the next enty
{
   if (iter==limit)
      return false;
   v1=(*iter).hash;
   v2=(*iter).page;
   ++iter;
   return true;
}
//---------------------------------------------------------------------------
static bool readAndPackStrings(DatabaseBuilder& builder,const char* fileName,vector<StringEntry>& strings)
   // Read the facts table and pack it into the output file
{
   ifstream in(fileName);
   if (!in.is_open()) {
      cout << "unable to open " << fileName << endl;
      return false;
   }

   // Scan the strings and dump them
   {
      StringReader reader(in,strings);
      builder.loadStrings(reader);
   }

   return true;
}
//---------------------------------------------------------------------------
bool buildDatabase(DatabaseBuilder& builder,const char* factsFile,const char* stringsFile)
   // Build the initial database
{
   // Process the facts
   {
      // Read the facts table
      cout << "Reading the facts table..." << endl;
      vector<Triple> facts;
      if (!readFacts(facts,factsFile))
         return false;

      // Produce the different orderings
      dumpFacts(builder,facts);
   }

   // Process the strings
   {
      // Read the strings table
      cout << "Reading the strings table..." << endl;
      vector<StringEntry> strings;
      if (!readAndPackStrings(builder,stringsFile,strings))
         return 1;

      // Write the string mapping
      cout << "Writing the string mapping..." << endl;
      {
         StringMappingReader reader(strings);
         builder.loadStringMappings(reader);
      }

      // Write the string index
      cout << "Writing the string index..." << endl;
      {
         std::sort(strings.begin(),strings.end(),OrderStringByHash());
         StringHashReader reader(strings);
         builder.loadStringHashes(reader);
      }
   }

   // Finish the load phase
   builder.finishLoading();

   return true;
}
//---------------------------------------------------------------------------
bool buildDatabaseStatistics(DatabaseBuilder& builder,const char* targetName)
   // Build the database statistics
{
   // Compute the individual statistics
   cout << "Computing statistics..." << endl;
   for (unsigned index=0;index<6;index++) {
      cout << "Building statistic " << (index+1) << "..." << endl;
      builder.computeStatistics(index);
   }
   cout << "Building path statistic" << endl;
   builder.computePathStatistics();

   cout << "Building exact statistic" << endl;
   string tmpName=targetName;
   tmpName+=".tmp";
   builder.computeExactStatistics(tmpName.c_str());
   remove(tmpName.c_str());

   return true;
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
int main(int argc,char* argv[])
{
   if (argc!=4) {
      cout << "usage: " << argv[0] << " <facts> <strings> <target>" << endl;
      return 1;
   }

   // Build the initial database
   DatabaseBuilder builder(argv[3]);
   if (!buildDatabase(builder,argv[1],argv[2]))
      return 1;

   // Compute the missing statistics
   if (!buildDatabaseStatistics(builder,argv[3]))
      return 1;
}
//---------------------------------------------------------------------------
