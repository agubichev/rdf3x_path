#include <fstream>
#include <iostream>
#include <vector>
#include <algorithm>
#include <map>
#include <set>
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
struct OrderTripleByPredicate {
   bool operator()(const Triple& a,const Triple& b) const {
      return (a.predicate<b.predicate)||
             ((a.predicate==b.predicate)&&((a.subject<b.subject)||
             ((a.subject==b.subject)&&(a.object<b.object))));
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
void dumpFacts(ofstream& out,const string& name,vector<Triple>& facts)
   // Dump the facts
{
   // Sort the facts
   sort(facts.begin(),facts.end(),OrderTripleByPredicate());

   // Eliminate duplicates
   vector<Triple>::iterator writer=facts.begin();
   unsigned lastSubject=~0u,lastPredicate=~0u,lastObject=~0u;
   for (vector<Triple>::iterator iter=facts.begin(),limit=facts.end();iter!=limit;++iter)
      if ((((*iter).subject)!=lastSubject)||(((*iter).predicate)!=lastPredicate)||(((*iter).object)!=lastObject)) {
         *writer=*iter;
         ++writer;
         lastSubject=(*iter).subject; lastPredicate=(*iter).predicate; lastObject=(*iter).object;
      }
   facts.resize(writer-facts.begin());

   // Dump the facts
   {
      unlink("/tmp/facts.sql");
      ofstream out("/tmp/facts.sql");
      for (vector<Triple>::const_iterator iter=facts.begin(),limit=facts.end();iter!=limit;++iter)
         out << (*iter).subject << "\t" << (*iter).predicate << "\t" << (*iter).object << std::endl;
   }

   // And write the copy statement
   out << "drop schema if exists " << name << " cascade;" << endl;
   out << "create schema " << name << ";" << endl;
   out << "create table " << name << ".facts(subject int not null, predicate int not null, object int not null);" << endl;
   out << "copy " << name << ".facts from '/tmp/facts.sql';" << endl;

   // Create indices
   out << "create index facts_spo on " << name << ".facts (subject, predicate, object);" << endl;
   out << "create index facts_pso on " << name << ".facts (predicate, subject, object);" << endl;
   out << "create index facts_pos on " << name << ".facts (predicate, object, subject);" << endl;
}
//---------------------------------------------------------------------------
string escapeCopy(const string& s)
   // Escape an SQL string
{
   string result;
   for (string::const_iterator iter=s.begin(),limit=s.end();iter!=limit;++iter) {
      char c=(*iter);
      switch (c) {
         case '\\': result+="\\\\"; break;
         case '\"': result+="\\\""; break;
         case '\'': result+="\\\'"; break;
         default:
            /* if (c<' ') {
               result+='\\';
               result+=c;
            } else */ result+=c;
      }
   }
   return result;
}
//---------------------------------------------------------------------------
bool readAndStoreStrings(ofstream& out,const string& name,const char* fileName)
   // Read the facts table and store it in the database
{
   // Now open the strings again
   ifstream in(fileName);
   if (!in.is_open()) {
      cout << "unable to open " << fileName << endl;
      return false;
   }

   // Prepare the strings table
   out << "create table " << name << ".strings(id int not null primary key, value varchar(4000) not null);" << endl;

   // Scan the strings and dump them
   {
      unlink("/tmp/strings.sql");
      ofstream out("/tmp/strings.sql");
      string s;
      while (true) {
         unsigned id;
         in >> id;
         in.get();
         getline(in,s);
         if (!in.good()) break;
         while (s.length()&&((s[s.length()-1]=='\r')||(s[s.length()-1]=='\n')))
            s=s.substr(0,s.length()-1);

         // Store the string
         out << id << "\t\"" << escapeCopy(s) << "\"" << endl;
      }
   }

   // Add the copy statement
   out << "copy " << name << ".strings from '/tmp/strings.sql';" << endl;

   return true;
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
int main(int argc,char* argv[])
{
   if (argc!=4) {
      cout << "usage: " << argv[0] << " <facts> <strings> <name>" << endl;
      return 1;
   }

   // Output
   ofstream out("commands.sql");

   // Process the facts
   {
      // Read the facts table
      cout << "Reading the facts table..." << endl;
      vector<Triple> facts;
      if (!readFacts(facts,argv[1]))
         return 1;

      // Write them to the database
      dumpFacts(out,argv[3],facts);
   }

   // Process the strings
   {
      // Read the strings table
      cout << "Reading the strings table..." << endl;
      if (!readAndStoreStrings(out,argv[3],argv[2]))
         return 1;
   }
}
//---------------------------------------------------------------------------
