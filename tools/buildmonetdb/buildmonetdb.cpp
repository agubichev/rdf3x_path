#include <fstream>
#include <iostream>
#include <vector>
#include <algorithm>
#include <map>
#include <set>
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
namespace {
//---------------------------------------------------------------------------
/// A RDF tripple
struct Tripple {
   /// The values as IDs
   unsigned subject,predicate,object;
};
//---------------------------------------------------------------------------
/// Order a RDF tripple lexicographically
struct OrderTrippleByPredicate {
   bool operator()(const Tripple& a,const Tripple& b) const {
      return (a.predicate<b.predicate)||
             ((a.predicate==b.predicate)&&((a.subject<b.subject)||
             ((a.subject==b.subject)&&(a.object<b.object))));
   }
};
//---------------------------------------------------------------------------
string monetDBCommand()
   // Build the base command
{
   return string(getenv("HOME"))+"/MonetDB/bin/mclient --language=sql --database=rdf";
}
//---------------------------------------------------------------------------
bool executeSQLFile(const std::string& file)
{
   string command=monetDBCommand()+" '"+file+"'";
   if (system(command.c_str())!=0) {
      return false;
   }
   return true;
}
//---------------------------------------------------------------------------
bool readFacts(vector<Tripple>& facts,const char* fileName)
   // Read the facts table
{
   ifstream in(fileName);
   if (!in.is_open()) {
      cout << "unable to open " << fileName << endl;
      return false;
   }

   facts.clear();
   while (true) {
      Tripple t;
      in >> t.subject >> t.predicate >> t.object;
      if (!in.good()) break;
      facts.push_back(t);
   }

   return true;
}
//---------------------------------------------------------------------------
void dumpFacts(ofstream& out,vector<Tripple>& facts,map<unsigned,unsigned>& predicates)
   // Dump the facts
{
   // Sort the facts
   sort(facts.begin(),facts.end(),OrderTrippleByPredicate());

   // Eliminate duplicates
   vector<Tripple>::iterator writer=facts.begin();
   unsigned lastSubject=~0u,lastPredicate=~0u,lastObject=~0u;
   for (vector<Tripple>::iterator iter=facts.begin(),limit=facts.end();iter!=limit;++iter)
      if ((((*iter).subject)!=lastSubject)||(((*iter).predicate)!=lastPredicate)||(((*iter).object)!=lastObject)) {
         *writer=*iter;
         ++writer;
         lastSubject=(*iter).subject; lastPredicate=(*iter).predicate; lastObject=(*iter).object;
      }
   facts.resize(writer-facts.begin());

   // And dump them
   vector<Tripple>::const_iterator lastStart=facts.begin();
   lastPredicate=~0u;
   unsigned predicateId=0;
   for (vector<Tripple>::const_iterator iter=facts.begin(),limit=facts.end();iter!=limit;++iter) {
      if ((*iter).predicate!=lastPredicate) {
         if (iter!=lastStart) {
            out << "create table p" << predicateId << "(subject int not null, object int not null);" << endl;
            out << "copy " << (iter-lastStart) << " records into \"p" << predicateId << "\" from stdin using delimiters '\\t';" << endl;
            for (;lastStart!=iter;++lastStart)
               out << (*lastStart).subject << "\t" << (*lastStart).object << endl;
            ++predicateId;
         }
         lastPredicate=(*iter).predicate;
         predicates[lastPredicate]=predicateId;
      }
   }
   if (facts.end()!=lastStart) {
      out << "create table p" << predicateId << "(subject int not null, object int not null);" << endl;
      out << "copy " << (facts.end()-lastStart) << " records into \"p" << predicateId << "\" from stdin using delimiters '\\t';" << endl;
      for (;lastStart!=facts.end();++lastStart)
         out << (*lastStart).subject << "\t" << (*lastStart).object << endl;
   }
}
//---------------------------------------------------------------------------
string escapeCopy(const string& s)
   // Escape an SQL string
{
   string result;
   for (string::const_iterator iter=s.begin(),limit=s.end();iter!=limit;++iter) {
      char c=(*iter);
      if (c=='\\')
         result+="\\\\"; else
      if (c=='\"') 
         result+="\\\""; else
         result+=c;
   }
   return result;
}
//---------------------------------------------------------------------------
bool readAndStoreStrings(ofstream& out,const char* fileName,const map<unsigned,unsigned>& properties)
   // Read the facts table and store it in the database
{
   // Now open the strings again
   ifstream in(fileName);
   if (!in.is_open()) {
      cout << "unable to open " << fileName << endl;
      return false;
   }

   // Prepare the strings table
   // out << "drop table strings;" << endl
   // out << "drop table reversestrings;" << endl
   out << "create table strings(id int not null primary key, value varchar(4000) not null);" << endl;
   out << "create table reversestrings(value varchar(4000) not null primary key,id int not null);" << endl;

   // Prepare the filter
   static const char* filterStrings[]={"http://simile.mit.edu/2006/01/ontologies/mods3#access","http://simile.mit.edu/2006/01/ontologies/mods3#address","http://simile.mit.edu/2006/01/ontologies/mods3#affiliation","http://simile.mit.edu/2006/01/ontologies/mods3#authority","http://simile.mit.edu/2006/01/ontologies/mods3#catalogingLanguage","http://simile.mit.edu/2006/01/ontologies/mods3#changed","http://simile.mit.edu/2006/01/ontologies/mods3#code","http://simile.mit.edu/2006/01/ontologies/mods3#contents","http://simile.mit.edu/2006/01/ontologies/mods3#copyrightDate","http://simile.mit.edu/2006/01/ontologies/mods3#created","http://simile.mit.edu/2006/01/ontologies/mods3#dateCreated","http://simile.mit.edu/2006/01/ontologies/mods3#dates","http://simile.mit.edu/2006/01/ontologies/mods3#edition","http://simile.mit.edu/2006/01/ontologies/mods3#encoding","http://simile.mit.edu/2006/01/ontologies/mods3#extent","http://simile.mit.edu/2006/01/ontologies/mods3#fullName","http://simile.mit.edu/2006/01/ontologies/mods3#issuance","http://simile.mit.edu/2006/01/ontologies/mods3#language","http://simile.mit.edu/2006/01/ontologies/mods3#nonSort","http://simile.mit.edu/2006/01/ontologies/mods3#origin","http://simile.mit.edu/2006/01/ontologies/mods3#partName","http://simile.mit.edu/2006/01/ontologies/mods3#partNumber","http://simile.mit.edu/2006/01/ontologies/mods3#physicalDescription","http://simile.mit.edu/2006/01/ontologies/mods3#point","http://simile.mit.edu/2006/01/ontologies/mods3#qualifier","http://simile.mit.edu/2006/01/ontologies/mods3#records","http://simile.mit.edu/2006/01/ontologies/mods3#sub","http://www.w3.org/1999/02/22-rdf-syntax-ns#type",0};
   set<unsigned> filteredProperties;

   // Scan the strings and dump them
   vector<pair<unsigned,string> > propertyNames;
   vector<pair<unsigned,string> > stringCache;
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
      stringCache.push_back(pair<unsigned,string>(id,s));
      if (stringCache.size()==100000) {
         out << "copy " << stringCache.size() << " records into \"strings\" from stdin using delimiters '\\t';" << endl;
         for (vector<pair<unsigned,string> >::const_iterator iter=stringCache.begin(),limit=stringCache.end();iter!=limit;++iter)
            out << (*iter).first << "\t\"" << escapeCopy((*iter).second) << "\"" << endl;
         out << "copy " << stringCache.size() << " records into \"reversestrings\" from stdin using delimiters '\\t';" << endl;
         for (vector<pair<unsigned,string> >::const_iterator iter=stringCache.begin(),limit=stringCache.end();iter!=limit;++iter)
            out << "\"" << escapeCopy((*iter).second) << "\"\t" << (*iter).first << endl;
         stringCache.clear();
      }

      // A known property?
      if (properties.count(id)) {
         propertyNames.push_back(pair<unsigned,string>((*(properties.find(id))).second,s));
         for (const char** iter=filterStrings;*iter;++iter)
            if (s==(*iter)) {
               filteredProperties.insert((*(properties.find(id))).second);
               break;
            }
      }
   }
   out << "copy " << stringCache.size() << " records into \"strings\" from stdin using delimiters '\\t';" << endl;
   for (vector<pair<unsigned,string> >::const_iterator iter=stringCache.begin(),limit=stringCache.end();iter!=limit;++iter)
      out << (*iter).first << "\t\"" << escapeCopy((*iter).second) << "\"" << endl;
   out << "copy " << stringCache.size() << " records into \"reversestrings\" from stdin using delimiters '\\t';" << endl;
   for (vector<pair<unsigned,string> >::const_iterator iter=stringCache.begin(),limit=stringCache.end();iter!=limit;++iter)
      out << "\"" << escapeCopy((*iter).second) << "\"\t" << (*iter).first << endl;

   // Dump the property names
   // out << "drop table propertynames;" << endl
   out << "create table propertynames (id int not null primary key, name varchar(4000) not null);" << endl;
   out << "copy " << propertyNames.size() << " records into \"propertynames\" from stdin using delimiters '\\t';" << endl;
   for (vector<pair<unsigned,string> >::const_iterator iter=propertyNames.begin(),limit=propertyNames.end();iter!=limit;++iter) {
      out << ((*iter).first) << "\t" << escapeCopy((*iter).second) << endl;
   }

   // Build the views
   //out << "drop view allproperties;" << endl
   out << "create view allproperties as ";
   for (vector<pair<unsigned,string> >::const_iterator iter=propertyNames.begin(),limit=propertyNames.end();iter!=limit;++iter) {
      if (iter!=propertyNames.begin())
         out << " union all";
      out << " (select subject," << (*iter).first << " as predicate,object from p" << (*iter).first <<")";
   }
   out << ";" << endl;
   //out << "drop view filteredproperties;" << endl
   out << "create view filteredproperties as ";
   for (set<unsigned>::const_iterator iter=filteredProperties.begin(),limit=filteredProperties.end();iter!=limit;++iter) {
      if (iter!=filteredProperties.begin())
         out << " union all";
      out << " (select subject," << (*iter) << " as predicate,object from p" << (*iter) <<")";
   }
   out << ";" << endl;

   return true;
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
int main(int argc,char* argv[])
{
   if (argc!=3) {
      cout << "usage: " << argv[0] << " <facts> <strings>" << endl;
      return 1;
   }

   // Output
   ofstream out("commands.sql");

   // Process the facts
   map<unsigned,unsigned> properties;
   {
      // Read the facts table
      cout << "Reading the facts table..." << endl;
      vector<Tripple> facts;
      if (!readFacts(facts,argv[1]))
         return 1;

      // Write them to the database
      dumpFacts(out,facts,properties);
   }

   // Process the strings
out.close();
   ofstream out2("commands2.sql");
   {
      // Read the strings table
      cout << "Reading the strings table..." << endl;
      if (!readAndStoreStrings(out2,argv[2],properties))
         return 1;
   }

   // Run it
   out2.close();

//   executeSQLFile("commands.sql");
//   executeSQLFile("commands2.sql");
}
//---------------------------------------------------------------------------
