#include "cts/parser/TurtleParser.hpp"
#include "rts/database/Database.hpp"
#include "rts/runtime/BulkOperation.hpp"
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
bool smallAddressSpace()
   // Is the address space too small?
{
   return sizeof(void*)<8;
}
//---------------------------------------------------------------------------
static void importStream(DifferentialIndex& diff,istream& in)
   // Import new triples from an input stream
{
   BulkOperation bulk(diff);
   TurtleParser parser(in);
   string subject,predicate,object,objectSubType;
   Type::ID objectType;
   while (true) {
      if (!parser.parse(subject,predicate,object,objectType,objectSubType))
         break;
      bulk.insert(subject,predicate,object,objectType,objectSubType);
   }
   bulk.commit();
}
//---------------------------------------------------------------------------
int main(int argc,char* argv[])
{
   // Warn first
   if (smallAddressSpace())
      cerr << "Warning: Running RDF-3X on a 32 bit system is not supported and will fail for large data sets. Please use a 64 bit system instead!" << endl;

   // Greeting
   cerr << "RDF-3X turtle updater" << endl
        << "(c) 2009 Thomas Neumann. Web site: http://www.mpi-inf.mpg.de/~neumann/rdf3x" << endl;

   // Check the arguments
   if (argc<2) {
      cerr << "usage: " << argv[0] << " <database> <input>" << endl
           << "without input file data is read from stdin" << endl;
      return 1;
   }

   // Open the database
   Database db;
   if (!db.open(argv[1])) {
      cerr << "unable to open " << argv[1] << endl;
      return 1;
   }

   // And incorporate changes
   DifferentialIndex diff(db);
   if (argc==2) {
      importStream(diff,cin);
   } else {
      for (int index=2;index<argc;index++) {
         ifstream in(argv[index]);
         if (!in.is_open()) {
            cerr << "unable top open " << argv[index] << endl;
            return 1;
         }
         importStream(diff,in);
      }
   }
   diff.sync();
}
//---------------------------------------------------------------------------
