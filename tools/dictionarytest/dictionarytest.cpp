#include "rts/database/Database.hpp"
#include "rts/segment/DictionarySegment.hpp"
#include "infra/osdep/Timestamp.hpp"
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
int main(int argc,char* argv[])
{
   if (argc<2) {
      std::cout << "usage: " << argv[0] << " <rdfstore> [term]" << std::endl;
      return 1;
   }

   // Open the database
   Database db;
   if (!db.open(argv[1])) {
      std::cout << "unable to open " << argv[1] << std::endl;
      return 1;
   }
   DictionarySegment& dict=db.getDictionary();

   // Choose a term
   std::string term="http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
   if (argc>=3)
      term=argv[2];

   // Lookup its id
   Timestamp t1;
   unsigned id;
   if (!dict.lookup(term,id)) {
      std::cout << "the term " << term << " was not found in the dictionary!" << std::endl;
      return 1;
   }


   // Lookup the value by id
   Timestamp t2;
   std::string val;
   if (!dict.lookupById(id,val)) {
      std::cout << "the id " << id << " was not found in the dictionary!" << std::endl;
      return 1;
   }

   // Compare
   Timestamp t3;
   if (term!=val) {
      std::cout << "the dictionary is not bijective, " << term << "!=" << val << std::endl;
      return 1;
   }

   // Timing
   std::cout << "Round-trip ok" << std::endl
             << "Duration lookup by string: " << (t2-t1) << std::endl
             << "Duration lookup by id: " << (t3-t2) << std::endl;
}
//---------------------------------------------------------------------------
