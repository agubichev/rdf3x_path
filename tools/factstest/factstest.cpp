#include "rts/database/Database.hpp"
#include "rts/segment/DictionarySegment.hpp"
#include "rts/segment/FactsSegment.hpp"
#include "infra/osdep/Timestamp.hpp"
#include <iostream>
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
   unsigned id;
   if (!dict.lookup(term,id)) {
      std::cout << "the term " << term << " was not found in the dictionary!" << std::endl;
      return 1;
   }

   // Scan the facts about this predicate
   Timestamp t1;
   FactsSegment& facts=db.getFacts(Database::Order_Predicate_Object_Subject);
   FactsSegment::Scan scan;
   unsigned results=0,groups=0,currentGroup=0;
   const char* groupNameStart,*groupNameStop;
   if (scan.first(facts,id,0,0)) {
      while (scan.getValue1()==id) {
         if ((!groups)||(scan.getValue2()!=currentGroup)) {
            currentGroup=scan.getValue2();
            dict.lookupById(currentGroup,groupNameStart,groupNameStop);
            groups++;
         }
         ++results;
         if (!scan.next())
            break;
      }
   }
   Timestamp t2;

   // Timing
   std::cout << "Got " << results << " matching facts in " << groups << " groups" << std::endl
             << "Duration: " << (t2-t1) << std::endl;
}
//---------------------------------------------------------------------------
