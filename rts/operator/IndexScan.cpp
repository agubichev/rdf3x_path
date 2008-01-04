#include "rts/operator/IndexScan.hpp"
#include "rts/runtime/Runtime.hpp"
#include <iostream>
//---------------------------------------------------------------------------
IndexScan::IndexScan(Database& db,Database::DataOrder order,Register* subject,bool subjectBound,Register* predicate,bool predicateBound,Register* object,bool objectBound)
   : prefix(0),filter(0),facts(db.getFacts(order)),order(order)
   // Constructor
{
   // Setup the slot bindings
   switch (order) {
      case Database::Order_Subject_Predicate_Object:
         value1=subject; value2=predicate; value3=object;
         bound1=subjectBound; bound2=predicateBound; bound3=objectBound;
         break;
      case Database::Order_Subject_Object_Predicate:
         value1=subject; value2=object; value3=predicate;
         bound1=subjectBound; bound2=objectBound; bound3=predicateBound;
         break;
      case Database::Order_Object_Predicate_Subject:
         value1=object; value2=predicate; value3=subject;
         bound1=objectBound; bound2=predicateBound; bound3=subjectBound;
         break;
      case Database::Order_Object_Subject_Predicate:
         value1=object; value2=subject; value3=predicate;
         bound1=objectBound; bound2=subjectBound; bound3=predicateBound;
         break;
      case Database::Order_Predicate_Subject_Object:
         value1=predicate; value2=subject; value3=object;
         bound1=predicateBound; bound2=subjectBound; bound3=objectBound;
         break;
      case Database::Order_Predicate_Object_Subject:
         value1=predicate; value2=object; value3=subject;
         bound1=predicateBound; bound2=objectBound; bound3=subjectBound;
         break;
   }
   // Construct the filtering slots (if any)
   if (!bound1) {
      if (bound2) filter|=2;
      if (bound3) filter|=1;
   } else {
      prefix|=4;
      if (!bound2) {
         if (bound3) filter|=1;
      } else {
         prefix|=2;
         if (bound3)
            prefix|=3;
      }
   }
}
//---------------------------------------------------------------------------
IndexScan::~IndexScan()
   // Destructor
{
}
//---------------------------------------------------------------------------
unsigned IndexScan::first()
   // Produce the first tuple
{
   // Compute the start/stop conditions
   unsigned start1=0,start2=0,start3=0;
   stop1=~0u; stop2=~0u; stop3=~0u;
   if (bound1) {
      start1=stop1=value1->value;
      if (bound2) {
         start2=stop2=value2->value;
         if (bound3) {
            start3=stop3=value3->value;
         }
      }
   }

   // Start the scan depending on if we have conditions or not
   if (start1||start2||start3) {
      if (!scan.first(facts,start1,start2,start3))
         return false;
   } else {
      if (!scan.first(facts))
         return false;
   }

   // Check the columns
   unsigned filter=this->filter,prefix=this->prefix;
   bool onBorder=false;
   if (prefix&4) {
      unsigned v1=scan.getValue1();
      if (v1>stop1)
         return false;
      onBorder=(v1==stop1);
   } else value1->value=scan.getValue1();
   if (prefix&2) {
      unsigned v2=scan.getValue2();
      if (onBorder&&(v2>stop2))
         return false;
      onBorder=onBorder&&(v2==stop2);
   } else if (filter&2) {
      if (scan.getValue2()!=value2->value)
         return next();
   } else value2->value=scan.getValue2();
   if (prefix&1) {
      unsigned v3=scan.getValue3();
      if (onBorder&&(v3>stop3))
         return false;
   } else if (filter&1) {
      if (scan.getValue3()!=value3->value)
         return next();
   } else value3->value=scan.getValue3();

   // We have a match
   return 1;
}
//---------------------------------------------------------------------------
unsigned IndexScan::next()
   // Produce the next tuple
{
   unsigned filter=this->filter,prefix=this->prefix;
   while (true) {
      // Access the next tuple
      if (!scan.next())
         return false;

      // Check the columns
      bool onBorder=false;
      if (prefix&4) {
         unsigned v1=scan.getValue1();
         if (v1>stop1)
            return false;
         onBorder=(v1==stop1);
      } else value1->value=scan.getValue1();
      if (prefix&2) {
         unsigned v2=scan.getValue2();
         if (onBorder&&(v2>stop2))
            return false;
         onBorder=onBorder&&(v2==stop2);
      } else if (filter&2) {
         if (scan.getValue2()!=value2->value)
            continue;
      } else value2->value=scan.getValue2();
      if (prefix&1) {
         unsigned v3=scan.getValue3();
         if (onBorder&&(v3>stop3))
            return false;
      } else if (filter&1) {
         if (scan.getValue3()!=value3->value)
            continue;
      } else value3->value=scan.getValue3();

      // We have a match
      return 1;
   }
}
//---------------------------------------------------------------------------
void IndexScan::print(unsigned level)
   // Print the operator tree. Debugging only.
{
   indent(level); std::cout << "<IndexScan ";
   switch (order) {
      case Database::Order_Subject_Predicate_Object: std::cout << "SubjectPredicateObject"; break;
      case Database::Order_Subject_Object_Predicate: std::cout << "SubjectObjectPredicate"; break;
      case Database::Order_Object_Predicate_Subject: std::cout << "ObjectPredicateSubject"; break;
      case Database::Order_Object_Subject_Predicate: std::cout << "ObjectSubjectPredicate"; break;
      case Database::Order_Predicate_Subject_Object: std::cout << "PredicateSubjectObject"; break;
      case Database::Order_Predicate_Object_Subject: std::cout << "PredicateObjectSubject"; break;
   }
   std::cout << std::endl;
   indent(level+1);
   printRegister(value1); if (bound1) std::cout << "*";
   std::cout << " ";
   printRegister(value2); if (bound2) std::cout << "*";
   std::cout << " ";
   printRegister(value3); if (bound3) std::cout << "*";
   std::cout << std::endl;
   indent(level); std::cout << ">" << std::endl;
}
//---------------------------------------------------------------------------
