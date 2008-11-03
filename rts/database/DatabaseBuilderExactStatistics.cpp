#include "rts/database/DatabaseBuilder.hpp"
#include "rts/database/Database.hpp"
#include "rts/segment/AggregatedFactsSegment.hpp"
#include "rts/segment/FactsSegment.hpp"
#include "rts/runtime/Runtime.hpp"
#include "rts/operator/FullyAggregatedIndexScan.hpp"
#include <iostream>
#include <cassert>
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
static void computeExact2Leaves(Database& db,ofstream& /*out*/,unsigned& /*page*/,Database::DataOrder order)
   // Compute the exact statistics for patterns with two constants
{
   FactsSegment::Scan scan;
   if (scan.first(db.getFacts(order))) {
      // Prepare scanning the aggregated indices
      Register mergeValue,valueS,valueP,valueO;
      mergeValue.reset(); valueS.reset(); valueP.reset(); valueO.reset();
      FullyAggregatedIndexScan* scanS=FullyAggregatedIndexScan::create(db,Database::Order_Subject_Predicate_Object,&valueS,false,0,false,0,false);
      FullyAggregatedIndexScan* scanP=FullyAggregatedIndexScan::create(db,Database::Order_Predicate_Subject_Object,0,false,&valueP,false,0,false);
      FullyAggregatedIndexScan* scanO=FullyAggregatedIndexScan::create(db,Database::Order_Object_Subject_Predicate,0,false,0,false,&valueO,false);
      scanS->addMergeHint(&mergeValue,&valueS);
      scanP->addMergeHint(&mergeValue,&valueP);
      scanO->addMergeHint(&mergeValue,&valueO);

      // And scan
      unsigned last1=~0u,last2=~0u,countS=0,countP=0,countO=0;
      unsigned lastS=0,lastP=0,lastO=0;
      do {
         // A new entry?
         if ((scan.getValue1()!=last1)||(scan.getValue2()!=last2)) {
            if (~last1) {
               cout << last1 << " " << last2 << " " << countS << " " << countP << " " << countO << endl;
            }
            last1=scan.getValue1();
            last2=scan.getValue2();
            countS=0;
            countP=0;
            countO=0;
            mergeValue.value=scan.getValue3();
            lastS=scanS->first();
            lastP=scanP->first();
            lastO=scanO->first();
         }
         // Compare
         mergeValue.value=scan.getValue3();
         while (lastS) {
            if (mergeValue.value==valueS.value) {
               countS+=lastS;
               break;
            }
            if (mergeValue.value<valueS.value)
               break;
            lastS=scanS->next();
         }
         while (lastP) {
            if (mergeValue.value==valueP.value) {
               countP+=lastP;
               break;
            }
            if (mergeValue.value<valueP.value)
               break;
            lastP=scanP->next();
         }
         while (lastO) {
            if (mergeValue.value==valueO.value) {
               countO+=lastO;
               break;
            }
            if (mergeValue.value<valueO.value)
               break;
            lastO=scanO->next();
         }
      } while (scan.next());
      // Add the last entry
      if (~last1) {
         cout << last1 << " " << last2 << " " << countS << " " << countP << " " << countO << endl;
      }
      // Cleanup
      delete scanS;
      delete scanP;
      delete scanO;
   }
}
//---------------------------------------------------------------------------
static unsigned computeExact2(Database& db,ofstream& out,unsigned& page,Database::DataOrder order)
   // Compute the exact statistics for patterns with two constants
{
   computeExact2Leaves(db,out,page,order);
   return 0;
}
//---------------------------------------------------------------------------
static unsigned collectMatches(FullyAggregatedIndexScan* scan,bool first,const Register& v1,const Register& v2,unsigned& c)
   // Collect matching entries
{
   // First?
   if (first) {
      c=scan->first();
      if (!c) return 0;
   }
   while (true) {
      // Compare
      if (v1.value==v2.value)
         return c;
      if (v1.value<v2.value)
         return 0;
      c=scan->next();
      if (!c) return 0;
   }
}
//---------------------------------------------------------------------------
static void computeExact1Leaves(Database& db,ofstream& /*out*/,unsigned& /*page*/,Database::DataOrder order1,Database::DataOrder order2)
   // Compute the exact statistics for patterns with one constant
{
   AggregatedFactsSegment::Scan scan1,scan2;
   if (scan1.first(db.getAggregatedFacts(order1))&&scan2.first(db.getAggregatedFacts(order2))) {
      // Prepare scanning the aggregated indices
      Register mergeValue1,valueS1,valueP1,valueO1;
      mergeValue1.reset(); valueS1.reset(); valueP1.reset(); valueO1.reset();
      FullyAggregatedIndexScan* scanS1=FullyAggregatedIndexScan::create(db,Database::Order_Subject_Predicate_Object,&valueS1,false,0,false,0,false);
      FullyAggregatedIndexScan* scanP1=FullyAggregatedIndexScan::create(db,Database::Order_Predicate_Subject_Object,0,false,&valueP1,false,0,false);
      FullyAggregatedIndexScan* scanO1=FullyAggregatedIndexScan::create(db,Database::Order_Object_Subject_Predicate,0,false,0,false,&valueO1,false);
      scanS1->addMergeHint(&mergeValue1,&valueS1);
      scanP1->addMergeHint(&mergeValue1,&valueP1);
      scanO1->addMergeHint(&mergeValue1,&valueO1);
      Register mergeValue2,valueS2,valueP2,valueO2;
      mergeValue2.reset(); valueS2.reset(); valueP2.reset(); valueO2.reset();
      FullyAggregatedIndexScan* scanS2=FullyAggregatedIndexScan::create(db,Database::Order_Subject_Predicate_Object,&valueS2,false,0,false,0,false);
      FullyAggregatedIndexScan* scanP2=FullyAggregatedIndexScan::create(db,Database::Order_Predicate_Subject_Object,0,false,&valueP2,false,0,false);
      FullyAggregatedIndexScan* scanO2=FullyAggregatedIndexScan::create(db,Database::Order_Object_Subject_Predicate,0,false,0,false,&valueO2,false);
      scanS2->addMergeHint(&mergeValue2,&valueS2);
      scanP2->addMergeHint(&mergeValue2,&valueP2);
      scanO2->addMergeHint(&mergeValue2,&valueO2);

      // And scan
      bool done=false;
      while (!done) {
         // Read scan1
         unsigned last1=scan1.getValue1(),countS1=0,countP1=0,countO1=0,cs1,cp1,co1;
         bool first1=true;
         while (true) {
            if (scan1.getValue1()!=last1)
               break;
            mergeValue1.value=scan1.getValue2();
            countS1+=collectMatches(scanS1,first1,mergeValue1,valueS1,cs1);
            countP1+=collectMatches(scanP1,first1,mergeValue1,valueP1,cp1);
            countO1+=collectMatches(scanO1,first1,mergeValue1,valueO1,co1);
            first1=false;
            if (!scan1.next()) {
               done=true;
               break;
            }
         }

         // Read scan2
         unsigned last2=scan2.getValue1(),countS2=0,countP2=0,countO2=0,cs2,cp2,co2;
         bool first2=true;
         while (true) {
            if (scan2.getValue1()!=last2)
               break;
            mergeValue2.value=scan2.getValue2();
            countS2+=collectMatches(scanS2,first2,mergeValue2,valueS2,cs2);
            countP2+=collectMatches(scanP2,first2,mergeValue2,valueP2,cp2);
            countO2+=collectMatches(scanO2,first2,mergeValue2,valueO2,co2);
            first2=false;
            if (!scan2.next()) {
               done=true;
               break;
            }
         }

         // Produce output tuple
         assert(last1==last2);
         cout << last1 << " " << countS1 << " " << countP1 << " " << countO1 << " " << countS2 << " " << countP2 << " " << countO2 << std::endl;
      }

      // Cleanup
      delete scanS1;
      delete scanP1;
      delete scanO1;
      delete scanS2;
      delete scanP2;
      delete scanO2;
   }
}
//---------------------------------------------------------------------------
static unsigned computeExact1(Database& db,ofstream& out,unsigned& page,Database::DataOrder order1,Database::DataOrder order2)
   // Compute the exact statistics for patterns with one constant
{
   computeExact1Leaves(db,out,page,order1,order2);
   return 0;
}
//---------------------------------------------------------------------------
static unsigned computeExact0(Database& db,Database::DataOrder order1,Database::DataOrder order2)
   // Compute the exact statistics for patterns without constants
{
   FullyAggregatedFactsSegment::Scan scan1,scan2;
   if (scan1.first(db.getFullyAggregatedFacts(order1))&&scan2.first(db.getFullyAggregatedFacts(order2))) {
      unsigned result=0;
      while (true) {
         if (scan1.getValue1()<scan2.getValue1()) {
            if (!scan1.next()) break;
         } else if (scan1.getValue1()>scan2.getValue1()) {
            if (!scan2.next()) break;
         } else {
            result+=scan1.getCount()*scan2.getCount();
            if (!scan1.next()) break;
            if (!scan2.next()) break;
         }
      }
      return result;
   } else {
      return 0;
   }
}
//---------------------------------------------------------------------------
void DatabaseBuilder::computeExactStatistics()
   // Compute exact statistics (after loading)
{
   // Open the database again
   Database db;
   if (!db.open(dbFile)) {
      cout << "Unable to open " << dbFile << endl;
      throw;
   }

   // Prepare for appending
   ofstream out(dbFile,ios::in|ios::out|ios::ate|ios::binary);
   if (!out.is_open()) {
      cout << "Unable to write " << dbFile << endl;
      throw;
   }
   unsigned page=out.tellp()/pageSize;

   // Compute the exact 2 statistics
   unsigned exactPS=computeExact2(db,out,page,Database::Order_Predicate_Subject_Object);
   unsigned exactPO=computeExact2(db,out,page,Database::Order_Predicate_Object_Subject);
   unsigned exactSO=computeExact2(db,out,page,Database::Order_Subject_Object_Predicate);

   // Compute the exact 1 statistics
   unsigned exactS=computeExact1(db,out,page,Database::Order_Subject_Predicate_Object,Database::Order_Subject_Object_Predicate);
   unsigned exactP=computeExact1(db,out,page,Database::Order_Predicate_Subject_Object,Database::Order_Predicate_Object_Subject);
   unsigned exactO=computeExact1(db,out,page,Database::Order_Object_Subject_Predicate,Database::Order_Object_Predicate_Subject);

   // Compute the exact 0 statistics
   unsigned exact0SS=computeExact0(db,Database::Order_Subject_Predicate_Object,Database::Order_Subject_Predicate_Object);
   unsigned exact0SP=computeExact0(db,Database::Order_Subject_Predicate_Object,Database::Order_Predicate_Subject_Object);
   unsigned exact0SO=computeExact0(db,Database::Order_Subject_Predicate_Object,Database::Order_Object_Subject_Predicate);
   unsigned exact0PS=computeExact0(db,Database::Order_Predicate_Subject_Object,Database::Order_Subject_Predicate_Object);
   unsigned exact0PP=computeExact0(db,Database::Order_Predicate_Subject_Object,Database::Order_Predicate_Subject_Object);
   unsigned exact0PO=computeExact0(db,Database::Order_Predicate_Subject_Object,Database::Order_Object_Subject_Predicate);
   unsigned exact0OS=computeExact0(db,Database::Order_Object_Subject_Predicate,Database::Order_Subject_Predicate_Object);
   unsigned exact0OP=computeExact0(db,Database::Order_Object_Subject_Predicate,Database::Order_Predicate_Subject_Object);
   unsigned exact0OO=computeExact0(db,Database::Order_Object_Subject_Predicate,Database::Order_Object_Subject_Predicate);
   cout << exact0SS << " " << exact0SP << " " << exact0SO << " " << exact0PS << " " << exact0PP << " " << exact0PO << " " << exact0OS << " " << exact0OP << " " << exact0OO << std::endl;

   // Update the directory page XXX
//   out.seekp(static_cast<unsigned long long>(directory.statistics[order])*pageSize,ios::beg);
//   out.write(reinterpret_cast<char*>(statisticPage),pageSize);
cout << exactPS << " " << exactPO << " " << exactSO << " " << exactS << " " << exactP << " " << exactO << endl;

   // Close the database
   out.flush();
   out.close();
   db.close();

}
//---------------------------------------------------------------------------
