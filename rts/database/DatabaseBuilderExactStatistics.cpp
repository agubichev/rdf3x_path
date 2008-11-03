#include "rts/database/DatabaseBuilder.hpp"
#include "rts/database/Database.hpp"
#include "rts/segment/FactsSegment.hpp"
#include "rts/runtime/Runtime.hpp"
#include "rts/operator/FullyAggregatedIndexScan.hpp"
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
      scanS->addMergeHint(&mergeValue,&valueP);
      scanS->addMergeHint(&mergeValue,&valueO);

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
static unsigned computeExact1(Database& /*db*/,ofstream& /*out*/,unsigned& /*page*/,Database::DataOrder /*order*/)
   // Compute the exact statistics for patterns with one constant
{
   return 0;
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
   unsigned exactS=computeExact1(db,out,page,Database::Order_Subject_Predicate_Object);
   unsigned exactP=computeExact1(db,out,page,Database::Order_Predicate_Subject_Object);
   unsigned exactO=computeExact1(db,out,page,Database::Order_Object_Predicate_Subject);

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
