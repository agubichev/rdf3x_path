#include "rts/database/Database.hpp"
#include "rts/buffer/BufferManager.hpp"
#define private public
#include "rts/segment/PathStatisticsSegment.hpp"
#undef private
#include "rts/segment/StatisticsSegment.hpp"
#include "rts/segment/ExactStatisticsSegment.hpp"
#include "cts/infra/QueryGraph.hpp"
#include "cts/parser/SPARQLLexer.hpp"
#include "cts/parser/SPARQLParser.hpp"
#include "cts/semana/SemanticAnalysis.hpp"
#include "rts/runtime/Runtime.hpp"
#include "rts/operator/MergeJoin.hpp"
#include "rts/operator/IndexScan.hpp"
#include "rts/operator/AggregatedIndexScan.hpp"
#include "rts/operator/FullyAggregatedIndexScan.hpp"
#include "infra/osdep/Timestamp.hpp"
#include <iostream>
#include <fstream>
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
static string readInput(istream& in)
   // Read the input query
{
   string result;
   while (true) {
      string s;
      getline(in,s);
      if (!in.good())
         break;
      result+=s;
      result+='\n';
   }
   return result;
}
//---------------------------------------------------------------------------
static void normalizePattern(Database::DataOrder order,unsigned& c1,unsigned& c2,unsigned& c3)
    // Extract subject/predicate/object order
{
   unsigned s=~0u,p=~0u,o=~0u;
   switch (order) {
      case Database::Order_Subject_Predicate_Object: s=c1; p=c2; o=c3; break;
      case Database::Order_Subject_Object_Predicate: s=c1; o=c2; p=c3; break;
      case Database::Order_Object_Predicate_Subject: o=c1; p=c2; s=c3; break;
      case Database::Order_Object_Subject_Predicate: o=c1; s=c2; p=c3; break;
      case Database::Order_Predicate_Subject_Object: p=c1; s=c2; o=c3; break;
      case Database::Order_Predicate_Object_Subject: p=c1; o=c2; s=c3; break;
   }
   c1=s; c2=p; c3=o;
}
//---------------------------------------------------------------------------
static void denormalizePattern(Database::DataOrder order,unsigned& s,unsigned& p,unsigned& o)
    // Extract data order
{
   unsigned c1=~0u,c2=~0u,c3=~0u;
   switch (order) {
      case Database::Order_Subject_Predicate_Object: c1=s; c2=p; c3=o; break;
      case Database::Order_Subject_Object_Predicate: c1=s; c2=o; c3=p; break;
      case Database::Order_Object_Predicate_Subject: c1=o; c2=p; c3=s; break;
      case Database::Order_Object_Subject_Predicate: c1=o; c2=s; c3=p; break;
      case Database::Order_Predicate_Subject_Object: c1=p; c2=s; c3=o; break;
      case Database::Order_Predicate_Object_Subject: c1=p; c2=o; c3=s; break;
   }
   s=c1; p=c2; o=c3;
}
//---------------------------------------------------------------------------
static void maximizePrefix(Database::DataOrder& order,unsigned& c1,unsigned& c2,unsigned& c3)
   // Reshuffle values to maximize the constant prefix
{
   // Reconstruct the original assignments first
   unsigned s=c1,p=c2,o=c3;
   normalizePattern(order,s,p,o);

   // Now find the maximum prefix
   if (~s) {
      if ((~p)||(!~o)) {
         order=Database::Order_Subject_Predicate_Object;
         c1=s; c2=p; c3=o;
      } else {
         order=Database::Order_Subject_Object_Predicate;
         c1=s; c2=o; c3=p;
      }
   } else if (~p) {
      order=Database::Order_Predicate_Object_Subject;
      c1=p; c2=o; c3=s;
   } else if (~o) {
      order=Database::Order_Object_Predicate_Subject;
      c1=o; c2=p; c3=s;
   } else {
      order=Database::Order_Subject_Predicate_Object;
      c1=s; c2=p; c3=o;
   }
}
//---------------------------------------------------------------------------
static double buildMaxSel(double sel,unsigned hits1,unsigned card1,unsigned card2)
   // Update the maximum selectivity
{
   double s1=static_cast<double>(hits1)/(static_cast<double>(card1)*card2);
   return max(sel,s1);
}
//---------------------------------------------------------------------------
static double joinSelectivityOldMethod(Database& db,const QueryGraph::Node& l,const QueryGraph::Node& r)
   // Build the information about a join
{
   // Extract patterns
   Database::DataOrder lo=Database::Order_Subject_Predicate_Object,ro=Database::Order_Subject_Predicate_Object;
   unsigned l1=l.constSubject?l.subject:~0u,l2=l.constPredicate?l.predicate:~0u,l3=l.constObject?l.object:~0u;
   unsigned r1=l.constSubject?r.subject:~0u,r2=r.constPredicate?r.predicate:~0u,r3=r.constObject?r.object:~0u;
   maximizePrefix(lo,l1,l2,l3);
   maximizePrefix(ro,r1,r2,r3);
   unsigned lv1=(!l.constSubject)?l.subject:~0u,lv2=(!l.constPredicate)?l.predicate:~0u,lv3=(!l.constObject)?l.object:~0u;
   unsigned rv1=(!l.constSubject)?r.subject:~0u,rv2=(!r.constPredicate)?r.predicate:~0u,rv3=(!r.constObject)?r.object:~0u;
   denormalizePattern(lo,lv1,lv2,lv3);
   denormalizePattern(ro,rv1,rv2,rv3);

   // Query the statistics
   StatisticsSegment::Bucket ls,rs;
   if (~l3) {
      db.getStatistics(lo).lookup(l1,l2,l3,ls);
   } else if (~l2) {
      db.getStatistics(lo).lookup(l1,l2,ls);
   } else if (~l1) {
      db.getStatistics(lo).lookup(l1,ls);
   } else {
      db.getStatistics(lo).lookup(ls);
   }
   if (~r3) {
      db.getStatistics(ro).lookup(r1,r2,r3,rs);
   } else if (~r2) {
      db.getStatistics(ro).lookup(r1,r2,rs);
   } else if (~r1) {
      db.getStatistics(ro).lookup(r1,rs);
   } else {
      db.getStatistics(ro).lookup(rs);
   }
   if (!ls.card) ls.card=1;
   if (!rs.card) rs.card=1;

   vector<unsigned> common;
   if ((l.subject==r.subject)&&(!l.constSubject)&&(!r.constSubject)) common.push_back(l.subject);
   if ((l.subject==r.predicate)&&(!l.constSubject)&&(!r.constPredicate)) common.push_back(l.subject);
   if ((l.subject==r.object)&&(!l.constSubject)&&(!r.constObject)) common.push_back(l.subject);
   if ((l.predicate==r.subject)&&(!l.constPredicate)&&(!r.constSubject)) common.push_back(l.predicate);
   if ((l.predicate==r.predicate)&&(!l.constPredicate)&&(!r.constPredicate)) common.push_back(l.predicate);
   if ((l.predicate==r.object)&&(!l.constPredicate)&&(!r.constObject)) common.push_back(l.predicate);
   if ((l.object==r.subject)&&(!l.constObject)&&(!r.constSubject)) common.push_back(l.object);
   if ((l.object==r.predicate)&&(!l.constObject)&&(!r.constPredicate)) common.push_back(l.object);
   if ((l.object==r.object)&&(!l.constObject)&&(!r.constObject)) common.push_back(l.object);

   // Estimate the selectivity
   double lsel=(1.0/(ls.card*rs.card)),rsel=lsel;
   for (vector<unsigned>::const_iterator iter=common.begin(),limit=common.end();iter!=limit;++iter) {
      unsigned v=(*iter);
      if (v==lv1) {
         if ((v==r.subject)&&(!r.constSubject))
            lsel=buildMaxSel(lsel,ls.val1S,ls.card,rs.card);
         if ((v==r.predicate)&&(!r.constPredicate))
            lsel=buildMaxSel(lsel,ls.val1P,ls.card,rs.card);
         if ((v==r.object)&&(!r.constObject))
            lsel=buildMaxSel(lsel,ls.val1O,ls.card,rs.card);
      }
      if (v==lv2) {
         if ((v==r.subject)&&(!r.constSubject))
            lsel=buildMaxSel(lsel,ls.val2S,ls.card,rs.card);
         if ((v==r.predicate)&&(!r.constPredicate))
            lsel=buildMaxSel(lsel,ls.val2P,ls.card,rs.card);
         if ((v==r.object)&&(!r.constObject))
            lsel=buildMaxSel(lsel,ls.val2O,ls.card,rs.card);
      }
      if (v==lv3) {
         if ((v==r.subject)&&(!r.constSubject))
            lsel=buildMaxSel(lsel,ls.val3S,ls.card,rs.card);
         if ((v==r.predicate)&&(!r.constPredicate))
            lsel=buildMaxSel(lsel,ls.val3P,ls.card,rs.card);
         if ((v==r.object)&&(!r.constObject))
            lsel=buildMaxSel(lsel,ls.val3O,ls.card,rs.card);
      }
      if (v==rv1) {
         if ((v==l.subject)&&(!l.constSubject))
            rsel=buildMaxSel(rsel,rs.val1S,rs.card,ls.card);
         if ((v==l.predicate)&&(!l.constPredicate))
            rsel=buildMaxSel(rsel,rs.val1P,rs.card,ls.card);
         if ((v==l.object)&&(!l.constObject))
            rsel=buildMaxSel(rsel,ls.val1O,ls.card,ls.card);
      }
      if (v==rv2) {
         if ((v==l.subject)&&(!l.constSubject))
            rsel=buildMaxSel(rsel,rs.val2S,rs.card,ls.card);
         if ((v==l.predicate)&&(!l.constPredicate))
            rsel=buildMaxSel(rsel,rs.val2P,rs.card,ls.card);
         if ((v==l.object)&&(!l.constObject))
            rsel=buildMaxSel(rsel,rs.val2O,rs.card,ls.card);
      }
      if (v==rv3) {
         if ((v==l.subject)&&(!l.constSubject))
            rsel=buildMaxSel(rsel,rs.val3S,rs.card,ls.card);
         if ((v==l.predicate)&&(!l.constPredicate))
            rsel=buildMaxSel(rsel,rs.val3P,rs.card,ls.card);
         if ((v==l.object)&&(!l.constObject))
            rsel=buildMaxSel(rsel,rs.val3O,rs.card,ls.card);
      }
   }
   return (lsel+rsel)/2.0;
}
//---------------------------------------------------------------------------
static double joinSelectivityNewMethod(Database& db,const QueryGraph::Node& l,const QueryGraph::Node& r)
   // Build the information about a join
{
   return db.getExactStatistics().getJoinSelectivity(l.constSubject,l.subject,l.constPredicate,l.predicate,l.constObject,l.object,r.constSubject,r.subject,r.constPredicate,r.predicate,r.constObject,r.object);
}
//---------------------------------------------------------------------------
static Operator* buildScan(Database& db,const QueryGraph::Node& n,Register* regs,unsigned joinVar,Register*& joinReg)
   // Build a suitable scan
{
   regs[0].reset(); regs[1].reset(); regs[2].reset();
   if (n.constSubject) regs[0].value=n.subject;
   if (n.constPredicate) regs[1].value=n.predicate;
   if (n.constObject) regs[2].value=n.object;

   if (n.constSubject) {
      if (n.constPredicate) {
         if (n.constObject) {
            return 0; // cannot happen!
         } else {
            joinReg=regs+2;
            return IndexScan::create(db,Database::Order_Subject_Predicate_Object,regs+0,n.constSubject,regs+1,n.constPredicate,regs+2,n.constObject);
         }
      } else {
         if (n.constObject) {
            joinReg=regs+1;
            return IndexScan::create(db,Database::Order_Subject_Object_Predicate,regs+0,n.constSubject,regs+1,n.constPredicate,regs+2,n.constObject);
         } else if (joinVar==n.predicate) {
            joinReg=regs+1;
            return AggregatedIndexScan::create(db,Database::Order_Subject_Predicate_Object,regs+0,n.constSubject,regs+1,n.constPredicate,0,false);
         } else {
            joinReg=regs+2;
            return AggregatedIndexScan::create(db,Database::Order_Subject_Object_Predicate,regs+0,n.constSubject,0,false,regs+2,n.constObject);
         }
      }
   } else {
      if (n.constPredicate) {
         if (n.constObject) {
            joinReg=regs+0;
            return IndexScan::create(db,Database::Order_Predicate_Object_Subject,regs+0,n.constSubject,regs+1,n.constPredicate,regs+2,n.constObject);
         } else if (joinVar==n.subject) {
            joinReg=regs+0;
            return AggregatedIndexScan::create(db,Database::Order_Predicate_Subject_Object,regs+0,n.constSubject,regs+1,n.constPredicate,0,false);
         } else {
            joinReg=regs+2;
            return AggregatedIndexScan::create(db,Database::Order_Predicate_Object_Subject,0,false,regs+1,n.constPredicate,regs+2,n.constObject);
         }
      } else {
         if (n.constObject) {
            if (joinVar==n.subject) {
               joinReg=regs+0;
               return AggregatedIndexScan::create(db,Database::Order_Object_Subject_Predicate,regs+0,n.constSubject,0,false,regs+2,n.constObject);
            } else {
               joinReg=regs+1;
               return AggregatedIndexScan::create(db,Database::Order_Object_Predicate_Subject,0,false,regs+1,n.constPredicate,regs+2,n.constObject);
            }
         } else {
            if (joinVar==n.subject) {
               joinReg=regs+0;
               return FullyAggregatedIndexScan::create(db,Database::Order_Subject_Predicate_Object,regs+0,false,0,false,0,false);
            } else if (joinVar==n.predicate) {
               joinReg=regs+1;
               return FullyAggregatedIndexScan::create(db,Database::Order_Predicate_Subject_Object,0,false,regs+1,false,0,false);
            } else {
               joinReg=regs+2;
               return FullyAggregatedIndexScan::create(db,Database::Order_Object_Subject_Predicate,0,false,0,false,regs+2,false);
            }
         }
      }
   }
}
//---------------------------------------------------------------------------
static double joinSelectivityExact(Database& db,const QueryGraph::Node& l,const QueryGraph::Node& r)
   // Compute the exact join selectivity
{
   // Compute the common variables
   vector<unsigned> common;
   if ((l.subject==r.subject)&&(!l.constSubject)&&(!r.constSubject)) common.push_back(l.subject);
   if ((l.subject==r.predicate)&&(!l.constSubject)&&(!r.constPredicate)) common.push_back(l.subject);
   if ((l.subject==r.object)&&(!l.constSubject)&&(!r.constObject)) common.push_back(l.subject);
   if ((l.predicate==r.subject)&&(!l.constPredicate)&&(!r.constSubject)) common.push_back(l.predicate);
   if ((l.predicate==r.predicate)&&(!l.constPredicate)&&(!r.constPredicate)) common.push_back(l.predicate);
   if ((l.predicate==r.object)&&(!l.constPredicate)&&(!r.constObject)) common.push_back(l.predicate);
   if ((l.object==r.subject)&&(!l.constObject)&&(!r.constSubject)) common.push_back(l.object);
   if ((l.object==r.predicate)&&(!l.constObject)&&(!r.constPredicate)) common.push_back(l.object);
   if ((l.object==r.object)&&(!l.constObject)&&(!r.constObject)) common.push_back(l.object);
   if (common.size()!=1) {
      cout << "only joins with one common variable currently implemented!";
      return 1;
   }
   unsigned joinVar=common[0];

   // Build the scan
   Register regs[6];
   Register* joinReg1,*joinReg2;
   Operator* op1=buildScan(db,l,regs+0,joinVar,joinReg1);
   Operator* op2=buildScan(db,r,regs+3,joinVar,joinReg2);

   // Count
   unsigned leftSize=0,rightSize=0;
   for (unsigned index=op1->first();index;index=op1->next())
      leftSize+=index;
   for (unsigned index=op2->first();index;index=op2->next())
      rightSize+=index;
   double crossSize=static_cast<double>(leftSize)*static_cast<double>(rightSize);
   delete op1;
   delete op2;
   op1=buildScan(db,l,regs+0,joinVar,joinReg1);
   op2=buildScan(db,r,regs+3,joinVar,joinReg2);

   // And join
   vector<Register*> tail;
   MergeJoin* join=new MergeJoin(op1,joinReg1,tail,op2,joinReg2,tail);
   unsigned resultSize=0;
   for (unsigned index=join->first();index;index=join->next())
      resultSize+=index;
   delete join;

   // Return the selectivity
   return static_cast<double>(resultSize)/crossSize;
}
//---------------------------------------------------------------------------
static double abs(double x) { return (x<0)?-x:x; }
//---------------------------------------------------------------------------
static unsigned long long hrclock()
{
   timespec t;
   clock_gettime(CLOCK_PROCESS_CPUTIME_ID,&t);
   return static_cast<unsigned long long>(t.tv_sec)*1000000+(t.tv_nsec/1000);
}
//---------------------------------------------------------------------------
static void computeJoinSelectivity(Database& db,const QueryGraph::Node& a,const QueryGraph::Node& b,vector<double>& errorsOld,vector<double>& errorsNew,unsigned& timeOld,unsigned& timeNew)
   // Compute the join selectivity between two nodes
{
   unsigned long long t1=hrclock();
   double sel1=joinSelectivityOldMethod(db,a,b);
   unsigned long long t2=hrclock();
   double sel2=joinSelectivityNewMethod(db,a,b);
   unsigned long long t3=hrclock();
   double sel3=joinSelectivityExact(db,a,b);
   double relErr1=(abs(sel1-sel3)/sel3);
   double relErr2=(abs(sel2-sel3)/sel3);

#if 1
   cout << "old estimation: " << sel1 << endl;
   cout << "new estimation: " << sel2 << endl;
   cout << "exact selecitivity: " << sel3 << endl;
   cout << "errors: " << relErr1 << " " << relErr2 << endl;
   cout << (t2-t1) << " " << (t3-t2) << endl;
#endif
   errorsOld.push_back(relErr1);
   errorsNew.push_back(relErr2);
   timeOld+=t2-t1;
   timeNew+=t3-t2;
}
//---------------------------------------------------------------------------
int main(int argc,char* argv[])
{
   if (argc<2) {
      cout << "usage: " << argv[0] << " <rdfstore> [queries]" << endl;
      return 1;
   }

   // Open the database
   Database db;
   if (!db.open(argv[1])) {
      cout << "unable to open " << argv[1] << endl;
      return 1;
   }

   // Find the start of the exact statistics
   unsigned statisticsStart=db.getPathStatistics(true).statisticsPage+1;
   unsigned totalPages=db.getPathStatistics(true).bufferManager.getPageCount();
   unsigned long long statisticsBytes=static_cast<unsigned long long>(totalPages-statisticsStart)*BufferManager::pageSize;

   cout << "using " << (totalPages-statisticsStart) << " pages out of " << totalPages << " for statistics (" << (100.0*(totalPages-statisticsStart)/totalPages) << "%)" << endl;
   cout << "this consumes " << (static_cast<double>(statisticsBytes*10/1024/1024/1024)/10.0) << " GB of space" << endl;

   // Check all queries
   vector<double> errorsNew,errorsOld;
   unsigned timeNew=0,timeOld=0;
   for (int index=2;index<argc;index++) {
      // Read the query
      ifstream in(argv[index]);
      if (!in) {
         cout << "unable to open " << argv[index] << endl;
         continue;
      }
      string query=readInput(in);
      // Build the query graph
      QueryGraph queryGraph;
      {
         // Parse the query
         SPARQLLexer lexer(query);
         SPARQLParser parser(lexer);
         try {
            parser.parse();
         } catch (const SPARQLParser::ParserException& e) {
            cout << "parse error: " << e.message << endl;
            continue;
         }

         // And perform the semantic anaylsis
         SemanticAnalysis semana(db);
         semana.transform(parser,queryGraph);
         if (queryGraph.knownEmpty()) {
            cout << "<empty result>" << endl;
            continue;
         }
      }
      // Check all join selectivities
      for (vector<QueryGraph::Edge>::const_iterator iter=queryGraph.getQuery().edges.begin(),limit=queryGraph.getQuery().edges.end();iter!=limit;++iter) {
         const QueryGraph::Node& from=queryGraph.getQuery().nodes[(*iter).from];
         const QueryGraph::Node& to=queryGraph.getQuery().nodes[(*iter).to];
         computeJoinSelectivity(db,from,to,errorsOld,errorsNew,timeOld,timeNew);
      }
   }

   // Produce the output
   sort(errorsNew.begin(),errorsNew.end());
   sort(errorsOld.begin(),errorsOld.end());
   double newMean=0,oldMean=0;
   for (vector<double>::const_iterator iter=errorsNew.begin(),limit=errorsNew.end();iter!=limit;++iter)
      newMean+=*iter;
   newMean=newMean/errorsNew.size();
   for (vector<double>::const_iterator iter=errorsOld.begin(),limit=errorsOld.end();iter!=limit;++iter)
      oldMean+=*iter;
   oldMean=oldMean/errorsOld.size();
   cout << "old errors: " << errorsOld.front() << " " << errorsOld[5*errorsOld.size()/100] << " " << errorsOld[50*errorsOld.size()/100] << " " << errorsOld[95*errorsOld.size()/100] << " " << errorsOld.back() << " " << oldMean << endl;
   cout << "new errors: " << errorsNew.front() << " " << errorsNew[5*errorsNew.size()/100] << " " << errorsNew[50*errorsNew.size()/100] << " " << errorsNew[95*errorsNew.size()/100] << " " << errorsNew.back() << " " << newMean << endl;
   cout << "durations: " << (static_cast<double>(timeOld)/errorsNew.size()) << " " << (static_cast<double>(timeNew)/errorsNew.size()) << endl;
}
//---------------------------------------------------------------------------
