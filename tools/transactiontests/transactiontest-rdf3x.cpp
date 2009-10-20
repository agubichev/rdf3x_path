#include "rts/database/Database.hpp"
#include "rts/runtime/DifferentialIndex.hpp"
#include "rts/runtime/BulkOperation.hpp"
#include "infra/osdep/Timestamp.hpp"
#include <iostream>
#include <fstream>
#include <cstdlib>
#include <unistd.h>
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
static void readEntry(ifstream& in,unsigned& id,string& value)
{
   in >> id;
   in.get();
   getline(in,value);

   if ((value.length()>0)&&(value[value.length()-1]=='\n'))
      value.resize(value.length()-1);
}
//---------------------------------------------------------------------------
static void writeURI(ofstream& out,const string& value)
{
   static const char hex[]="0123456789abcdef";

   out << "<";
   for (string::const_iterator iter=value.begin(),limit=value.end();iter!=limit;++iter) {
      char c=*iter;
      if ((c&0xFF)<=' ') {
         out << "%" << hex[c>>4] << hex[c&0x0F];
         continue;
      }
      switch (c) {
         case '<': case '>': case '"': case '{': case '}': case '|': case '^': case '`': case '\\':
         case '%':
            out << "%" << hex[c>>4] << hex[c&0x0F];
            break;
         default: out << c; break;
      }
   }
   out << ">";
}
//---------------------------------------------------------------------------
int main(int argc,char* argv[])
{
   // Check the arguments
   if (argc<4) {
      cout << "usage: " << argv[0] << " <database> <transactionlog> <mode>" << endl;
      return 1;
   }
   enum { InsertMode, TransactionMode } mode;
   if (string(argv[3])=="insert") {
      mode=InsertMode;
   } else if (string(argv[3])=="transaction") {
      mode=TransactionMode;
   } else {
      cout << "unknown execution mode " << argv[3] << endl;
      return 1;
   }

   // Produce an input file
   ifstream in(argv[2]);
   if (!in.is_open()) {
      cout << "unable to open transaction log " << argv[2] << endl;
      return 1;
   }
   unsigned simpleCounts,transactionCounts,initialTransactions,initialTriples=0;
   in >> simpleCounts;
   {
      ofstream out("transactions.tmp");
      for (unsigned index=0;index<simpleCounts;index++) {
         unsigned s,p,o;
         string ss,ps,os;
         readEntry(in,s,ss);
         readEntry(in,p,ps);
         readEntry(in,o,os);
         writeURI(out,ss);
         out << " ";
         writeURI(out,ps);
         out << " ";
         writeURI(out,os);
         out << ".\n";
         ++initialTriples;
      }
      in >> transactionCounts;
      initialTransactions=transactionCounts/2;
      for (unsigned index=0;index<initialTransactions;index++) {
         unsigned tagCount;
         in >> tagCount;
         unsigned s,p,o;
         string ss,ps,os;
         readEntry(in,s,ss);
         readEntry(in,o,os);
         for (unsigned index2=0;index2<tagCount;index2++) {
            readEntry(in,p,ps);
            writeURI(out,ss);
            out << " ";
            writeURI(out,ps);
            out << " ";
            writeURI(out,os);
            out << ".\n";
            ++initialTriples;
         }
      }
      out.flush();
      out.close();
   }

   // Build the initial database
   {
      Timestamp start;
      string command=string("./bin/rdf3xload ")+argv[1]+" transactions.tmp";
      if (system(command.c_str())!=0) {
         cerr << "build failed" << endl;
         remove("transactions.tmp");
         return 1;
      }
      remove("transactions.tmp");
      sync();
      Timestamp stop;

      cout << "bulkload: " << (stop-start) << "ms for " << initialTriples << " triples" << endl;
      cout << "triples per second: " << (static_cast<double>(initialTriples)/(static_cast<double>(stop-start)/1000.0)) << endl;
   }

   // Open the database
   Database db;
   if (!db.open(argv[1],false)) {
      cout << "unable to open database " << argv[1] << endl;
      return 1;
   }

   if (mode==InsertMode) {
      Timestamp start;
      DifferentialIndex diff(db);
      unsigned total=0,inMem=0;
      string empty;
      for (unsigned index=initialTransactions;index<transactionCounts;index++) {
         BulkOperation bulk(diff);
         unsigned tagCount;
         in >> tagCount;
         unsigned s,p,o;
         string ss,ps,os;
         readEntry(in,s,ss);
         readEntry(in,o,os);
         for (unsigned index2=0;index2<tagCount;index2++) {
            readEntry(in,p,ps);
            bulk.insert(ss,ps,os,Type::URI,empty);
            ++total; ++inMem;
         }
         bulk.commit();
         if (inMem>500000) {
            cout << "sync start..." << endl;
            diff.sync();
            cout << "sync done..." << endl;
            inMem=0;
         }
      }
      diff.sync();
      Timestamp stop;
      cout << "incremental load: " << (stop-start) << "ms for " << (transactionCounts-initialTransactions) << " transactions, " << total << " triples" << endl;
      cout << "transactions per second: " << (static_cast<double>(transactionCounts-initialTransactions)/(static_cast<double>(stop-start)/1000.0)) << endl;
      cout << "triples per second: " << (static_cast<double>(total)/(static_cast<double>(stop-start)/1000.0)) << endl;
   } else {
      // Process the remaining transactions
      for (unsigned index=initialTransactions;index<transactionCounts;index++) {
         // XXX
      }
   }
}
//---------------------------------------------------------------------------
