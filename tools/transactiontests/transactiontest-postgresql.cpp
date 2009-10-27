#include "infra/osdep/Timestamp.hpp"
#include <pqxx/connection>
#include <pqxx/nontransaction>
#include <pqxx/transaction>
#include <iostream>
#include <fstream>
#include <sstream>
#include <cstdlib>
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

   out << "\"";
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
   out << "\"";
}
//---------------------------------------------------------------------------
static void execV(pqxx::nontransaction& trans,const char* sql)
   // Execute a statement and print debug output
{
   cout << sql << endl;
   try {
      trans.exec(sql);
   } catch (const exception &e) {
      cerr << e.what() << endl;
      throw;
   }
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
      ofstream out("/tmp/facts.tmp"),out2("/tmp/strings.tmp");
      vector<bool> seen;
      for (unsigned index=0;index<simpleCounts;index++) {
         unsigned s,p,o;
         string ss,ps,os;
         readEntry(in,s,ss);
         readEntry(in,p,ps);
         readEntry(in,o,os);

         unsigned m=max(s,max(p,o));
         if (m>=seen.size())
            seen.resize(m+1024+(seen.size()/8));

         if (!seen[s]) {
            out2 << s << "\t";
            writeURI(out2,ss);
            out2 << "\n";
            seen[s]=true;
         }
         if (!seen[p]) {
            out2 << p << "\t";
            writeURI(out2,ps);
            out2 << "\n";
            seen[p]=true;
         }
         if (!seen[o]) {
            out2 << o << "\t";
            writeURI(out2,os);
            out2 << "\n";
            seen[o]=true;
         }

         out << s << "\t" << p << "\t" << o << "\n";
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

            unsigned m=max(s,max(p,o));
            if (m>=seen.size())
               seen.resize(m+1024+(seen.size()/8));

            if (!seen[s]) {
               out2 << s << "\t";
               writeURI(out2,ss);
               out2 << "\n";
               seen[s]=true;
            }
            if (!seen[p]) {
               out2 << p << "\t";
               writeURI(out2,ps);
               out2 << "\n";
               seen[p]=true;
            }
            if (!seen[o]) {
               out2 << o << "\t";
               writeURI(out2,os);
               out2 << "\n";
               seen[o]=true;
            }

            out << s << "\t" << p << "\t" << o << "\n";
            ++initialTriples;
         }
      }
      out.flush();
      out.close();
      out2.flush();
      out2.close();
   }

   // Build the initial database
   {
      Timestamp start;
      {
         pqxx::connection con;
         pqxx::nontransaction trans(con);
         execV(trans,"drop schema if exists transactiontest cascade;");
         execV(trans,"create schema transactiontest;");
         execV(trans,"create table transactiontest.facts(subject int not null, predicate int not null, object int not null);");
         execV(trans,"copy transactiontest.facts from '/tmp/facts.tmp';");
         remove("/tmp/facts.tmp");
         execV(trans,"create index facts_osp on transactiontest.facts (object, subject, predicate);");
         // execV(trans,"create index facts_pso on transactiontest.facts (predicate, subject, object);");
         execV(trans,"create index facts_pos on transactiontest.facts (predicate, object, subject);");
         execV(trans,"create table transactiontest.strings(id int not null primary key, value varchar(16384) not null);");
         execV(trans,"copy transactiontest.strings from '/tmp/strings.tmp';");
         remove("/tmp/strings.tmp");
      }
      sync();
      Timestamp stop;

      cout << "bulkload: " << (stop-start) << "ms for " << initialTriples << " triples" << endl;
      cout << "triples per second: " << (static_cast<double>(initialTriples)/(static_cast<double>(stop-start)/1000.0)) << endl;
   }

   // Open the database
   if (mode==InsertMode) {
      pqxx::connection con;
      Timestamp start;
      unsigned total=0;
      pqxx::transaction<> trans(con);
      for (unsigned index=initialTransactions;index<transactionCounts;index++) {
         unsigned tagCount;
         in >> tagCount;
         unsigned s,p,o;
         string ss,ps,os;
         readEntry(in,s,ss);
         readEntry(in,o,os);

         stringstream sql;
         sql << "insert into transactiontest.facts values ";
         for (unsigned index2=0;index2<tagCount;index2++) {
            if (index2) sql << ",";
            sql << "(" << s << "," << p << "," << o << ")";
            ++total;;
         }
         sql << ";";

         trans.exec(sql.str());
      }
      trans.commit();
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
