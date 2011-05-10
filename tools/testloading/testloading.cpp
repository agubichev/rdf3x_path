#include "Sorter.hpp"
#include "TempFile.hpp"
#include "cts/parser/TurtleParser.hpp"
#include "infra/osdep/MemoryMappedFile.hpp"
#include "rts/database/DatabaseBuilder.hpp"
#include <iostream>
#include <cassert>
#include <cstring>
#include <fstream>
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
#define ensure(x) if (!(x)) assert(false)
//---------------------------------------------------------------------------
int main(int argc,char* argv[])
{
   // Check the arguments
   if (argc<2) {
      cerr <<  "usage: " << argv[0] << " <tmp file> " << endl;
      return 1;
   }

   // Parse the input

   {
   ofstream out("yago_aftertest.txt");

   MemoryMappedFile in;
   const char* iter,*limit;

    ensure(in.open(argv[1])); iter=in.getBegin(); limit=in.getEnd();
   while (iter != limit){
   	uint64_t i1, i2, i3;
   	iter = TempFile::readId(iter, i1);
   	iter = TempFile::readId(iter, i2);
   	iter = TempFile::readId(iter, i3);
   	unsigned node = i1;

   	iter = TempFile::readId(iter, i1);
   	iter = TempFile::readId(iter, i2);
   	iter = TempFile::readId(iter, i3);
   	unsigned dir = i2;

   	iter = TempFile::readId(iter, i1);
   	iter = TempFile::readId(iter, i2);
   	iter = TempFile::readId(iter, i3);
   	unsigned selectivity = i3;

   	iter = TempFile::readId(iter, i1);
   	iter = TempFile::readId(iter, i2);
   	iter = TempFile::readId(iter, i3);

   	out<<node<<" "<<dir<<" "<<selectivity<<endl;
   }
   }
   char a = 1000000;
   cerr<<static_cast<unsigned>(a)<<endl;

   cout << "Done." << endl;
}
//---------------------------------------------------------------------------
