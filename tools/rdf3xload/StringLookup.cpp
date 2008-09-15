#include "StringLookup.hpp"
#include "TempFile.hpp"
#include "infra/util/Hash.hpp"
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
StringLookup::StringLookup()
   : strings(new string[lookupSize]),ids(new uint64_t[lookupSize]),nextPredicate(0),nextNonPredicate(0)
   // Constructor
{
   for (unsigned index=0;index<lookupSize;index++)
      ids[index]=~static_cast<uint64_t>(0);
}
//---------------------------------------------------------------------------
StringLookup::~StringLookup()
   // Destructor
{
   delete[] ids;
   delete[] strings;
}
//---------------------------------------------------------------------------
unsigned StringLookup::lookupPredicate(TempFile& stringFile,const string& predicate)
   // Lookup a predicate
{
   // Already known?
   unsigned slot=Hash::hash(predicate)%lookupSize;
   if ((strings[slot]==predicate)&&(!(ids[slot]&1)))
      return ids[slot];

   // No, construct a new id
   strings[slot]=predicate;
   uint64_t id=ids[slot]=((nextPredicate++)<<1);

   // And write to file
   stringFile.writeString(predicate.size(),predicate.c_str());
   stringFile.writeId(id);

   return id;
}
//---------------------------------------------------------------------------
unsigned StringLookup::lookupValue(TempFile& stringFile,const string& value)
   // Lookup a value
{
   // Already known?
   unsigned slot=Hash::hash(value)%lookupSize;
   if ((strings[slot]==value)&&(~ids[slot]))
      return ids[slot];

   // No, construct a new id
   strings[slot]=value;
   uint64_t id=ids[slot]=((nextNonPredicate++)<<1)|1;

   // And write to file
   stringFile.writeString(value.size(),value.c_str());
   stringFile.writeId(id);

   return id;
}
//---------------------------------------------------------------------------
