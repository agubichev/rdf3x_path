#include "StringLookup.hpp"
#include "TempFile.hpp"
#include "infra/util/Hash.hpp"
#include <iostream>
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
static const unsigned maxStringLen = 16364;
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

   // Check the len
   if (predicate.size()>maxStringLen) {
      cerr << "error while processing '" << predicate << "'" << endl << "strings larger than " << maxStringLen << " currently not supported!" << endl;
      throw; // XXX could be supported relatively easily
   }

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

   // Check the len
   if (value.size()>maxStringLen) {
      cerr << "error while processing '" << value << "'" << endl << "strings larger than " << maxStringLen << " currently not supported!" << endl;
      throw; // XXX could be supported relatively easily
   }

   // And write to file
   stringFile.writeString(value.size(),value.c_str());
   stringFile.writeId(id);

   return id;
}
//---------------------------------------------------------------------------
