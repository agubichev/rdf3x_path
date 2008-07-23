#ifndef H_tools_rdf3xload_StringLookup
#define H_tools_rdf3xload_StringLookup
//---------------------------------------------------------------------------
#include <string>
//---------------------------------------------------------------------------
class TempFile;
//---------------------------------------------------------------------------
/// Lookup cache for early string aggregation
class StringLookup {
   private:
   /// The hash table size
   static const unsigned lookupSize = 1009433;

   /// Strings seen so far
   std::string* strings;
   /// Ids for the strings
   uint64_t* ids;
   /// The next IDs
   uint64_t nextPredicate,nextNonPredicate;

   public:
   /// Constructor
   StringLookup();
   /// Destructor
   ~StringLookup();

   /// Lookup a predicate
   unsigned lookupPredicate(TempFile& stringsFile,const std::string& predicate);
   /// Lookup a value
   unsigned lookupValue(TempFile& stringsFile,const std::string& value);
};
//---------------------------------------------------------------------------
#endif
