#ifndef H_rts_database_DatabaseBuilder
#define H_rts_database_DatabaseBuilder
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
#include <fstream>
//---------------------------------------------------------------------------
/// Builds a new RDF database from scratch
class DatabaseBuilder
{
   public:
   /// The page size
   static const unsigned pageSize = 16384;

   /// A facts reader
   class FactsReader
   {
      public:
      /// Constructor
      FactsReader();
      /// Destructor
      virtual ~FactsReader();

      /// Load a new fact
      virtual bool next(unsigned& v1,unsigned& v2,unsigned& v3) = 0;
      /// Reset the reader
      virtual void reset() = 0;
   };
   /// A strings reader
   class StringsReader
   {
      public:
      /// Constructor
      StringsReader();
      /// Destructor
      virtual ~StringsReader();

      /// Load a new string
      virtual bool next(unsigned& len,const char*& data) = 0;
      /// Remember a string position and hash
      virtual void rememberInfo(unsigned page,unsigned ofs,unsigned hash) = 0;
   };
   /// A string info reader (reads previously remembered data)
   class StringInfoReader
   {
      public:
      /// Constructor
      StringInfoReader();
      /// Destructor
      virtual ~StringInfoReader();

      /// Load a new data item
      virtual bool next(unsigned& v1,unsigned& v2) = 0;
   };

   private:
   /// The database directory
   struct Directory
   {
      /// Statistics for a facts table
      struct FactsStatistics {
         /// The number of pages
         unsigned pages;
         /// The number of aggregated pages
         unsigned aggregatedPages;
         /// The number of level 1 groups
         unsigned groups1;
         /// The number of level 2 groups
         unsigned groups2;
         /// The number of tuples
         unsigned cardinality;
      };
      /// Begin of the facts tables in all orderings
      unsigned factStarts[6];
      /// Root of the fact indices in all orderings
      unsigned factIndices[6];
      /// Begin of the aggregated facts tables in all orderings
      unsigned aggregatedFactStarts[6];
      /// Root of the aggregatedfact indices in all orderings
      unsigned aggregatedFactIndices[6];
      /// Begin of the fully aggregated facts tables in all orderings
      unsigned fullyAggregatedFactStarts[3];
      /// Root of the fully aggregated facts tables in all orderings
      unsigned fullyAggregatedFactIndices[3];
      /// The statistics
      FactsStatistics factStatistics[6];
      /// Begin of the string table
      unsigned stringStart;
      /// Begin of the string mapping
      unsigned stringMapping;
      /// Root of the string index
      unsigned stringIndex;
      /// Pages with statistics
      unsigned statistics[6];
      /// Pages with path statistics
      unsigned pathStatistics[2];
   };

   /// The database
   std::ofstream out;
   /// The file name
   const char* dbFile;
   /// The current page number
   unsigned page;
   /// The directory
   Directory directory;

   /// Load the triples into the database
   void loadFullFacts(unsigned order,FactsReader& reader);
   /// Load the triples aggregated into the database
   void loadAggregatedFacts(unsigned order,FactsReader& reader);
   /// Load the triples fully aggregated into the database
   void loadFullyAggregatedFacts(unsigned order,FactsReader& reader);

   DatabaseBuilder(const DatabaseBuilder&);
   void operator=(const DatabaseBuilder&);

   public:
   /// Constructor
   DatabaseBuilder(const char* fileName);
   /// Destructor
   ~DatabaseBuilder();

   /// Loads the facts in a given order
   void loadFacts(unsigned order,FactsReader& reader);
   /// Load the raw strings (must be in id order, ids 0,1,2,...)
   void loadStrings(StringsReader& reader);
   /// Load the strings mappings (must be in id order, ids 0,1,2,...)
   void loadStringMappings(StringInfoReader& reader);
   /// Load the hash->page mappings (must be in hash order)
   void loadStringHashes(StringInfoReader& reader);
   /// Finish the load phase, write the directory
   void finishLoading();

   /// Compute specific statistics (after loading)
   void computeStatistics(unsigned order);
   /// Compute statistics about frequent paths (after loading)
   void computePathStatistics();
   /// Compute the exact statistics (after loading)
   void computeExactStatistics(const char* tempFile);
};
//---------------------------------------------------------------------------
#endif
