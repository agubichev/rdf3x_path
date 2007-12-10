#ifndef H_rts_database_Database
#define H_rts_database_Database
//---------------------------------------------------------------------------
class BufferManager;
//---------------------------------------------------------------------------
/// Access to the RDF database
class Database
{
   public:
   /// Supported data orders
   enum DataOrder {
      Order_Subject_Predicate_Object=0,Order_Subject_Object_Predicate,Order_Object_Predicate_Subject,
      Order_Object_Subject_Predicate,Order_Predicate_Subject_Object,Order_Predicate_Object_Subkect
   };

   private:
   /// The database buffer
   BufferManager* bufferManager;

   /// Begin of the facts tables in all orderings
   unsigned factStarts[6];
   /// Root of the fact indices in all orderings
   unsigned factIndices[6];
   /// Begin of the string table
   unsigned stringStart;
   /// Begin of the string mapping
   unsigned stringMapping;
   /// Root of the string index
   unsigned stringIndex;

   Database(const Database&);
   void operator=(const Database&);

   public:
   /// Constructor
   Database();
   /// Destructor
   ~Database();

   /// Open a database
   bool open(const char* fileName);
   /// Close the current database
   void close();
};
//---------------------------------------------------------------------------
#endif
