#ifndef H_cts_semana_SemanticAnalysis
#define H_cts_semana_SemanticAnalysis
//---------------------------------------------------------------------------
class Database;
class SPARQLParser;
class QueryGraph;
//---------------------------------------------------------------------------
/// Semantic anaylsis for SPARQL queries. Transforms the parse result into a query graph
class SemanticAnalysis
{
   private:
   /// The database. Used for string and IRI resolution
   Database& db;

   public:
   /// Constructor
   explicit SemanticAnalysis(Database& db);

   /// Perform the transformation
   bool transform(const SPARQLParser& input,QueryGraph& output);
};
//---------------------------------------------------------------------------
#endif
