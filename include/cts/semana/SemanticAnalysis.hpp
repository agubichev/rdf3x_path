#ifndef H_cts_semana_SemanticAnalysis
#define H_cts_semana_SemanticAnalysis
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
