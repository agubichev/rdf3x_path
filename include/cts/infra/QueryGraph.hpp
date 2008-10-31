#ifndef H_cts_infra_QueryGraph
#define H_cts_infra_QueryGraph
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
#include <vector>
//---------------------------------------------------------------------------
/// A query graph representing a SPARQL query
class QueryGraph
{
   public:
   /// Possible duplicate handling modes
   enum DuplicateHandling { AllDuplicates, CountDuplicates, ReducedDuplicates, NoDuplicates, ShowDuplicates };

   /// A node in the graph
   struct Node {
      /// The values
      unsigned subject,predicate,object;
      /// Which of the three values are constants?
      bool constSubject,constPredicate,constObject;

      /// Is there an implicit join edge to another node?
      bool canJoin(const Node& other) const;
   };
   /// The potential join edges
   struct Edge {
      /// The endpoints
      unsigned from,to;
      /// Common variables
      std::vector<unsigned> common;

      /// Constructor
      Edge(unsigned from,unsigned to,const std::vector<unsigned>& common);
      /// Destructor
      ~Edge();
   };
   /// A value filter
   struct Filter {
      /// The id
      unsigned id;
      /// The valid values. Sorted by id.
      std::vector<unsigned> values;
      /// Negative filter?
      bool exclude;
   };
   /// A (potentially) complex filter. Currently very limited.
   struct ComplexFilter {
      /// The ids
      unsigned id1,id2;
      /// Test for  equal?
      bool equal;
   };
   /// Description of a subquery
   struct SubQuery {
      /// The nodes
      std::vector<Node> nodes;
      /// The edges
      std::vector<Edge> edges;
      /// The filter conditions
      std::vector<Filter> filters;
      /// The complex filter conditions
      std::vector<ComplexFilter> complexFilters;
      /// Optional subqueries
      std::vector<SubQuery> optional;
      /// Union subqueries
      std::vector<std::vector<SubQuery> > unions;
   };
   private:
   /// The query itself
   SubQuery query;
   /// The projection
   std::vector<unsigned> projection;
   /// The duplicate handling
   DuplicateHandling duplicateHandling;
   /// Maximum result size
   unsigned limit;
   /// Is the query known to produce an empty result?
   bool knownEmptyResult;

   QueryGraph(const QueryGraph&);
   void operator=(const QueryGraph&);

   public:
   /// Constructor
   QueryGraph();
   /// Destructor
   ~QueryGraph();

   /// Clear the graph
   void clear();
   /// Construct the edges
   void constructEdges();

   /// Set the duplicate handling mode
   void setDuplicateHandling(DuplicateHandling d) { duplicateHandling=d; }
   /// Get the duplicate handling mode
   DuplicateHandling getDuplicateHandling() const { return duplicateHandling; }
   /// Set the result limit
   void setLimit(unsigned l) { limit=l; }
   /// Get the result limit
   unsigned getLimit() const { return limit; }
   /// Known empty result
   void markAsKnownEmpty() { knownEmptyResult=true; }
   /// Known empty result?
   bool knownEmpty() const { return knownEmptyResult; }

   /// Get the query
   SubQuery& getQuery() { return query; }
   /// Get the query
   const SubQuery& getQuery() const { return query; }

   /// Add an entry to the output projection
   void addProjection(unsigned id) { projection.push_back(id); }
   /// Iterator over the projection
   typedef std::vector<unsigned>::const_iterator projection_iterator;
   /// Iterator over the projection
   projection_iterator projectionBegin() const { return projection.begin(); }
   /// Iterator over the projection
   projection_iterator projectionEnd() const { return projection.end(); }
};
//---------------------------------------------------------------------------
#endif
