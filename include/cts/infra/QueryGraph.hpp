#ifndef H_cts_infra_QueryGraph
#define H_cts_infra_QueryGraph
//---------------------------------------------------------------------------
#include <vector>
//---------------------------------------------------------------------------
/// A query graph representing a SPARQL query
class QueryGraph
{
   public:
   /// Possible duplicate handling modes
   enum DuplicateHandling { AllDuplicates, CountDuplicates, ReducedDuplicates, NoDuplicates };

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
      const Node* from,*to;
   };
   /// A value filter
   struct Filter {
      /// The id
      unsigned id;
      /// The valid values. Sorted by id.
      std::vector<unsigned> values;
   };
   private:
   /// The nodes
   std::vector<Node> nodes;
   /// The edges
   std::vector<Edge> edges;
   /// The filter conditions
   std::vector<Filter> filters;
   /// The projection
   std::vector<unsigned> projection;
   /// The duplicate handling
   DuplicateHandling duplicateHandling;

   QueryGraph(const QueryGraph&);
   void operator=(const QueryGraph&);

   public:
   /// Constructor
   QueryGraph();
   /// Destructor
   ~QueryGraph();

   /// Clear the graph
   void clear();
   /// Add a node
   void addNode(const Node& node);
   /// Construct the edges
   void constructEdges();

   /// Set the duplicate handling mode
   void setDuplicateHandling(DuplicateHandling d) { duplicateHandling=d; }
   /// Get the duplicate handling mode
   DuplicateHandling getDuplicateHandling() const { return duplicateHandling; }
   /// Add a filter condition
   void addFilter(const Filter& filter);
   /// Add an entry to the output projection
   void addProjection(unsigned id) { projection.push_back(id); }

   /// The number of nodes
   unsigned getNodeCount() const { return nodes.size(); }
   /// Iterator over the nodes
   typedef std::vector<Node>::const_iterator node_iterator;
   /// Iterator over the nodes
   node_iterator nodesBegin() const { return nodes.begin(); }
   /// Iterator over the nodes
   node_iterator nodesEnd() const { return nodes.end(); }

   /// Iterator over the edges
   typedef std::vector<Edge>::const_iterator edge_iterator;
   /// Iterator over the edges
   edge_iterator edgesBegin() const { return edges.begin(); }
   /// Iterator over the edges
   edge_iterator edgesEnd() const { return edges.end(); }

   /// Iterator over the projection
   typedef std::vector<unsigned>::const_iterator projection_iterator;
   /// Iterator over the projection
   projection_iterator projectionBegin() const { return projection.begin(); }
   /// Iterator over the projection
   projection_iterator projectionEnd() const { return projection.end(); }
};
//---------------------------------------------------------------------------
#endif
