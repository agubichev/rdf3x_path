#ifndef H_cts_infra_QueryGraph
#define H_cts_infra_QueryGraph
//---------------------------------------------------------------------------
#include <vector>
//---------------------------------------------------------------------------
/// A query graph representing a SPARQL query
class QueryGraph
{
   public:
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
   private:
   /// The nodes
   std::vector<Node> nodes;
   /// The edges
   std::vector<Edge> edges;
   /// The projection
   std::vector<unsigned> projection;

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

   /// Iterator over the projection
   typedef std::vector<unsigned>::const_iterator projection_iterator;
   /// Iterator over the projection
   projection_iterator projectionBegin() const { return projection.begin(); }
   /// Iterator over the projection
   projection_iterator projectionEnd() const { return projection.end(); }
};
//---------------------------------------------------------------------------
#endif
