#include "cts/infra/QueryGraph.hpp"
#include <set>
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
bool QueryGraph::Node::canJoin(const Node& other) const
   // Is there an implicit join edge to another node?
{
   // Extract variables
   unsigned v11=0,v12=0,v13=0;
   if (!constSubject) v11=subject+1;
   if (!constPredicate) v12=predicate+1;
   if (!constObject) v13=object+1;
   unsigned v21=0,v22=0,v23=0;
   if (!other.constSubject) v21=other.subject+1;
   if (!other.constPredicate) v22=other.predicate+1;
   if (!other.constObject) v23=other.object+1;

   // Do they have a variable in common?
   bool canJoin=false;
   if (v11&&v21&&(v11==v21)) canJoin=true;
   if (v11&&v22&&(v11==v22)) canJoin=true;
   if (v11&&v23&&(v11==v23)) canJoin=true;
   if (v12&&v21&&(v12==v21)) canJoin=true;
   if (v12&&v22&&(v12==v22)) canJoin=true;
   if (v12&&v23&&(v12==v23)) canJoin=true;
   if (v13&&v21&&(v13==v21)) canJoin=true;
   if (v13&&v22&&(v13==v22)) canJoin=true;
      if (v13&&v23&&(v13==v23)) canJoin=true;

   return canJoin;
}
//---------------------------------------------------------------------------
QueryGraph::QueryGraph()
   : duplicateHandling(AllDuplicates)
   // Constructor
{
}
//---------------------------------------------------------------------------
QueryGraph::~QueryGraph()
   // Destructor
{
}
//---------------------------------------------------------------------------
void QueryGraph::clear()
   // Clear the graph
{
   nodes.clear();
   edges.clear();
   projection.clear();
   duplicateHandling=AllDuplicates;
}
//---------------------------------------------------------------------------
void QueryGraph::addNode(const Node& node)
   // Add a node
{
   nodes.push_back(node);
}
//---------------------------------------------------------------------------
void QueryGraph::constructEdges()
   // Construct the edges
{
   edges.clear();
   for (vector<Node>::const_iterator iter=nodes.begin(),limit=nodes.end();iter!=limit;++iter) {
      const Node& n1=(*iter);
      for (vector<Node>::const_iterator iter2=iter+1;iter2!=limit;++iter2) {
         const Node& n2=(*iter2);
         // Store an edge if they can be joined
         if (n1.canJoin(n2)) {
            Edge e;
            e.from=&n1; e.to=&n2;
            edges.push_back(e);
         }
      }
   }
}
//---------------------------------------------------------------------------
void QueryGraph::addFilter(const Filter& filter)
   // Add a filter condition
{
   // Does a filter on the same variable already exist?
   for (vector<Filter>::iterator iter=filters.begin(),limit=filters.end();iter!=limit;++iter) {
      if ((*iter).id==filter.id) {
         // Yes, intersect the two filters
         set<unsigned> oldValues;
         oldValues.insert((*iter).values.begin(),(*iter).values.end());
         (*iter).values.clear();
         for (vector<unsigned>::const_iterator iter2=filter.values.begin(),limit2=filter.values.end();iter2!=limit2;++iter2)
            if (oldValues.count(*iter2))
               (*iter).values.push_back(*iter2);
         return;
      }
   }

   // No, add it
   filters.push_back(filter);
}
//---------------------------------------------------------------------------
