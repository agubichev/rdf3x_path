#include "cts/semana/SemanticAnalysis.hpp"
#include "cts/parser/SPARQLParser.hpp"
#include "cts/infra/QueryGraph.hpp"
#include "rts/database/Database.hpp"
#include "rts/segment/DictionarySegment.hpp"
//---------------------------------------------------------------------------
SemanticAnalysis::SemanticAnalysis(Database& db)
   : db(db)
   // Constructor
{
}
//---------------------------------------------------------------------------
static bool encode(Database& db,const SPARQLParser::Element& element,unsigned& id,bool& constant)
   // Encode an element for the query graph
{
   switch (element.type) {
      case SPARQLParser::Element::Variable:
         id=element.id;
         constant=false;
         return true;
      case SPARQLParser::Element::String:
      case SPARQLParser::Element::IRI:
         if (db.getDictionary().lookup(element.value,id)) {
            constant=true;
            return true;
         } else return false;
      default: return false; // Error, this should not happen!
   }
}
//---------------------------------------------------------------------------
bool SemanticAnalysis::transform(const SPARQLParser& input,QueryGraph& output)
   // Perform the transformation
{
   output.clear();

   // Encode all patterns
   for (SPARQLParser::pattern_iterator iter=input.patternsBegin(),limit=input.patternsEnd();iter!=limit;++iter) {
      // Encode the entries
      QueryGraph::Node node;
      if ((!encode(db,(*iter).subject,node.subject,node.constSubject))||
          (!encode(db,(*iter).predicate,node.predicate,node.constPredicate))||
          (!encode(db,(*iter).object,node.object,node.constObject))) {
         // A constant could not be resolved. This will produce an empty resut
         output.clear();
         return true;
      }
      output.addNode(node);
   }

   // Compute the edges
   output.constructEdges();

   // Add the projection entry
   for (SPARQLParser::projection_iterator iter=input.projectionBegin(),limit=input.projectionEnd();iter!=limit;++iter)
      output.addProjection(*iter);

   return true;
}
//---------------------------------------------------------------------------
