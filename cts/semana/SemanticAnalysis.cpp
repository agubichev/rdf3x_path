#include "cts/semana/SemanticAnalysis.hpp"
#include "cts/parser/SPARQLParser.hpp"
#include "cts/infra/QueryGraph.hpp"
#include "rts/database/Database.hpp"
#include "rts/segment/DictionarySegment.hpp"
#include <set>
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
static bool encode(Database& db,const SPARQLParser& query,const SPARQLParser::Filter& input,QueryGraph::Filter& filter)
   // Encode an element for the query graph
{
   // Check if the id is bound somewhere
   bool found=false;
   for (SPARQLParser::pattern_iterator iter=query.patternsBegin(),limit=query.patternsEnd();iter!=limit;++iter)
      if ((((*iter).subject.type==SPARQLParser::Element::Variable)&&((*iter).subject.id==input.id))||
          (((*iter).predicate.type==SPARQLParser::Element::Variable)&&((*iter).predicate.id==input.id))||
          (((*iter).object.type==SPARQLParser::Element::Variable)&&((*iter).object.id==input.id))) {
         found=true;
         break;
      }
   if (!found)
      return false;

   // Resolve all values
   std::set<unsigned> values;
   for (std::vector<SPARQLParser::Element>::const_iterator iter=input.values.begin(),limit=input.values.end();iter!=limit;++iter) {
      unsigned id;
      if (db.getDictionary().lookup((*iter).value,id))
         values.insert(id);
   }

   // Construct the filter
   filter.id=input.id;
   filter.values.clear();
   for (std::set<unsigned>::const_iterator iter=values.begin(),limit=values.end();iter!=limit;++iter)
      filter.values.push_back(*iter);

   return true;
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
         // A constant could not be resolved. This will produce an empty result
         output.clear();
         return true;
      }
      output.addNode(node);
   }

   // Encode the filter conditions
   for (SPARQLParser::filter_iterator iter=input.filtersBegin(),limit=input.filtersEnd();iter!=limit;++iter) {
      QueryGraph::Filter filter;
      if (!encode(db,input,*iter,filter)) {
         // The filter variable is not bound. This will produce an empty result
         output.clear();
         return true;
      }
      output.addFilter(filter);
   }

   // Compute the edges
   output.constructEdges();

   // Add the projection entry
   for (SPARQLParser::projection_iterator iter=input.projectionBegin(),limit=input.projectionEnd();iter!=limit;++iter)
      output.addProjection(*iter);

   // Set the duplicate handling
   switch (input.getProjectionModifier()) {
      case SPARQLParser::Modifier_None: output.setDuplicateHandling(QueryGraph::AllDuplicates); break;
      case SPARQLParser::Modifier_Distinct: output.setDuplicateHandling(QueryGraph::NoDuplicates); break;
      case SPARQLParser::Modifier_Reduced: output.setDuplicateHandling(QueryGraph::ReducedDuplicates); break;
      case SPARQLParser::Modifier_Count: output.setDuplicateHandling(QueryGraph::CountDuplicates); break;
      case SPARQLParser::Modifier_Duplicates: output.setDuplicateHandling(QueryGraph::ShowDuplicates); break;
   }

   return true;
}
//---------------------------------------------------------------------------
