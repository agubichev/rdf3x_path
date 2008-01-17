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
static bool binds(const SPARQLParser::PatternGroup& group,unsigned id)
   // Check if a variable is bound in a pattern group
{
   for (std::vector<SPARQLParser::Pattern>::const_iterator iter=group.patterns.begin(),limit=group.patterns.end();iter!=limit;++iter)
      if ((((*iter).subject.type==SPARQLParser::Element::Variable)&&((*iter).subject.id==id))||
          (((*iter).predicate.type==SPARQLParser::Element::Variable)&&((*iter).predicate.id==id))||
          (((*iter).object.type==SPARQLParser::Element::Variable)&&((*iter).object.id==id)))
         return true;
   for (std::vector<SPARQLParser::PatternGroup>::const_iterator iter=group.optional.begin(),limit=group.optional.end();iter!=limit;++iter)
      if (binds(*iter,id))
         return true;
   for (std::vector<std::vector<SPARQLParser::PatternGroup> >::const_iterator iter=group.unions.begin(),limit=group.unions.end();iter!=limit;++iter)
      for (std::vector<SPARQLParser::PatternGroup>::const_iterator iter2=(*iter).begin(),limit2=(*iter).end();iter2!=limit2;++iter2)
         if (binds(*iter2,id))
            return true;
   return false;
}
//---------------------------------------------------------------------------
static bool encodeFilter(Database& db,const SPARQLParser::PatternGroup& group,const SPARQLParser::Filter& input,QueryGraph::Filter& filter)
   // Encode an element for the query graph
{
   // Check if the id is bound somewhere
   if (!binds(group,input.id))
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
   filter.exclude=input.exclude;

   return true;
}
//---------------------------------------------------------------------------
static bool transformSubquery(Database& db,const SPARQLParser::PatternGroup& group,QueryGraph::SubQuery& output)
   // Transform a subquery
{
   // Encode all patterns
   for (std::vector<SPARQLParser::Pattern>::const_iterator iter=group.patterns.begin(),limit=group.patterns.end();iter!=limit;++iter) {
      // Encode the entries
      QueryGraph::Node node;
      if ((!encode(db,(*iter).subject,node.subject,node.constSubject))||
          (!encode(db,(*iter).predicate,node.predicate,node.constPredicate))||
          (!encode(db,(*iter).object,node.object,node.constObject))) {
         // A constant could not be resolved. This will produce an empty result
         return false;
      }
      output.nodes.push_back(node);
   }

   // Encode the filter conditions
   for (std::vector<SPARQLParser::Filter>::const_iterator iter=group.filters.begin(),limit=group.filters.end();iter!=limit;++iter) {
      QueryGraph::Filter filter;
      if (!encodeFilter(db,group,*iter,filter)) {
         // The filter variable is not bound. This will produce an empty result
         return false;
      }
      output.filters.push_back(filter);
   }

   // Encode all optional parts
   for (std::vector<SPARQLParser::PatternGroup>::const_iterator iter=group.optional.begin(),limit=group.optional.end();iter!=limit;++iter) {
      QueryGraph::SubQuery subQuery;
      if (!transformSubquery(db,*iter,subQuery)) {
         // Known to produce an empty result, skip it
         continue;
      }
      output.optional.push_back(subQuery);
   }

   // Encode all union parts
   for (std::vector<std::vector<SPARQLParser::PatternGroup> >::const_iterator iter=group.unions.begin(),limit=group.unions.end();iter!=limit;++iter) {
      std::vector<QueryGraph::SubQuery> unionParts;
      for (std::vector<SPARQLParser::PatternGroup>::const_iterator iter2=(*iter).begin(),limit2=(*iter).end();iter2!=limit2;++iter2) {
         QueryGraph::SubQuery subQuery;
         if (!transformSubquery(db,*iter2,subQuery)) {
            // Known to produce an empty result, skip it
            continue;
         }
         unionParts.push_back(subQuery);
      }
      // Empty union?
      if (unionParts.empty())
         return false;
      output.unions.push_back(unionParts);
   }

   return true;
}
//---------------------------------------------------------------------------
bool SemanticAnalysis::transform(const SPARQLParser& input,QueryGraph& output)
   // Perform the transformation
{
   output.clear();

   if (!transformSubquery(db,input.getPatterns(),output.getQuery())) {
      // A constant could not be resolved. This will produce an empty result
      output.markAsKnownEmpty();
      return true;
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
