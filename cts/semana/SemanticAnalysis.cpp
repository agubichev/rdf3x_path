#include "cts/semana/SemanticAnalysis.hpp"
#include "cts/parser/SPARQLParser.hpp"
#include "cts/infra/QueryGraph.hpp"
#include "rts/database/Database.hpp"
#include "rts/segment/DictionarySegment.hpp"
#include "rts/segment/FactsSegment.hpp"
#include <set>
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
static bool encodeFilter(Database& db,const SPARQLParser::PatternGroup& group,const SPARQLParser::Filter& input,QueryGraph::SubQuery& output)
   // Encode an element for the query graph
{
   // Check if the id is bound somewhere
   if (!binds(group,input.id))
      return false;

   // A complex filter? XXX handles only primitive filters
   if ((input.values.size()==1)&&(input.values[0].type==SPARQLParser::Element::Variable)) {
      if (!binds(group,input.id))
         return input.type==SPARQLParser::Filter::Exclude;
      QueryGraph::ComplexFilter filter;
      filter.id1=input.id;
      filter.id2=input.values[0].id;
      filter.equal=(input.type==SPARQLParser::Filter::Normal);
      output.complexFilters.push_back(filter);
      return true;
   }

   // Resolve all values
   std::set<unsigned> values;
   for (std::vector<SPARQLParser::Element>::const_iterator iter=input.values.begin(),limit=input.values.end();iter!=limit;++iter) {
      unsigned id;
      if (db.getDictionary().lookup((*iter).value,id))
         values.insert(id);
   }

   // Construct the filter
   QueryGraph::Filter filter;
   filter.id=input.id;
   filter.values.clear();
   if (input.type!=SPARQLParser::Filter::Path) {
      for (std::set<unsigned>::const_iterator iter=values.begin(),limit=values.end();iter!=limit;++iter)
         filter.values.push_back(*iter);
      filter.exclude=(input.type==SPARQLParser::Filter::Exclude);
   } else if (values.size()==2) {
      unsigned target,via;
      db.getDictionary().lookup(input.values[0].value,target);
      db.getDictionary().lookup(input.values[1].value,via);

      // Explore the path
      std::set<unsigned> explored;
      std::vector<unsigned> toDo;
      toDo.push_back(target);
      while (!toDo.empty()) {
         // Examine the next reachable node
         unsigned current=toDo.front();
         toDo.erase(toDo.begin());
         if (explored.count(current))
            continue;
         explored.insert(current);
         filter.values.push_back(current);

         // Request all other reachable nodes
         FactsSegment::Scan scan;
         if (scan.first(db.getFacts(Database::Order_Predicate_Object_Subject),via,current,0)) {
            while ((scan.getValue1()==via)&&(scan.getValue2()==current)) {
               toDo.push_back(scan.getValue3());
               if (!scan.next())
                  break;
            }
         }
      }
   }

   output.filters.push_back(filter);
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
      if (!encodeFilter(db,group,*iter,output)) {
         // The filter variable is not bound. This will produce an empty result
         return false;
      }
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

   // Set the limit
   output.setLimit(input.getLimit());

   return true;
}
//---------------------------------------------------------------------------
