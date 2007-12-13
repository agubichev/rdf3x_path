#include "cts/parser/SPARQLParser.hpp"
#include "cts/parser/SPARQLLexer.hpp"
//---------------------------------------------------------------------------
SPARQLParser::ParserException::ParserException(const std::string& message)
  : message(message)
   // Constructor
{
}
//---------------------------------------------------------------------------
SPARQLParser::ParserException::ParserException(const char* message)
  : message(message)
   // Constructor
{
}
//---------------------------------------------------------------------------
SPARQLParser::ParserException::~ParserException()
   // Destructor
{
}
//---------------------------------------------------------------------------
SPARQLParser::Pattern::Pattern(Element subject,Element predicate,Element object)
   : subject(subject),predicate(predicate),object(object)
   // Constructor
{
}
//---------------------------------------------------------------------------
SPARQLParser::Pattern::~Pattern()
   // Destructor
{
}
//---------------------------------------------------------------------------
SPARQLParser::SPARQLParser(SPARQLLexer& lexer)
   : lexer(lexer),variableCount(0)
   // Constructor
{
}
//---------------------------------------------------------------------------
SPARQLParser::~SPARQLParser()
   // Destructor
{
}
//---------------------------------------------------------------------------
unsigned SPARQLParser::nameVariable(const std::string& name)
   // Lookup or create a named variable
{
   if (namedVariables.count(name))
      return namedVariables[name];

   unsigned result=variableCount++;
   namedVariables[name]=result;
   return result;
}
//---------------------------------------------------------------------------
void SPARQLParser::parsePrefix()
   // Parse the prefix part if any
{
   while (true) {
      SPARQLLexer::Token token=lexer.getNext();

      if ((token==SPARQLLexer::Identifier)&&(lexer.isKeyword("prefix"))) {
         // Parse the prefix entry
         if (lexer.getNext()!=SPARQLLexer::Identifier)
            throw ParserException("prefix name expected");
         std::string name=lexer.getTokenValue();
         if (lexer.getNext()!=SPARQLLexer::Colon)
            throw ParserException("':' expected");
         if (lexer.getNext()!=SPARQLLexer::IRI)
            throw ParserException("IRI expected");
         std::string iri=lexer.getTokenValue();

         // Register the new prefix
         if (prefixes.count(name))
            throw ParserException("duplicate prefix '"+name+"'");
         prefixes[name]=iri;
      } else {
         lexer.unget(token);
         return;
      }
   }
}
//---------------------------------------------------------------------------
void SPARQLParser::parseProjection()
   // Parse the projection
{
   // Parse the projection
   if ((lexer.getNext()!=SPARQLLexer::Identifier)||(!lexer.isKeyword("select")))
      throw ParserException("'select' expected");
   bool first=true;
   while (true) {
      SPARQLLexer::Token token=lexer.getNext();
      if (token==SPARQLLexer::Variable) {
         projection.push_back(nameVariable(lexer.getTokenValue()));
      } else if (token==SPARQLLexer::Star) {
         // We do nothing here. Empty projections will be filled with all
         // named variables after parsin.
      } else {
         if (first)
            throw ParserException("projection required after select");
         break;
      }
      first=false;
   }
}
//---------------------------------------------------------------------------
void SPARQLParser::parseFrom()
   // Parse the from part if any
{
   while (true) {
      SPARQLLexer::Token token=lexer.getNext();

      if ((token==SPARQLLexer::Identifier)&&(lexer.isKeyword("from"))) {
         throw ParserException("from clause currently not implemented");
      } else {
         lexer.unget(token);
         return;
      }
   }
}
//---------------------------------------------------------------------------
SPARQLParser::Element SPARQLParser::parseBlankNode(std::map<std::string,unsigned>& localVars)
   // Parse blank node patterns
{
   // The subject is a blank node
   Element subject;
   subject.type=Element::Variable;
   subject.id=variableCount++;

   // Parse the the remaining part of the pattern
   SPARQLParser::Element predicate=parsePatternElement(localVars);
   SPARQLParser::Element object=parsePatternElement(localVars);
   patterns.push_back(Pattern(subject,predicate,object));

   // Check for the tail
   while (true) {
      SPARQLLexer::Token token=lexer.getNext();
      if (token==SPARQLLexer::Semicolon) {
         predicate=parsePatternElement(localVars);
         object=parsePatternElement(localVars);
         continue;
      } else if (token==SPARQLLexer::Comma) {
         object=parsePatternElement(localVars);
         continue;
      } else if (token==SPARQLLexer::Dot) {
         return subject;
      } else if (token==SPARQLLexer::RBracket) {
         lexer.unget(token);
         return subject;
      } else {
         // Error while parsing, let out caller handle it
         lexer.unget(token);
         return subject;
      }
   }
}
//---------------------------------------------------------------------------
SPARQLParser::Element SPARQLParser::parsePatternElement(std::map<std::string,unsigned>& localVars)
   // Parse an entry in a pattern
{
   Element result;
   SPARQLLexer::Token token=lexer.getNext();
   if (token==SPARQLLexer::Variable) {
      result.type=Element::Variable;
      result.id=nameVariable(lexer.getTokenValue());
   } else if (token==SPARQLLexer::String) {
      result.type=Element::String;
      result.value=lexer.getTokenValue();
   } else if (token==SPARQLLexer::IRI) {
      result.type=Element::IRI;
      result.value=lexer.getTokenValue();
   } else if (token==SPARQLLexer::Anon) {
      result.type=Element::Variable;
      result.id=variableCount++;
   } else if (token==SPARQLLexer::LBracket) {
      result=parseBlankNode(localVars);
      if (lexer.getNext()!=SPARQLLexer::RBracket)
         throw ParserException("']' expected");
   } else if (token==SPARQLLexer::Underscore) {
      // _:variable
      if (lexer.getNext()!=SPARQLLexer::Colon)
         throw ParserException("':' expected");
      if (lexer.getNext()!=SPARQLLexer::Identifier)
         throw ParserException("identifier expected after ':'");
      result.type=Element::Variable;
      if (localVars.count(lexer.getTokenValue()))
         result.id=localVars[lexer.getTokenValue()]; else
         result.id=localVars[lexer.getTokenValue()]=variableCount++;
   } else if (token==SPARQLLexer::Colon) {
      // :identifier. Should reference the base
      if (lexer.getNext()!=SPARQLLexer::Identifier)
         throw ParserException("identifier expected after ':'");
      result.type=Element::IRI;
      result.value=lexer.getTokenValue();
   } else if (token==SPARQLLexer::Identifier) {
      std::string prefix=lexer.getTokenValue();
      // Handle the keyword 'a'
      if (prefix=="a") {
         result.type=Element::IRI;
         result.value="http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
      } else {
         // prefix:suffix
         if (lexer.getNext()!=SPARQLLexer::Colon)
            throw ParserException("':' expected after '"+prefix+"'");
         if (!prefixes.count(prefix))
            throw ParserException("unknown prefix '"+prefix+"'");
         if (lexer.getNext()!=SPARQLLexer::Identifier)
            throw ParserException("identifier expected after ':'");
         result.type=Element::IRI;
         result.value=prefixes[prefix]+lexer.getTokenValue();
      }
   } else {
      throw ParserException("invalid pattern element");
   }
   return result;
}
//---------------------------------------------------------------------------
void SPARQLParser::parseGraphPattern()
   // Parse a graph pattern
{
   std::map<std::string,unsigned> localVars;

   // Parse the first pattern
   Element subject=parsePatternElement(localVars);
   Element predicate=parsePatternElement(localVars);
   Element object=parsePatternElement(localVars);
   patterns.push_back(Pattern(subject,predicate,object));

   // Check for the tail
   while (true) {
      SPARQLLexer::Token token=lexer.getNext();
      if (token==SPARQLLexer::Semicolon) {
         predicate=parsePatternElement(localVars);
         object=parsePatternElement(localVars);
         continue;
      } else if (token==SPARQLLexer::Comma) {
         object=parsePatternElement(localVars);
         continue;
      } else if (token==SPARQLLexer::Dot) {
         return;
      } else if (token==SPARQLLexer::RCurly) {
         lexer.unget(token);
         return;
      } else {
         // Error while parsing, let out caller handle it
         lexer.unget(token);
         return;
      }
   }
}
//---------------------------------------------------------------------------
void SPARQLParser::parseGroupGraphPattern()
   // Parse a group of patterns
{
   while (true) {
      SPARQLLexer::Token token=lexer.getNext();

      if (token==SPARQLLexer::LCurly) {
         parseGroupGraphPattern();
      } else if ((token==SPARQLLexer::Identifier)||(token==SPARQLLexer::String)||(token==SPARQLLexer::Underscore)||(token==SPARQLLexer::Colon)||(token==SPARQLLexer::LBracket)) {
         lexer.unget(token);
         parseGraphPattern();
      } else if (token==SPARQLLexer::RCurly) {
         break;
      } else {
         throw ParserException("'}' expected");
      }
   }
}
//---------------------------------------------------------------------------
void SPARQLParser::parseWhere()
   // Parse the where part if any
{
   while (true) {
      SPARQLLexer::Token token=lexer.getNext();

      if ((token==SPARQLLexer::Identifier)&&(lexer.isKeyword("where"))) {
         if (lexer.getNext()!=SPARQLLexer::LCurly)
            throw ParserException("'{' expected");
         parseGroupGraphPattern();
      } else {
         lexer.unget(token);
         return;
      }
   }
}
//---------------------------------------------------------------------------
void SPARQLParser::parse()
   // Parse the input
{
   // Parse the prefix part
   parsePrefix();

   // Parse the projection
   parseProjection();

   // Parse the from clause
   parseFrom();

   // Parse the where clause
   parseWhere();

   // Check that the input is done
   if (lexer.getNext()!=SPARQLLexer::Eof)
      throw ParserException("syntax error");

   // Fixup empty projections (i.e. *)
   if (!projection.size()) {
      for (std::map<std::string,unsigned>::const_iterator iter=namedVariables.begin(),limit=namedVariables.end();iter!=limit;++iter)
         projection.push_back((*iter).second);
   }
}
//---------------------------------------------------------------------------
