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
SPARQLParser::SPARQLParser(SPARQLLexer& lexer)
   : lexer(lexer)
   // Constructor
{
}
//---------------------------------------------------------------------------
SPARQLParser::~SPARQLParser()
   // Destructor
{
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
void SPARQLParser::parse()
   // Parse the input
{
   // Parse the prefix part
   parsePrefix();

   // Parse the projection
   if ((lexer.getNext()!=SPARQLLexer::Identifier)||(!lexer.isKeyword("select")))
      throw ParserException("'select' expected");

}
//---------------------------------------------------------------------------
