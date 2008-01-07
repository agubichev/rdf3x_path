#ifndef H_cts_parser_SPARQLLexer
#define H_cts_parser_SPARQLLexer
//---------------------------------------------------------------------------
#include <string>
//---------------------------------------------------------------------------
/// A lexer for SPARQL input
class SPARQLLexer
{
   public:
   /// Possible tokens
   enum Token { None, Error, Eof, IRI, String, Variable, Identifier, Colon, Semicolon, Comma, Dot, Star, Underscore, LCurly, RCurly, LParen, RParen, LBracket, RBracket, Anon };

   private:
   /// The input
   std::string input;
   /// The current position
   std::string::const_iterator pos;
   /// The start of the current token
   std::string::const_iterator tokenStart;
   /// The end of the curent token. Only set if delimiters are stripped
   std::string::const_iterator tokenEnd;
   /// The token put back with unget
   Token putBack;

   public:
   /// Constructor
   SPARQLLexer(const std::string& input);
   /// Destructor
   ~SPARQLLexer();

   /// Get the next token
   Token getNext();
   /// Get the value of the current token
   std::string getTokenValue() const;
   /// Check if the current token matches a keyword
   bool isKeyword(const char* keyword) const;
   /// Put the last token back
   void unget(Token value) { putBack=value; }
};
//---------------------------------------------------------------------------
#endif
