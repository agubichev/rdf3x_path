#ifndef H_cts_parser_SPARQLParser
#define H_cts_parser_SPARQLParser
//---------------------------------------------------------------------------
#include <map>
#include <string>
//---------------------------------------------------------------------------
class SPARQLLexer;
//---------------------------------------------------------------------------
/// A parser for SPARQL input
class SPARQLParser
{
   public:
   /// A parsing exception
   struct ParserException {
      /// The message
      std::string message;

      /// Constructor
      ParserException(const std::string& message);
      /// Constructor
      ParserException(const char* message);
      /// Destructor
      ~ParserException();
   };
   private:
   /// The lexer
   SPARQLLexer& lexer;
   /// The registered prefixes
   std::map<std::string,std::string> prefixes;

   /// Parse the prefix part if any
   void parsePrefix();

   public:
   /// Constructor
   explicit SPARQLParser(SPARQLLexer& lexer);
   /// Destructor
   ~SPARQLParser();

   /// Parse the input. Throws an exception in the case of an error
   void parse();
};
//---------------------------------------------------------------------------
#endif
