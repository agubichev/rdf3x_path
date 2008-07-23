#ifndef H_tools_rdf3xload_TurtleParser
#define H_tools_rdf3xload_TurtleParser
//---------------------------------------------------------------------------
#include <istream>
#include <string>
#include <map>
#include <vector>
//---------------------------------------------------------------------------
/// Parse a turtle file
class TurtleParser
{
   public:
   /// A parse error
   class Exception {};

   private:
   /// A turtle lexer
   class Lexer {
      public:
      /// Possible tokens
      enum Token { Eof, Dot, Colon, Comma, Semicolon, LBracket, RBracket, LParen, RParen, At, Type, Integer, Decimal, Double, Name, A, True, False, String, URI };

      private:
      /// The input
      std::istream& in;
      /// The token buffer
      std::string token;
      /// The putback
      Token putBack;
      /// The current line
      unsigned line;

      /// Lex a hex code
      unsigned lexHexCode(unsigned len);
      /// Lex an escape sequence
      void lexEscape();
      /// Lex a long string
      Token lexLongString();
      /// Lex a string
      Token lexString(char c);
      /// Lex a URI
      Token lexURI(char c);
      /// Lex a number
      Token lexNumber(char c);

      public:
      /// Constructor
      Lexer(std::istream& in);
      /// Destructor
      ~Lexer();

      /// The next token
      Token next();
      /// Put a token pack
      void unget(Token t) { putBack=t; }
      /// Get the token value
      const std::string& getTokenValue() const { return token; }
      /// Get the line
      unsigned getLine() const { return line; }
   };
   /// A triple
   struct Triple {
      /// The entries
      std::string subject,predicate,object;

      /// Constructor
      Triple(const std::string& subject,const std::string& predicate,const std::string& object) : subject(subject),predicate(predicate),object(object) {}
   };

   /// The lexer
   Lexer lexer;
   /// The uri base
   std::string base;
   /// All known prefixes
   std::map<std::string,std::string> prefixes;
   /// The currently available triples
   std::vector<Triple> triples;
   /// The next blank node id
   unsigned nextBlank;

   /// Is a (generalized) name token?
   static inline bool isName(Lexer::Token token);

   /// Construct a new blank node
   std::string newBlankNode();
   /// Report an error
   void parseError(const std::string& message);
   /// Parse a qualified name
   std::string parseQualifiedName(std::string prefix);
   /// Parse a blank entry
   std::string parseBlank();
   /// Parse a subject
   std::string parseSubject();
   /// Parse an object
   std::string parseObject();
   /// Parse a predicate object list
   void parsePredicateObjectList(const std::string& subject);
   /// Parse a directive
   void parseDirective();
   /// Parse a new triple
   void parseTriple();

   public:
   /// Constructor
   TurtleParser(std::istream& in);
   /// Destructor
   ~TurtleParser();

   /// Read the next triple
   bool parse(std::string& subject,std::string& predicate,std::string& object);
};
//---------------------------------------------------------------------------
#endif
