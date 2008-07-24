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
      /// The putback
      Token putBack;
      /// The putback string
      std::string putBackValue;
      /// Buffer for parsing when ignoring the value
      std::string ignored;
      /// The current line
      unsigned line;

      /// Lex a hex code
      unsigned lexHexCode(unsigned len);
      /// Lex an escape sequence
      void lexEscape(std::string& token);
      /// Lex a long string
      Token lexLongString(std::string& token);
      /// Lex a string
      Token lexString(std::string& token,char c);
      /// Lex a URI
      Token lexURI(std::string& token,char c);
      /// Lex a number
      Token lexNumber(std::string& token,char c);

      public:
      /// Constructor
      Lexer(std::istream& in);
      /// Destructor
      ~Lexer();

      /// The next token (including value)
      Token next(std::string& value);
      /// The next token (ignoring the value)
      Token next() { return next(ignored); }
      /// Put a token and a string back
      void unget(Token t,const std::string& s) { putBack=t; if (t>=Integer) putBackValue=s; }
      /// Put a token back
      void ungetIgnored(Token t) { putBack=t; if (t>=Integer) putBackValue=ignored; }
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
   /// Reader in the triples
   unsigned triplesReader;
   /// The next blank node id
   unsigned nextBlank;

   /// Is a (generalized) name token?
   static inline bool isName(Lexer::Token token);

   /// Construct a new blank node
   void newBlankNode(std::string& node);
   /// Report an error
   void parseError(const std::string& message);
   /// Parse a qualified name
   void parseQualifiedName(const std::string& prefix,std::string& name);
   /// Parse a blank entry
   void parseBlank(std::string& entry);
   /// Parse a subject
   void parseSubject(Lexer::Token token,std::string& subject);
   /// Parse an object
   void parseObject(std::string& object);
   /// Parse a predicate object list
   void parsePredicateObjectList(const std::string& subject,std::string& predicate,std::string& object);
   /// Parse a directive
   void parseDirective();
   /// Parse a new triple
   void parseTriple(Lexer::Token token,std::string& subject,std::string& predicate,std::string& object);

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
