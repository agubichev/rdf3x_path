#ifndef H_cts_parser_SPARQLParser
#define H_cts_parser_SPARQLParser
//---------------------------------------------------------------------------
#include <map>
#include <string>
#include <vector>
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
   /// An element in a graph pattern
   struct Element {
      /// Possible types
      enum Type { Variable, String, IRI };
      /// The type
      Type type;
      /// The string value
      std::string value;
      /// The id for variables
      unsigned id;
   };
   /// A graph pattern
   struct Pattern {
      /// The entires
      Element subject,predicate,object;

      /// Constructor
      Pattern(Element subject,Element predicate,Element object);
      /// Destructor
      ~Pattern();
   };
   private:
   /// The lexer
   SPARQLLexer& lexer;
   /// The registered prefixes
   std::map<std::string,std::string> prefixes;
   /// The named variables
   std::map<std::string,unsigned> namedVariables;
   /// The total variable count
   unsigned variableCount;

   /// The projection clause
   std::vector<unsigned> projection;
   /// The patterns in the where clause
   std::vector<Pattern> patterns;

   /// Lookup or create a named variable
   unsigned nameVariable(const std::string& name);

   /// Parse an entry in a pattern
   Element parsePatternElement(std::map<std::string,unsigned>& localVars);
   /// Parse blank node patterns
   Element parseBlankNode(std::map<std::string,unsigned>& localVars);
   // Parse a graph pattern
   void parseGraphPattern();
   // Parse a group of patterns
   void parseGroupGraphPattern();

   /// Parse the prefix part if any
   void parsePrefix();
   /// Parse the projection
   void parseProjection();
   /// Parse the from part if any
   void parseFrom();
   /// Parse the where part if any
   void parseWhere();

   public:
   /// Constructor
   explicit SPARQLParser(SPARQLLexer& lexer);
   /// Destructor
   ~SPARQLParser();

   /// Parse the input. Throws an exception in the case of an error
   void parse();

   /// Iterator over the patterns
   typedef std::vector<Pattern>::const_iterator pattern_iterator;
   /// Iterator over the patterns
   pattern_iterator patternsBegin() const { return patterns.begin(); }
   /// Iterator over the patterns
   pattern_iterator patternsEnd() const { return patterns.end(); }

   /// Iterator over the projection clause
   typedef std::vector<unsigned>::const_iterator projection_iterator;
   /// Iterator over the projection
   projection_iterator projectionBegin() const { return projection.begin(); }
   /// Iterator over the projection
   projection_iterator projectionEnd() const { return projection.end(); }
};
//---------------------------------------------------------------------------
#endif
