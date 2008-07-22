#include "TurtleParser.hpp"
#include <iostream>
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
TurtleParser::Lexer::Lexer(istream& in)
   : in(in),putBack(Eof),line(1)
   // Constructor
{
}
//---------------------------------------------------------------------------
TurtleParser::Lexer::~Lexer()
   // Destructor
{
}
//---------------------------------------------------------------------------
static bool issep(char c) { return (c==' ')||(c=='\t')||(c=='\n')||(c=='\r')||(c=='[')||(c==']')||(c=='(')||(c==')')||(c==',')||(c==';'); }
//---------------------------------------------------------------------------
TurtleParser::Lexer::Token TurtleParser::Lexer::lexNumber(char c)
   // Lex a number
{
   token="";

   while (true) {
      // Sign?
      if ((c=='+')||(c=='-')) {
         token+=c;
         if (!in.get(c)) break;
      }

      // First number block
      if (c!='.') {
         if ((c<'0')||(c>'9')) break;
         while ((c>='0')&&(c<='9')) {
            token+=c;
            if (!in.get(c)) return Integer;
         }
         if (issep(c)) {
            in.unget();
            return Integer;
         }
      }

      // Dot?
      if (c=='.') {
         token+=c;
         if (!in.get(c)) break;
         // Second number block
         while ((c>='0')&&(c<='9')) {
            token+=c;
            if (!in.get(c)) return Decimal;
         }
         if (issep(c)) {
            in.unget();
            return Decimal;
         }
      }

      // Exponent
      if ((c!='e')&&(c!='E')) break;
      token+=c;
      if (!in.get(c)) break;
      if ((c=='-')||(c=='+')) {
         token+=c;
         if (!in.get(c)) break;
      }
      if ((c<'0')||(c>'9')) break;
      while ((c>='0')&&(c<='9')) {
         token+=c;
         if (!in.get(c)) return Double;
      }
      if (issep(c)) {
         in.unget();
         return Double;
      }
      break;
   }
   cerr << "lexer error in line " << line << ": invalid number " << token << c << endl;
   throw Exception();
}
//---------------------------------------------------------------------------
unsigned TurtleParser::Lexer::lexHexCode(unsigned len)
   // Parse a hex code
{
   unsigned result=0;
   for (unsigned index=0;;index++) {
      // Done?
      if (index==len) return result;

      // Read the next char
      char c;
      if (!in.get(c)) break;

      // Interpret it
      if ((c>='0')&&(c<='9')) result=(result<<4)|(c-'0'); else
      if ((c>='A')&&(c<='F')) result=(result<<4)|(c-'A'+10); else
      if ((c>='a')&&(c<='f')) result=(result<<4)|(c-'a'+10); else
         break;
   }
   cerr << "lexer error in line " << line << ": invalid unicode escape" << endl;
   throw Exception();
}
//---------------------------------------------------------------------------
static string encodeUtf8(unsigned code)
   // Encode a unicode character as utf8
{
   string result;
   if (code&&(code<0x80)) {
      result+=static_cast<char>(code);
   } else if (code<0x800) {
      result+=static_cast<char>(0xc0 | (0x1f & (code >> 6)));
      result+=static_cast<char>(0x80 | (0x3f & code));
   } else {
      result+=static_cast<char>(0xe0 | (0x0f & (code >> 12)));
      result+=static_cast<char>(0x80 | (0x3f & (code >>  6)));
      result+=static_cast<char>(0x80 | (0x3f & code));
   }
   return result;
}
//---------------------------------------------------------------------------
void TurtleParser::Lexer::lexEscape()
   // Lex an escape sequence, \ already consumed
{
   while (true) {
      char c;
      if (!in.get(c)) break;
      // Standard escapes?
      if (c=='t') { token+='\t'; return; }
      if (c=='n') { token+='\n'; return; }
      if (c=='r') { token+='\r'; return; }
      if (c=='\"') { token+='\"'; return; }
      if (c=='>') { token+='>'; return; }
      if (c=='\\') { token+='\\'; return; }

      // Unicode sequences?
      if (c=='u') {
         unsigned code=lexHexCode(4);
         token+=encodeUtf8(code);
         return;
      }
      if (c=='U') {
         unsigned code=lexHexCode(8);
         token+=encodeUtf8(code);
         return;
      }

      // Invalid escape
      break;
   }
   cerr << "lexer error in line " << line << ": invalid escape sequence" << endl;
   throw Exception();
}
//---------------------------------------------------------------------------
TurtleParser::Lexer::Token TurtleParser::Lexer::lexLongString()
   // Lex a long string, first """ already consumed
{
   char c;
   while (in.get(c)) {
      if (c=='\"') {
         if (!in.get(c)) break;
         if (c!='\"') { token+='\"'; continue; }
         if (!in.get(c)) break;
         if (c!='\"') { token+="\"\""; continue; }
         return String;
      }
      if (c=='\\') {
         lexEscape();
      } else {
         token+=c;
         if (c=='\n') line++;
      }
   }
   cerr << "lexer error in line " << line << ": invalid string" << endl;
   throw Exception();
}
//---------------------------------------------------------------------------
TurtleParser::Lexer::Token TurtleParser::Lexer::lexString(char c)
   // Lex a string
{
   token="";

   // Check the next character
   if (!in.get(c)) {
      cerr << "lexer error in line " << line << ": invalid string" << endl;
      throw Exception();
   }

   // Another quote?
   if (c=='\"') {
      if (!in.get(c))
         return String;
      if (c=='\"')
         return lexLongString();
      in.unget();
      return String;
   }

   // Process normally
   while (true) {
      if (c=='\"') return String;
      if (c=='\\') {
         lexEscape();
      } else {
         token+=c;
         if (c=='\n') line++;
      }
      if (!in.get(c)) {
         cerr << "lexer error in line " << line << ": invalid string" << endl;
         throw Exception();
      }
   }
}
//---------------------------------------------------------------------------
TurtleParser::Lexer::Token TurtleParser::Lexer::lexURI(char c)
   // Lex a URI
{
   token="";

   // Check the next character
   if (!in.get(c)) {
      cerr << "lexer error in line " << line << ": invalid URI" << endl;
      throw Exception();
   }

   // Process normally
   while (true) {
      if (c=='>') return String;
      if (c=='\\') {
         lexEscape();
      } else {
         token+=c;
         if (c=='\n') line++;
      }
      if (!in.get(c)) {
         cerr << "lexer error in line " << line << ": invalid URI" << endl;
         throw Exception();
      }
   }
}
//---------------------------------------------------------------------------
TurtleParser::Lexer::Token TurtleParser::Lexer::next()
   // Get the next token
{
   // Do we already have one?
   if (putBack!=Eof) {
      Token result=putBack;
      putBack=Eof;
      return result;
   }

   // Read more
   char c;
   while (in.get(c)) {
      switch (c) {
         case ' ': case '\t': case '\r': continue;
         case '\n': line++; continue;
         case '#': while (in.get(c)) if ((c=='\n')||(c=='\r')) break; continue;
         case '.': if (!in.get(c)) return Dot; in.unget(); if ((c>='0')&&(c<='9')) return lexNumber('.'); return Dot;
         case ':': return Colon;
         case ';': return Semicolon;
         case ',': return Comma;
         case '[': return LBracket;
         case ']': return RBracket;
         case '(': return LParen;
         case ')': return RParen;
         case '@': return At;
         case '+': case '-': case '0': case '1': case '2': case '3': case '4': case '5': case '6': case '7': case '8': case '9':
            return lexNumber(c);
         case '^':
            if (in.get(c)||(c!='^')) {
               cerr << "lexer error in line " << line << ": '^' expected" << endl;
               throw Exception();
            }
            return Type;
         case '\"': return lexString(c);
         case '<': return lexURI(c);
         default:
            if (((c>='A')&&(c<='Z'))||((c>='a')&&(c<='z'))||(c=='_')) { // XXX unicode!
               token=c;
               while (in.get(c)) {
                  if (issep(c)) { in.unget(); break; }
                  token+=c;
               }
               if (token=="a") return A;
               if (token=="true") return True;
               if (token=="false") return False;
               return Name;
            } else {
               cerr << "lexer error in line " << line << ": unexpected character " << c << endl;
               throw Exception();
            }
      }
   }

   return Eof;
}
//---------------------------------------------------------------------------
TurtleParser::TurtleParser(istream& in)
   : lexer(in),nextBlank(0)
   // Constructor
{
}
//---------------------------------------------------------------------------
TurtleParser::~TurtleParser()
   // Destructor
{
}
//---------------------------------------------------------------------------
void TurtleParser::parseError(const string& message)
   // Report an error
{
   cerr << "parse error in line " << lexer.getLine() << ": " << message << endl;
   throw Exception();
}
//---------------------------------------------------------------------------
string TurtleParser::newBlankNode()
   // Construct a new blank node
{
   char buffer[50];
   snprintf(buffer,sizeof(buffer),"_:_%u",nextBlank++);
   return string(buffer);
}
//---------------------------------------------------------------------------
void TurtleParser::parseDirective()
   // Parse a directive
{
   if (lexer.next()!=Lexer::Name)
      parseError("directive name expected after '@'");

   if (lexer.getTokenValue()=="base") {
      if (lexer.next()!=Lexer::URI)
         parseError("URI expected after @base");
      static bool warned=false;
      if (!warned) {
         cerr << "warning: @base directives are currently ignored" << endl;
         warned=true; // XXX
      }
      base=lexer.getTokenValue();
   } else if (lexer.getTokenValue()=="prefix") {
      Lexer::Token token=lexer.next();
      std::string prefixName;
      // A prefix name?
      if (token==Lexer::Name) {
         prefixName=lexer.getTokenValue();
         token=lexer.next();
      }
      // Colon
      if (token!=Lexer::Colon)
         parseError("':' expected after @prefix");
      // URI
      if (lexer.next()!=Lexer::URI)
         parseError("URI expected after @prefix");
      prefixes[prefixName]=lexer.getTokenValue();
   } else {
      parseError("unknown directive @"+lexer.getTokenValue());
   }

   // Final dot
   if (lexer.next()!=Lexer::Dot)
      parseError("'.' expected after directive");
}
//---------------------------------------------------------------------------
string TurtleParser::parseQualifiedName(string prefix)
   // Parse a qualified name
{
   if (lexer.next()!=Lexer::Colon)
      parseError("':' expected in qualified name");
   if (!prefixes.count(prefix))
      parseError("unknown prefix '"+prefix+"'");
   Lexer::Token token=lexer.next();
   if (token==Lexer::Name) {
      return prefixes[prefix]+lexer.getTokenValue();
   } else {
      lexer.unget(token);
      return prefixes[prefix];
   }
}
//---------------------------------------------------------------------------
string TurtleParser::parseBlank()
   // Parse a blank entry
{
   Lexer::Token token=lexer.next();
   switch (token) {
      case Lexer::Name:
         if ((lexer.getTokenValue()!="_")||(lexer.next()!=Lexer::Colon)||(lexer.next()!=Lexer::Name))
            parseError("blank nodes must start with '_:'");
         return "_:"+lexer.getTokenValue();
      case Lexer::LBracket:
         {
            string node=newBlankNode();
            token=lexer.next();
            if (token!=Lexer::RBracket) {
               lexer.unget(token);
               parsePredicateObjectList(node);
               if (lexer.next()!=Lexer::RBracket)
                  parseError("']' expected");
            }
            return node;
         }
      case Lexer::LParen:
         {
            // Collection
            vector<string> entries;
            while ((token=lexer.next())!=Lexer::RParen) {
               lexer.unget(token);
               entries.push_back(parseObject());
            }

            // Empty collection?
            if (entries.empty())
               return "http://www.w3.org/1999/02/22-rdf-syntax-ns#nil";

            // Build blank nodes
            vector<string> nodes;
            for (unsigned index=0;index<entries.size();index++)
               nodes.push_back(newBlankNode());
            nodes.push_back("http://www.w3.org/1999/02/22-rdf-syntax-ns#nil");

            // Derive triples
            for (unsigned index=0;index<entries.size();index++) {
               triples.push_back(Triple(nodes[index],"http://www.w3.org/1999/02/22-rdf-syntax-ns#first",entries[index]));
               triples.push_back(Triple(nodes[index],"http://www.w3.org/1999/02/22-rdf-syntax-ns#rest",nodes[index+1]));
            }
            return nodes.front();
         }

      default: parseError("invalid blank entry");
   }
   // Not reachable
   return "";
}
//---------------------------------------------------------------------------
string TurtleParser::parseSubject()
   // Parse a subject
{
   Lexer::Token token=lexer.next();
   switch (token) {
      case Lexer::URI:
         // URI
         return lexer.getTokenValue();
      case Lexer::A: return "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
      case Lexer::Colon:
         // Qualified name with empty prefix?
         lexer.unget(token);
         return parseQualifiedName("");
      case Lexer::Name:
         // Qualified name
         // Blank node?
         if (lexer.getTokenValue()=="_") {
            lexer.unget(token);
            return parseBlank();
         }
         // No
         return parseQualifiedName(lexer.getTokenValue());
      case Lexer::LBracket: case Lexer::LParen:
         // Opening bracket/parenthesis
         lexer.unget(token);
         return parseBlank();
      default: parseError("invalid subject");
   }
   // Not reachable
   return "";
}
//---------------------------------------------------------------------------
string TurtleParser::parseObject()
   // Parse an object
{
   Lexer::Token token=lexer.next();
   switch (token) {
      case Lexer::URI:
         // URI
         return lexer.getTokenValue();
      case Lexer::Colon:
         // Qualified name with empty prefix?
         lexer.unget(token);
         return parseQualifiedName("");
      case Lexer::Name:
         // Qualified name
         // Blank node?
         if (lexer.getTokenValue()=="_") {
            lexer.unget(token);
            return parseBlank();
         }
         // No
         return parseQualifiedName(lexer.getTokenValue());
      case Lexer::LBracket: case Lexer::LParen:
         // Opening bracket/parenthesis
         lexer.unget(token);
         return parseBlank();
      case Lexer::Integer: case Lexer::Decimal: case Lexer::Double: case Lexer::A: case Lexer::True: case Lexer::False:
         // Literal
         return lexer.getTokenValue();
      case Lexer::String:
         // String literal
         {
            string result=lexer.getTokenValue();
            token=lexer.next();
            if (token==Lexer::At) {
               if (lexer.next()!=Lexer::Name)
                  parseError("language tag expected");
               static bool warned=false;
               if (!warned) {
                  cerr << "warning: language tags are currently ignored" << endl;
                  warned=true; // XXX
               }
            } else if (token==Lexer::Type) {
               token=lexer.next();
               string type;
               if (token==Lexer::URI) {
                  type=lexer.getTokenValue();
               } else if (token==Lexer::Colon) {
                  type=parseQualifiedName("");
               } else if (token==Lexer::Name) {
                  type=parseQualifiedName(lexer.getTokenValue());
               }
               static bool warned=false;
               if (!warned) {
                  cerr << "warning: literal types are currently ignored" << endl;
                  warned=true; // XXX
               }
            } else {
               lexer.unget(token);
            }
            return result;
         }
      default: parseError("invalid object");
   }
   // Not reachable
   return "";
}
//---------------------------------------------------------------------------
void TurtleParser::parsePredicateObjectList(const string& subject)
   // Parse a predicate object list
{
   bool first=true;
   while (true) {
      // Parse the predicate
      Lexer::Token token;
      string predicate;
      switch (token=lexer.next()) {
         case Lexer::URI: predicate=lexer.getTokenValue(); break;
         case Lexer::A: predicate="http://www.w3.org/1999/02/22-rdf-syntax-ns#type"; break;
         case Lexer::Colon: lexer.unget(token); predicate=parseQualifiedName(""); break;
         case Lexer::Name: if (lexer.getTokenValue()=="_") parseError("blank nodes not allowed as predicate"); predicate=parseQualifiedName(lexer.getTokenValue()); break;
         default: if (first) parseError("invalid predicate"); lexer.unget(token); return;
      }
      first=false;

      // Parse the objects
      while (true) {
         string object=parseObject();
         triples.push_back(Triple(subject,predicate,object));
         token=lexer.next();
         if (token!=Lexer::Comma) break;
      }
      // Semicolon?
      if (token!=Lexer::Semicolon) {
         lexer.unget(token);
         return;
      }
   }
}
//---------------------------------------------------------------------------
void TurtleParser::parseTriple()
   // Parse a triple
{
   string subject=parseSubject();
   parsePredicateObjectList(subject);
   if (lexer.next()!=Lexer::Dot)
      parseError("'.' expected after triple");
}
//---------------------------------------------------------------------------
bool TurtleParser::parse(std::string& subject,std::string& predicate,std::string& object)
   // Read the next triple
{
   while (true) {
      // Some triples left?
      if (!triples.empty()) {
         subject=triples.back().subject;
         predicate=triples.back().predicate;
         object=triples.back().object;
         triples.pop_back();
         return true;
      }

      // No, check if the input is done
      Lexer::Token token=lexer.next();
      if (token==Lexer::Eof) return false;

      // A directive?
      if (token==Lexer::At) {
         parseDirective();
         continue;
      }

      // No, parse a triple
      lexer.unget(token);
      parseTriple();
   }
}
//---------------------------------------------------------------------------
