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
static bool issep(char c) { return (c==' ')||(c=='\t')||(c=='\n')||(c=='\r')||(c=='[')||(c==']')||(c=='(')||(c==')')||(c==',')||(c==';')||(c==':')||(c=='.'); }
//---------------------------------------------------------------------------
TurtleParser::Lexer::Token TurtleParser::Lexer::lexNumber(std::string& token,char c)
   // Lex a number
{
   token.resize(0);

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
void TurtleParser::Lexer::lexEscape(std::string& token)
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
TurtleParser::Lexer::Token TurtleParser::Lexer::lexLongString(std::string& token)
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
         lexEscape(token);
      } else {
         token+=c;
         if (c=='\n') line++;
      }
   }
   cerr << "lexer error in line " << line << ": invalid string" << endl;
   throw Exception();
}
//---------------------------------------------------------------------------
TurtleParser::Lexer::Token TurtleParser::Lexer::lexString(std::string& token,char c)
   // Lex a string
{
   token.resize(0);

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
         return lexLongString(token);
      in.unget();
      return String;
   }

   // Process normally
   while (true) {
      if (c=='\"') return String;
      if (c=='\\') {
         lexEscape(token);
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
TurtleParser::Lexer::Token TurtleParser::Lexer::lexURI(std::string& token,char c)
   // Lex a URI
{
   token.resize(0);

   // Check the next character
   if (!in.get(c)) {
      cerr << "lexer error in line " << line << ": invalid URI" << endl;
      throw Exception();
   }

   // Process normally
   while (true) {
      if (c=='>') return URI;
      if (c=='\\') {
         lexEscape(token);
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
TurtleParser::Lexer::Token TurtleParser::Lexer::next(std::string& token)
   // Get the next token
{
   // Do we already have one?
   if (putBack!=Eof) {
      Token result=putBack;
      token=putBackValue;
      putBack=Eof;
      return result;
   }

   // Read more
   char c;
   while (in.get(c)) {
      switch (c) {
         case ' ': case '\t': case '\r': continue;
         case '\n': line++; continue;
         case '#': while (in.get(c)) if ((c=='\n')||(c=='\r')) break; if (c=='\n') ++line; continue;
         case '.': if (!in.get(c)) return Dot; in.unget(); if ((c>='0')&&(c<='9')) return lexNumber(token,'.'); return Dot;
         case ':': return Colon;
         case ';': return Semicolon;
         case ',': return Comma;
         case '[': return LBracket;
         case ']': return RBracket;
         case '(': return LParen;
         case ')': return RParen;
         case '@': return At;
         case '+': case '-': case '0': case '1': case '2': case '3': case '4': case '5': case '6': case '7': case '8': case '9':
            return lexNumber(token,c);
         case '^':
            if ((!in.get(c))||(c!='^')) {
               cerr << "lexer error in line " << line << ": '^' expected" << endl;
               throw Exception();
            }
            return Type;
         case '\"': return lexString(token,c);
         case '<': return lexURI(token,c);
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
   : lexer(in),triplesReader(0),nextBlank(0)
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
void TurtleParser::newBlankNode(std::string& node)
   // Construct a new blank node
{
   char buffer[50];
   snprintf(buffer,sizeof(buffer),"_:_%u",nextBlank++);
   node=buffer;
}
//---------------------------------------------------------------------------
void TurtleParser::parseDirective()
   // Parse a directive
{
   std::string value;
   if (lexer.next(value)!=Lexer::Name)
      parseError("directive name expected after '@'");

   if (value=="base") {
      if (lexer.next(base)!=Lexer::URI)
         parseError("URI expected after @base");
      static bool warned=false;
      if (!warned) {
         cerr << "warning: @base directives are currently ignored" << endl;
         warned=true; // XXX
      }
   } else if (value=="prefix") {
      std::string prefixName;
      Lexer::Token token=lexer.next(prefixName);
      // A prefix name?
      if (token==Lexer::Name) {
         token=lexer.next();
      } else prefixName.resize(0);
      // Colon
      if (token!=Lexer::Colon)
         parseError("':' expected after @prefix");
      // URI
      std::string uri;
      if (lexer.next(uri)!=Lexer::URI)
         parseError("URI expected after @prefix");
      prefixes[prefixName]=uri;
   } else {
      parseError("unknown directive @"+value);
   }

   // Final dot
   if (lexer.next()!=Lexer::Dot)
      parseError("'.' expected after directive");
}
//---------------------------------------------------------------------------
inline bool TurtleParser::isName(Lexer::Token token)
   // Is a (generalized) name token?
{
   return (token==Lexer::Name)||(token==Lexer::A)||(token==Lexer::True)||(token==Lexer::False);
}
//---------------------------------------------------------------------------
void TurtleParser::parseQualifiedName(const string& prefix,string& name)
   // Parse a qualified name
{
   if (lexer.next()!=Lexer::Colon)
      parseError("':' expected in qualified name");
   if (!prefixes.count(prefix))
      parseError("unknown prefix '"+prefix+"'");
   Lexer::Token token=lexer.next(name);
   if (isName(token)) {
      name=prefixes[prefix]+name;
   } else {
      lexer.unget(token,name);
      name=prefixes[prefix];
   }
}
//---------------------------------------------------------------------------
void TurtleParser::parseBlank(std::string& entry)
   // Parse a blank entry
{
   Lexer::Token token=lexer.next(entry);
   switch (token) {
      case Lexer::Name:
         if ((entry!="_")||(lexer.next()!=Lexer::Colon)||(!isName(lexer.next(entry))))
            parseError("blank nodes must start with '_:'");
         entry="_:"+entry;
         return;
      case Lexer::LBracket:
         {
            newBlankNode(entry);
            token=lexer.next();
            if (token!=Lexer::RBracket) {
               lexer.ungetIgnored(token);
               std::string predicate,object;
               parsePredicateObjectList(entry,predicate,object);
               triples.push_back(Triple(entry,predicate,object));
               if (lexer.next()!=Lexer::RBracket)
                  parseError("']' expected");
            }
            return;
         }
      case Lexer::LParen:
         {
            // Collection
            vector<string> entries;
            while ((token=lexer.next())!=Lexer::RParen) {
               lexer.ungetIgnored(token);
               entries.push_back(string());
               parseObject(entries.back());
            }

            // Empty collection?
            if (entries.empty()) {
               entry="http://www.w3.org/1999/02/22-rdf-syntax-ns#nil";
               return;
            }

            // Build blank nodes
            vector<string> nodes;
            nodes.resize(entries.size());
            for (unsigned index=0;index<entries.size();index++)
               newBlankNode(nodes[index]);
            nodes.push_back("http://www.w3.org/1999/02/22-rdf-syntax-ns#nil");

            // Derive triples
            for (unsigned index=0;index<entries.size();index++) {
               triples.push_back(Triple(nodes[index],"http://www.w3.org/1999/02/22-rdf-syntax-ns#first",entries[index]));
               triples.push_back(Triple(nodes[index],"http://www.w3.org/1999/02/22-rdf-syntax-ns#rest",nodes[index+1]));
            }
            entry=nodes.front();
         }

      default: parseError("invalid blank entry");
   }
}
//---------------------------------------------------------------------------
void TurtleParser::parseSubject(Lexer::Token token,std::string& subject)
   // Parse a subject
{
   switch (token) {
      case Lexer::URI:
         // URI
         return;
      case Lexer::A: subject="http://www.w3.org/1999/02/22-rdf-syntax-ns#type"; return;
      case Lexer::Colon:
         // Qualified name with empty prefix?
         lexer.unget(token,subject);
         parseQualifiedName("",subject);
         return;
      case Lexer::Name:
         // Qualified name
         // Blank node?
         if (subject=="_") {
            lexer.unget(token,subject);
            parseBlank(subject);
            return;
         }
         // No
         parseQualifiedName(subject,subject);
         return;
      case Lexer::LBracket: case Lexer::LParen:
         // Opening bracket/parenthesis
         lexer.unget(token,subject);
         parseBlank(subject);
      default: parseError("invalid subject");
   }
}
//---------------------------------------------------------------------------
void TurtleParser::parseObject(std::string& object)
   // Parse an object
{
   Lexer::Token token=lexer.next(object);
   switch (token) {
      case Lexer::URI:
         // URI
         return;
      case Lexer::Colon:
         // Qualified name with empty prefix?
         lexer.unget(token,object);
         parseQualifiedName("",object);
         return;
      case Lexer::Name:
         // Qualified name
         // Blank node?
         if (object=="_") {
            lexer.unget(token,object);
            parseBlank(object);
            return;
         }
         // No
         parseQualifiedName(object,object);
         return;
      case Lexer::LBracket: case Lexer::LParen:
         // Opening bracket/parenthesis
         lexer.unget(token,object);
         parseBlank(object);
         return;
      case Lexer::Integer: case Lexer::Decimal: case Lexer::Double: case Lexer::A: case Lexer::True: case Lexer::False:
         // Literal
         return;
      case Lexer::String:
         // String literal
         {
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
               string type;
               token=lexer.next(type);
               if (token==Lexer::URI) {
                  // Already parsed
               } else if (token==Lexer::Colon) {
                  parseQualifiedName("",type);
               } else if (token==Lexer::Name) {
                  parseQualifiedName(type,type);
               }
               static bool warned=false;
               if (!warned) {
                  cerr << "warning: literal types are currently ignored" << endl;
                  warned=true; // XXX
               }
            } else {
               lexer.ungetIgnored(token);
            }
            return;
         }
      default: parseError("invalid object");
   }
}
//---------------------------------------------------------------------------
void TurtleParser::parsePredicateObjectList(const string& subject,string& predicate,string& object)
   // Parse a predicate object list
{
   // Parse the first predicate
   Lexer::Token token;
   switch (token=lexer.next(predicate)) {
      case Lexer::URI: break;
      case Lexer::A: predicate="http://www.w3.org/1999/02/22-rdf-syntax-ns#type"; break;
      case Lexer::Colon: lexer.unget(token,predicate); parseQualifiedName("",predicate); break;
      case Lexer::Name: if (predicate=="_") parseError("blank nodes not allowed as predicate"); parseQualifiedName(predicate,predicate); break;
      default: parseError("invalid predicate");
   }

   // Parse the object
   parseObject(object);

   // Additional objects?
   token=lexer.next();
   while (token==Lexer::Comma) {
      string additionalObject;
      parseObject(additionalObject);
      triples.push_back(Triple(subject,predicate,additionalObject));
      token=lexer.next();
   }

   // Additional predicates?
   while (token==Lexer::Semicolon) {
      // Parse the predicate
      string additionalPredicate;
      switch (token=lexer.next(additionalPredicate)) {
         case Lexer::URI: break;
         case Lexer::A: additionalPredicate="http://www.w3.org/1999/02/22-rdf-syntax-ns#type"; break;
         case Lexer::Colon: lexer.unget(token,additionalPredicate); parseQualifiedName("",additionalPredicate); break;
         case Lexer::Name: if (additionalPredicate=="_") parseError("blank nodes not allowed as predicate"); parseQualifiedName(additionalPredicate,additionalPredicate); break;
         default: lexer.unget(token,additionalPredicate); return;
      }

      // Parse the object
      string additionalObject;
      parseObject(object);
      triples.push_back(Triple(subject,additionalPredicate,additionalObject));

      // Additional objects?
      token=lexer.next();
      while (token==Lexer::Comma) {
         parseObject(additionalObject);
         triples.push_back(Triple(subject,additionalPredicate,additionalObject));
         token=lexer.next();
      }
   }
   lexer.ungetIgnored(token);
}
//---------------------------------------------------------------------------
void TurtleParser::parseTriple(Lexer::Token token,std::string& subject,std::string& predicate,std::string& object)
   // Parse a triple
{
   parseSubject(token,subject);
   parsePredicateObjectList(subject,predicate,object);
   if (lexer.next()!=Lexer::Dot)
      parseError("'.' expected after triple");
}
//---------------------------------------------------------------------------
bool TurtleParser::parse(std::string& subject,std::string& predicate,std::string& object)
   // Read the next triple
{
   // Some triples left?
   if (triplesReader<triples.size()) {
      subject=triples[triplesReader].subject;
      predicate=triples[triplesReader].predicate;
      object=triples[triplesReader].object;
      if ((++triplesReader)>=triples.size()) {
         triples.clear();
         triplesReader=0;
      }
      return true;
   }

   // No, check if the input is done
   Lexer::Token token;
   while (true) {
      token=lexer.next(subject);
      if (token==Lexer::Eof) return false;

      // A directive?
      if (token==Lexer::At) {
         parseDirective();
         continue;
      } else break;
   }

   // No, parse a triple
   parseTriple(token,subject,predicate,object);
   return true;
}
//---------------------------------------------------------------------------
