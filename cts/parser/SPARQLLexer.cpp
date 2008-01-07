#include "cts/parser/SPARQLLexer.hpp"
//---------------------------------------------------------------------------
SPARQLLexer::SPARQLLexer(const std::string& input)
   : input(input),pos(this->input.begin()),tokenStart(pos),tokenEnd(pos),putBack(None)
   // Constructor
{
}
//---------------------------------------------------------------------------
SPARQLLexer::~SPARQLLexer()
   // Destructor
{
}
//---------------------------------------------------------------------------
SPARQLLexer::Token SPARQLLexer::getNext()
   // Get the next token
{
   // Do we have a token already?
   if (putBack!=None) {
      Token result=putBack;
      putBack=None;
      return result;
   }

   // Reset the token end
   tokenEnd=tokenStart-1;

   // Read the string
   while (pos!=input.end()) {
      tokenStart=pos;
      // Interpret the first character
      switch (*(pos++)) {
         // Whitespace
         case ' ': case '\t': case '\n': case '\r': case '\f': continue;
         // Single line comment
         case '#':
            while (pos!=input.end()) {
               if (((*pos)=='\n')||((*pos)=='\r'))
                  break;
               ++pos;
            }
            if (pos!=input.end()) ++pos;
            continue;
         // Simple tokens
         case ':': return Colon;
         case ';': return Semicolon;
         case ',': return Comma;
         case '.': return Dot;
         case '*': return Star;
         case '_': return Underscore;
         case '{': return LCurly;
         case '}': return RCurly;
         case '(': return LParen;
         case ')': return RParen;
         // Brackets
         case '[':
            // Skip whitespaces
            while (pos!=input.end()) {
               switch (*pos) {
                  case ' ': case '\t': case '\n': case '\r': case '\f': ++pos; continue;
               }
               break;
            }
            // Check for a closing ]
            if ((pos!=input.end())&&((*pos)==']')) {
               ++pos;
               return Anon;
            }
            return LBracket;
         case ']': return RBracket;
         // IRI Ref
         case '<':
            tokenStart=pos;
            while (pos!=input.end()) {
               if ((*pos)=='>')
                  break;
               ++pos;
            }
            tokenEnd=pos;
            if (pos!=input.end()) ++pos;
            return IRI;
         // String
         case '\'':
            tokenStart=pos;
            while (pos!=input.end()) {
               if ((*pos)=='\\') {
                  ++pos;
                  if (pos!=input.end()) ++pos;
                  continue;
               }
               if ((*pos)=='\'')
                  break;
               ++pos;
            }
            tokenEnd=pos;
            if (pos!=input.end()) ++pos;
            return String;
         // String
         case '\"':
            tokenStart=pos;
            while (pos!=input.end()) {
               if ((*pos)=='\\') {
                  ++pos;
                  if (pos!=input.end()) ++pos;
                  continue;
               }
               if ((*pos)=='\"')
                  break;
               ++pos;
            }
            tokenEnd=pos;
            if (pos!=input.end()) ++pos;
            return String;
         // Variables
         case '?': case '$':
            tokenStart=pos;
            while (pos!=input.end()) {
               char c=*pos;
               if (((c>='0')&&(c<='9'))||((c>='A')&&(c<='Z'))||((c>='a')&&(c<='z'))) {
                  ++pos;
               } else break;
            }
            tokenEnd=pos;
            return Variable;
         // Identifier
         default:
            --pos;
            while (pos!=input.end()) {
               char c=*pos;
               if (((c>='0')&&(c<='9'))||((c>='A')&&(c<='Z'))||((c>='a')&&(c<='z'))) {
                  ++pos;
               } else break;
            }
            if (pos==tokenStart)
               return Error;
            return Identifier;
      }
   }
   return Eof;
}
//---------------------------------------------------------------------------
std::string SPARQLLexer::getTokenValue() const
   // Get the value of the current token
{
   if (tokenEnd>=tokenStart)
      return std::string(tokenStart,tokenEnd); else
      return std::string(tokenStart,pos);
}
//---------------------------------------------------------------------------
bool SPARQLLexer::isKeyword(const char* keyword) const
   // Check if the current token matches a keyword
{
   std::string::const_iterator iter=tokenStart,limit=(tokenEnd>=tokenStart)?tokenEnd:pos;

   while (iter!=limit) {
      char c=*iter;
      if ((c>='A')&&(c<='Z')) c+='a'-'A';
      if (c!=(*keyword))
         return false;
      if (!*keyword) return false;
      ++iter; ++keyword;
   }
   return !*keyword;
}
//---------------------------------------------------------------------------
