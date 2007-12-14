#ifndef H_cts_codegen_CodeGen
#define H_cts_codegen_CodeGen
//---------------------------------------------------------------------------
class Operator;
class Runtime;
class QueryGraph;
//---------------------------------------------------------------------------
/// Interfact to the code generation part of the compiletime system
class CodeGen
{
   public:
   /// Perform a naive translation of a query into an operator tree
   static Operator* translate(Runtime& runtime,const QueryGraph& query,bool silent=false);
};
//---------------------------------------------------------------------------
#endif
