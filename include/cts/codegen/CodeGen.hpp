#ifndef H_cts_codegen_CodeGen
#define H_cts_codegen_CodeGen
//---------------------------------------------------------------------------
class Operator;
class Plan;
class Runtime;
class QueryGraph;
//---------------------------------------------------------------------------
/// Interfact to the code generation part of the compiletime system
class CodeGen
{
   public:
   /// Translate an execution plan into an operator tree
   static Operator* translate(Runtime& runtime,const QueryGraph& query,Plan* plan,bool silent=false);
};
//---------------------------------------------------------------------------
#endif
