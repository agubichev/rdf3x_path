#ifndef H_cts_codegen_CodeGen
#define H_cts_codegen_CodeGen
//---------------------------------------------------------------------------
// RDF-3X
// (c) 2008 Thomas Neumann. Web site: http://www.mpi-inf.mpg.de/~neumann/rdf3x
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
//---------------------------------------------------------------------------
#include <set>
#include <map>
#include <vector>
#include "rts/ferrari/Index.h"
//---------------------------------------------------------------------------
class Operator;
class Plan;
class Register;
class VectorRegister;
class Runtime;
class QueryGraph;
//---------------------------------------------------------------------------
/// Interfact to the code generation part of the compiletime system
class CodeGen
{
   public:
   /// Output for code generation, consists of registers for single value and path output
   struct Output{
	    /// single value registers
		std::vector<Register*> valueoutput;
		/// path registers
		std::vector<VectorRegister*> pathoutput;
		/// order of paths and single values in projection
		/// value[i] == 0 if i-th element in the projection is single value, 1 if it's a path
		std::vector<bool> order;
   };
   /// Collect all variables contained in a plan
   static void collectVariables(std::set<unsigned>& variables,Plan* plan);
   /// Translate an execution plan into an operator tree without output generation
   static Operator* translateIntern(Runtime& runtime,const QueryGraph& query,Plan* plan,Output& output,std::map<unsigned,Index*>& ferrari);
   /// Translate an execution plan into an operator tree
   static Operator* translate(Runtime& runtime,const QueryGraph& query,Plan* plan,std::map<unsigned,Index*>& ferrari,bool silent=false);
};
//---------------------------------------------------------------------------
#endif
