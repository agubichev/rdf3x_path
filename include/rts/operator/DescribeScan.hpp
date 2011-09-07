#ifndef H_rts_operator_DescribeScan
#define H_rts_operator_DescribeScan
//---------------------------------------------------------------------------
// RDF-3X
// (c) 2011 Thomas Neumann, Andrey Gubichev. Web site: http://www.mpi-inf.mpg.de/~neumann/rdf3x
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
//---------------------------------------------------------------------------
#include "rts/operator/Operator.hpp"
#include "cts/codegen/CodeGen.hpp"
#include "rts/segment/FactsSegment.hpp"
#include "rts/database/Database.hpp"
#include <vector>
//---------------------------------------------------------------------------
class Register;
//---------------------------------------------------------------------------
/// A scan that returns some information about the literal (or URI)
class DescribeScan : public Operator
{
	private:
	/// The input
	Operator* input;
	/// The output
	CodeGen::Output output;
	/// The registers for the different parts of the triple
	Register* value1,*value2,*value3;
	/// The facts segments
	FactsSegment &factsSPO,&factsOPS;
	/// The scans
	FactsSegment::Scan scanSPO, scanOPS;
	/// Alternate between SPO and OPS scans
	bool switchScan;
	/// Go to next input element?
	bool nextInput;

	/// Possible states while scanning the input
	enum ScanState {
	      needNewTuple, checkSPO, checkOPS,
	      scanningSPO, scanningOPS
	};
    /// The current scan state
    ScanState scanState;


	public:
	/// Constructor
	DescribeScan(Database& db,Operator* input,const CodeGen::Output &output,Register* value1,Register* value2,Register* value3,double expectedOutputCardinality);

	/// Destructor
	~DescribeScan();

	/// Produce the first tuple
	unsigned first();
	/// Produce the next tuple
	unsigned next();

	/// Print the operator tree. Debugging only.
	void print(PlanPrinter& out);
	/// Add a merge join hint
	void addMergeHint(Register* reg1,Register* reg2);
	/// Register parts of the tree that can be executed asynchronous
	void getAsyncInputCandidates(Scheduler& scheduler);

};
#endif
