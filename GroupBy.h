#ifndef GROUPBY_H
#define GROUPBY_H
#include "RelOp.h"
#include "Function.h"

class GroupBy : public RelationalOp {

public:

	GroupBy();
	~GroupBy();

	void Run(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);
	void WaitUntilDone();
	void Use_n_Pages(int n) { _runLength = n; }

private:

	static void* RunWorker(void *op);
	void DoWork();
	void CreateEntryandInsert(Record * prevRecord, int numOfAttsOld, int * attListNew, int intSum, double doubleSum, Type type);
	Pipe *_inPipe;
	Pipe *_outPipe;
	OrderMaker *_groupAtts;
	Function *_computeMe;
	int _runLength;
	
};
#endif