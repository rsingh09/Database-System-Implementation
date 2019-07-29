#ifndef DUPLICATEREMOVAL_H
#define DUPLICATEREMOVAL_H
#include "RelOp.h"
#include "Function.h"
class DuplicateRemoval : public RelationalOp
{
public:
	DuplicateRemoval();
	~DuplicateRemoval();	void WaitUntilDone();
	void Use_n_Pages(int n) { _runLength = n; }
	void Run(Pipe & inPipe, Pipe & outPipe, Schema & mySchema);
private:

	static void* RunWorker(void *op);
	void DoWork();
	Pipe *_inPipe;
	Pipe *_outPipe;
	Schema *_mySchema;
	OrderMaker *myOrder;
	int _runLength;
};

#endif

