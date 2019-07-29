#ifndef SELECTPIPE_H
#define SELECTPIPE_H
#include "RelOp.h"
#include "Function.h"

class SelectPipe : public RelationalOp {

public:

	SelectPipe();
	~SelectPipe();

	void Run(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone();
	void Use_n_Pages(int n) {/*No needed to use pages in this case*/; }

private:

	static void* RunWorker(void *op);
	void DoWork();

	Pipe *_inPipe;
	Pipe *_outPipe;
	CNF *_selOp;
	Record *_literal;

};

#endif