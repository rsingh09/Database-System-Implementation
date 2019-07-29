#ifndef PROJECT_H
#define PROJECT_H
#include "RelOp.h"
#include "Function.h"


class Project : public RelationalOp {

public:

	Project();
	~Project();

	void Run(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
	void WaitUntilDone();
	void Use_n_Pages(int n) {/*No needed to use pages in this case*/; }

private:

	static void* RunWorker(void *op);
	void DoWork();

	Pipe *_inPipe;
	Pipe *_outPipe;
	int *_keepMe;
	int _numAttsInput;
	int _numAttsOutput;

};

#endif