#include "RelOp.h"
#include <iostream>
#include <sstream>
#include <stdio.h>
#include <fstream>

class WriteOut : public RelationalOp {

public:

	WriteOut();
	~WriteOut();

	void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema);
	void WaitUntilDone ();
	void Use_n_Pages (int n){/*Not needed for this functionality*/ ;}

private:

	static void* RunWorker (void *op);
	void DoWork ();

	Pipe *inPipe_;
	FILE *outFile_;
	Schema *mySchema_;

};