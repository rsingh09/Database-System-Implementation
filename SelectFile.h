#include "RelOp.h"
#include "Function.h"

class SelectFile : public RelationalOp { 

public:

	SelectFile ();
	~SelectFile ();

	void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n){/*No needed to use pages in this case*/;}

private:

	static void* RunWorker (void *op);
	void DoWork ();

	DBFile *inFile_;
	Pipe *outPipe_;
	CNF *selOp_;
	Record *literal_;

};