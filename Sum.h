#ifndef SUM_H
#define SUM_H
#include "RelOp.h"
#include "Function.h"
#include <iostream>
#include <sstream>


class Sum : public RelationalOp {
	public:
        Sum();
        ~Sum();
	    void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe);
	    void WaitUntilDone ();
	    void Use_n_Pages (int n) {/*Pages are not needed for SUM */;}

    private:
        Pipe *inPipe_;
        Pipe *outPipe_;
        Function *computeMe_;
        static void* RunWorker (void *op);
        void DoWork ();
};

#endif