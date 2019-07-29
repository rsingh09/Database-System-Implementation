#ifndef JOIN_H
#define JOIN_H
#include "RelOp.h"
#include "Comparison.h"
#include <vector>

using std::vector;

class Join : public RelationalOp { 
    public:
        Join();
        ~Join();
	    void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
	    void WaitUntilDone ();
	    void Use_n_Pages (int n);

    private:
        Pipe *inPipe_L;
        Pipe *inPipe_R;
        Pipe *outPipe_;
        CNF * selOp_;
        Record *literal_;

        int runlen;


        static void* RunWorker(void *op);
        void DoWork();

        void SortMergeJoin(OrderMaker *left, OrderMaker *right);
        bool GetCommonRecords(Pipe &pipe, Record *rec, vector<Record*> &vec, OrderMaker *sortOrder);
        void CreateAndInsertRecords(Record *l, Record *r);
        void BlockNestedLoopJoin();

};

#endif