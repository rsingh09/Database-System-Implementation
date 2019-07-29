#include "SelectPipe.h"
SelectPipe::SelectPipe() {

}

SelectPipe::~SelectPipe() {

}

void SelectPipe::Run(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) {

	_inPipe = &inPipe;
	_outPipe = &outPipe;
	_selOp = &selOp;
	_literal = &literal;


	pthread_create(&workerThread, NULL, RunWorker, (void*)this);

}

void * SelectPipe::RunWorker(void *op)
{
	SelectPipe *select = static_cast<SelectPipe*>(op);
	select->DoWork();
	pthread_exit(NULL);
}

void SelectPipe::DoWork()
{
	Record *tempRec = new Record;
	ComparisonEngine cp;
	
	//Get all the records that match with the CNF from the file
	while (_inPipe->Remove(tempRec))
	{
		if (cp.Compare(tempRec, _literal, _selOp))
		{
			//Schema schem("catalog", "region");
			//tempRec->Print(&schem);
			_outPipe->Insert(tempRec);
		}
	}

	delete tempRec;
	//Shutdown the outPipe
	_outPipe->ShutDown();
}

void SelectPipe::WaitUntilDone() {
	pthread_join(workerThread, NULL);
}
