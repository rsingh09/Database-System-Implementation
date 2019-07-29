#include "DuplicateRemoval.h"

DuplicateRemoval::DuplicateRemoval()
{
}

DuplicateRemoval::~DuplicateRemoval()
{
}
void DuplicateRemoval::Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema)
{
	_inPipe = &inPipe;
	_outPipe = &outPipe;
	_mySchema = &mySchema;
	myOrder = new OrderMaker(_mySchema);

	pthread_create(&workerThread, NULL, RunWorker, (void*) this);
}

void * DuplicateRemoval::RunWorker(void *op)
{
	DuplicateRemoval *select = static_cast<DuplicateRemoval*>(op);
	select->DoWork();
	pthread_exit(NULL);
}


void DuplicateRemoval::DoWork()
{
	Record *currRecord = new Record();
	Record *RecordtoInsert = new Record();
	Record *prevRecord = NULL;
	Pipe sortedRecords(PIPESIZE);
	ComparisonEngine cp;
	BigQ big(*_inPipe, sortedRecords, *myOrder, _runLength);
	while (sortedRecords.Remove(currRecord))
	{
		if (prevRecord == NULL || cp.Compare(prevRecord, currRecord, myOrder) != 0)
		{
			if(prevRecord == NULL)
				prevRecord = new Record;
			RecordtoInsert->Copy(currRecord);
			_outPipe->Insert(RecordtoInsert);
			prevRecord->Consume(currRecord);
		}
	}
	
	delete currRecord;
	delete RecordtoInsert;
	delete prevRecord;
	
	_outPipe->ShutDown();
}

void DuplicateRemoval::WaitUntilDone() {
	pthread_join(workerThread, NULL);
}
