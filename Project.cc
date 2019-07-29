#include "Project.h"

Project::Project() {

}

Project::~Project() {

}

void Project::Run(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) {

	_inPipe = &inPipe;
	_outPipe = &outPipe;
	_keepMe = keepMe;
	_numAttsInput = numAttsInput;
	_numAttsOutput = numAttsOutput;
	pthread_create(&workerThread, NULL, RunWorker, (void*)this);

}

void * Project::RunWorker(void *op)
{
	Project *select = static_cast<Project*>(op);
	select->DoWork();
	pthread_exit(NULL);
}


void Project::DoWork()
{
	Record *tempRec = new Record;
	ComparisonEngine cp;
	while (_inPipe->Remove(tempRec))
	{
		tempRec->Project(_keepMe, _numAttsOutput, _numAttsInput);
		_outPipe->Insert(tempRec);
	}
	
	delete tempRec;
	_outPipe->ShutDown();
}

void Project::WaitUntilDone() {
	pthread_join(workerThread, NULL);
}
