#include "GroupBy.h"
GroupBy::GroupBy() {

}

GroupBy::~GroupBy() {

}

void GroupBy::Run(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) {
	_inPipe = &inPipe;
	_outPipe = &outPipe;
	_groupAtts = &groupAtts;
	_computeMe = &computeMe;
	pthread_create(&workerThread, NULL, RunWorker, (void*)this);

}

void * GroupBy::RunWorker(void *op)
{
	GroupBy *select = static_cast<GroupBy*>(op);
	select->DoWork();
	pthread_exit(NULL);
}

void GroupBy::DoWork()
{
	Record *currRec = new Record;
	Record *prevRecord = NULL;

	ComparisonEngine cp;
	Pipe sortedRecords(PIPESIZE);
	BigQ big(*_inPipe, sortedRecords, *_groupAtts, _runLength);

	int numOfAttsOld = _groupAtts->GetNumOfAttributes();
	Type type;
	int attListOld[numOfAttsOld];
	int attListNew[1+numOfAttsOld];
	_groupAtts->GetAttributes(attListOld);
	//Create a new attribute list with Sum of grouping as the 1st element
	attListNew[0] = 0;
	for (int i = 0; i < numOfAttsOld; i++)
	{
		attListNew[i + 1] = attListOld[i];
	}

	int intSum = 0;
	double doubleSum = 0.0;

	//Get all the records that match with the CNF from the file
	while (sortedRecords.Remove(currRec))
	{
		int intResult = 0;
		double doubleResult = 0.0;
		if (prevRecord == NULL || cp.Compare(prevRecord,currRec,_groupAtts) != 0)
		{
			if (prevRecord != NULL)
			{
				CreateEntryandInsert(prevRecord, numOfAttsOld, attListNew, intSum, doubleSum, type);
				/*Attribute *sum_att = new Attribute;
				sum_att->name = "sum_sch";
				sum_att->myType = type;
				char *fileName = "tempSchemaFile";
				Schema sum_schema(fileName, 1, sum_att);    
				std::stringstream s;
				if (type == INT)
					s << intSum;
				else
					s << doubleSum;

				s << "|";
				tempRecord->ComposeRecord(&sum_schema, s.str().c_str());
				recordtoInsert->MergeRecords(tempRecord, prevRecord, 1, numOfAttsOld, attListNew, numOfAttsOld + 1, 1);
				_outPipe->Insert(recordtoInsert);*/
				intSum = 0;
				doubleSum = 0;
			}
			else
			{
				prevRecord = new Record;
			}
			
			type = _computeMe->Apply(*currRec, intResult, doubleResult);
			intSum = intResult;
			doubleSum = doubleResult;
			prevRecord->Consume(currRec);
		}
		else
		{
			type = _computeMe->Apply(*currRec, intResult, doubleResult);
			intSum += intResult;
			doubleSum += doubleResult;
			prevRecord->Consume(currRec);
		}
	}
	if(prevRecord != NULL)
		CreateEntryandInsert(prevRecord, numOfAttsOld, attListNew, intSum, doubleSum, type);
	//Shutdown the outPipe
	_outPipe->ShutDown();
}

void GroupBy::CreateEntryandInsert(Record *prevRecord,int numOfAttsOld,int *attListNew, int intSum, double doubleSum, Type type) {
	Record *tempRecord = new Record;
	Record *recordtoInsert = new Record();
	Attribute *sum_att = new Attribute;
	sum_att->name = "sum_sch";
	sum_att->myType = type;
	char *fileName = "tempSchemaFile";
	Schema sum_schema(fileName, 1, sum_att);
	std::stringstream s;
	if (type == INT)
		s << intSum;
	else
		s << doubleSum;

	s << "|";
	tempRecord->ComposeRecord(&sum_schema, s.str().c_str());
	recordtoInsert->MergeRecords(tempRecord, prevRecord, 1, numOfAttsOld, attListNew, numOfAttsOld + 1, 1);
	_outPipe->Insert(recordtoInsert);
}

void GroupBy::WaitUntilDone() {
	pthread_join(workerThread, NULL);
}
