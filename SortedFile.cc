#include <iostream>
#include <stdlib.h>
#include "SortedFile.h"
#include "string.h"
#include "unistd.h"

SortedFile::SortedFile()
{
	readBuffer = new Page();
	currentPageNum = 0;
}

SortedFile::SortedFile(void * startup)
{
	sortInfo = (SortInfo*)startup;
	readBuffer = new Page();
	currentPageNum = 0;
}

void SortedFile::deleteQ()
{
	delete bigQ;
	bigQ = NULL;
	delete inPipe;
	delete outPipe;
}

int SortedFile::Open(const char *f_path)
{
	mainFile = new File();
	mainFile->Open(1, (char *)f_path);
	if (mainFile->GetLength() > 0)
		mainFile->GetPage(readBuffer, 0);
	fileMode = Read;
	strcpy(filePath, f_path);
	return 1;
}
int SortedFile::Create(const char *f_path, fType f_type, void *startup)
{
	mainFile = new File();
	mainFile->Open(ZERO, (char *)f_path);
	sortInfo = (SortInfo*)startup;
	strcpy(filePath, f_path);
	fileMode = Read;
	return 1;
}

/*
	Same as described about Add function, but loop through the file in the loadpath.
*/
void SortedFile::Load(Schema & myschema, char * loadpath)
{
	if (fileMode == Read || bigQ == NULL)
	{
		inPipe = new Pipe(PIPESIZE);
		outPipe = new Pipe(PIPESIZE);
		bigQ = new BigQ(*inPipe, *outPipe, *(sortInfo->myOrder), sortInfo->runLength);
		fileMode = Write;
	}

	FILE *fileToLoad = fopen(loadpath, "r");
	Record *tempRec = new Record;
	while (tempRec->SuckNextRecord(&myschema, fileToLoad) == 1)
	{
		inPipe->Insert(tempRec);
		tempRec = new Record;
	}

	fclose(fileToLoad);
	MoveFirst();

}
void SortedFile::MoveFirst()
{
	readBuffer->EmptyItOut();
	if (fileMode == Write) {
		MergeRecords();
		deleteQ();
		fileMode = Read;
	}
	currentPageNum = ZERO;
	mainFile->GetPage(readBuffer, (off_t)currentPageNum);
}

/*
	Check if the file mode is write. If yes, then the BigQ is initialized and it should have an input pipe.
	Anyway, just checking for failproof.

	If the file mode is read, then we change it to write, initialize the BigQ and the input and output pipes.
	Send the record to the input pipe
*/
void SortedFile::Add(Record & addme)
{
	if (fileMode == Read || bigQ == NULL)
	{
		inPipe = new Pipe(PIPESIZE);
		outPipe = new Pipe(PIPESIZE);
		bigQ = new BigQ(*inPipe, *outPipe, *(sortInfo->myOrder), sortInfo->runLength);
		fileMode = Write;
	}

	inPipe->Insert(&addme);
}

/*
	Read the next record from the Buffer page.
*/
int SortedFile::GetNext(Record & fetchme)
{
	int numOfpages = GETLENGTH(mainFile->GetLength());
	if (fileMode == Write) {
		MergeRecords();
		deleteQ();
		fileMode = Read;
	}

	if (readBuffer->GetFirst(&fetchme) == 0)
	{
		if (++currentPageNum < numOfpages)
		{
			mainFile->GetPage(readBuffer, (off_t)currentPageNum);
			return readBuffer->GetFirst(&fetchme);
		}
	}
	if (currentPageNum < numOfpages)
		return 1;
	return 0;
}


/*
	1. Check if the sortedInfo CNF matches with the sortOrder info in the SortedFile.
		- Build an instance of the ordermaker from the cnf passed(query ordermaker).
		- Need sortOrder from sortInfo, CNF, queryOrder
		- For each attribute in the sortOrder in sortInfo
			* Check if the attribute is present in any of the subexpressions in the CNF instance.
			* If this attribute is present in the CNF instance and it is the only attribute present in its
				subexpression and the CNF says that it is comparing that attribute with a literal value using
				an equality check, then we add it to the end of the query maker that we are creating.
			* As soon as we find any attribute in the file's sort order ordermaker that is not present
				in the CNF instance that we are trying to evaluate, we have to stop building up the query order
				maker because past that point, we can't make use of the sort order that the file provides.
	2. If the sort order matches, then do a binary search for the record in the sortedFile.
		- Consider all the records that come after the current record
*/
int SortedFile::GetNext(Record &fetchme, CNF &cnf, Record &literal)
{
	OrderMaker queryOrderMaker, cnfOrderMaker;
	ComparisonEngine cmp;
	int numOfAtts = OrderMaker::prepareQueryOrder(*(sortInfo->myOrder), cnf, queryOrderMaker, cnfOrderMaker);
	if(numOfAtts == 0)
	{
		//while we have a record in the page
		while (GetNext(fetchme) != ZERO)
		{
			//if there is a match return the record and exit
			if (cmp.Compare(&fetchme, &literal, &cnf))
			{
				return 1;
			}
		}
	}
	/*
	Got the queryOrderMaker, now run the binary search to find the record that is
	less than, greater than or equal to the literal record with respect to  both the sortfile order
	and the cnf passed
	*/
	if (!binarySearch(fetchme, cnfOrderMaker, literal, *(sortInfo->myOrder), queryOrderMaker))
		return 0;
	do {
		if (cmp.Compare(&fetchme, &queryOrderMaker, &literal, &cnfOrderMaker) > 0)
			return 0;
		if (cmp.Compare(&fetchme, &literal, &cnf))
			return 1;
	} while (GetNext(fetchme));

	return 0;
}


int SortedFile::binarySearch(Record& fetchme, OrderMaker& cnfOrder, Record& literal, OrderMaker& sortOrder, OrderMaker& queryOrder)
{
	//Reached the end of the file and there are no more records
	if (GetNext(fetchme) == 0)
		return 0;
	
	ComparisonEngine comp;
	int result = comp.Compare(&fetchme, &sortOrder, &literal, &cnfOrder);
	if ( result	> 0)
		return 0;
	if (result == 0)
		return 1;

	//What about the equality case?

	int low = currentPageNum, high = GETLENGTH(mainFile->GetLength());
	int mid = (low + high) / 2;
	while (low < mid)
	{
		
		mid = (low + high) / 2;
		mainFile->GetPage(readBuffer, mid);
		GetNext(fetchme);
		int result = comp.Compare(&fetchme, &queryOrder, &literal, &cnfOrder);
		if (result < 0)
			low = mid + 1;
		else if (result > 0)
			high = mid - 1;
		else
			high = mid;

	}
	//Getting the low-1 page
	mainFile->GetPage(readBuffer, low);
	currentPageNum = low;
	do
	{
		if (!GetNext(fetchme))
			return 0;
		result = comp.Compare(&fetchme, &queryOrder, &literal, &cnfOrder);

	} while (result < 0);

	return result == 0;
}


int SortedFile::Close()
{
	if (fileMode == Write) {
		MergeRecords();
		fileMode = Read;
		deleteQ();
		bigQ = NULL;
	}
	mainFile->Close();
	delete readBuffer;
	delete mainFile;
	return 0;
}

void SortedFile::MergeRecords()
{
	int count = 0;
	inPipe->ShutDown();
	string TempFilePath = "tmpFile.bin";
	string MainFilePath = filePath;
	Page *TempPageBuffer = new Page();
	Page *TempWriteBuffer = new Page();
	int WriteBufferIndex = 0;
	ComparisonEngine comp;

	int index = 0;
	int numOfPages = GETLENGTH(mainFile->GetLength());
	Record *r1 = new Record();
	Record *r2 = new Record();
	Record *TempRecordToWrite;
	bool EOFile = false;
	bool EmptyPipe = false;
	bool changePipe = true;
	File *TempFile = new File();
	TempFile->Open(ZERO, (char *)TempFilePath.c_str());
	//Initialize the values in the temp records
	if (numOfPages > 0)
	{
		mainFile->GetPage(TempPageBuffer, index);
		TempPageBuffer->GetFirst(r1);
	}
	else {
		EOFile = true;
	}
	if (outPipe->Remove(r2) == 0)
	{
		EmptyPipe = true;
	}
	while (!EOFile || !EmptyPipe)
	{
		//if data is present in file and in the pipe compare and select the smaller value to write in the Pipe
		if ((!EOFile) && (!EmptyPipe))
		{
			if (comp.Compare(r1, r2, sortInfo->myOrder) < 0)
			{
				TempRecordToWrite = r1;
				changePipe = false;
			}
			else
			{
				TempRecordToWrite = r2;
				changePipe = true;
			}
		}
		//if pipe is empty and file is not pick record from file to write
		else if ((!EOFile) && EmptyPipe)
		{
			TempRecordToWrite = r1;
			changePipe = false;
		}

		//if file is empty and pipe is not pick record from pipe to write
		else if ((!EmptyPipe) && EOFile)
		{
			TempRecordToWrite = r2;
			changePipe = true;
		}

		//Append the record in the buffer page, if page is full than write to the file and create new page
		count++;
		if (TempWriteBuffer->Append(TempRecordToWrite) == 0)
		{
			TempFile->AddPage(TempWriteBuffer, GETLENGTH(TempFile->GetLength()));
			TempWriteBuffer->EmptyItOut();
			TempWriteBuffer->Append(TempRecordToWrite);
			WriteBufferIndex++;
		}
		//Get the next setfor comparision
		if (changePipe)
		{
			if (outPipe->Remove(r2) == 0)
				EmptyPipe = true;
		}
		else
		{
			if (TempPageBuffer->GetFirst(r1) == 0)
			{
				if (++index < numOfPages)
				{
					mainFile->GetPage(TempPageBuffer, index);
					TempPageBuffer->GetFirst(r1);
				}
				else
				{
					EOFile = true;
				}
			}
			/*else
				EOFile = true;*/
		}
	}
	//Write last buffer page
	TempFile->AddPage(TempWriteBuffer, GETLENGTH(TempFile->GetLength()));
	WriteBufferIndex++;
	outPipe->ShutDown();
	remove(filePath);
	TempFile->Close();
	mainFile->Close();
	delete mainFile;
	delete TempFile;
	mainFile = new File();
	rename((char *)TempFilePath.c_str(), filePath);
	mainFile->Open(1, filePath);
	delete TempPageBuffer;
	delete TempWriteBuffer;
	delete r1;
	delete r2;
	//delete TempRecordToWrite;
}


SortedFile::~SortedFile()
{}