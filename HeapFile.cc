#include <iostream>
#include <stdlib.h>
#include "HeapFile.h"
#include "string.h"



HeapFile::HeapFile() {
	index = ZERO;
	isDirty = false;

}

HeapFile::~HeapFile() {
	Close();
}


int HeapFile::Create(const char *f_path, fType f_type, void *startup) {
	mainFile = new File();
	mainFile->Open(ZERO, (char *)f_path);
	bufferPage = new Page();
	readBuffer = new Page();
	index = ZERO;
	return 1;
}


int HeapFile::Open(const char *f_path) {
	mainFile = new File();
	mainFile->Open(1, (char *)f_path);
	bufferPage = new Page();
	readBuffer = new Page();
	index = ZERO;
	if(mainFile->GetLength() > 0)
		mainFile->GetPage(readBuffer, index);
	return 1;
}

void HeapFile::Load(Schema &f_schema, char *loadpath) {
	FILE *fileToLoad = fopen(loadpath, "r");
	Record tempRecord;
	isDirty = true;
	while (tempRecord.SuckNextRecord(&f_schema, fileToLoad) == 1) {
		Add(tempRecord);
	}
	WritePage();
	cout << "Load done. Number of Pages: " << mainFile->GetLength() << endl;
	fclose(fileToLoad);
}

void HeapFile::WritePage()
{
	int totalPages = mainFile->GetLength();
	mainFile->AddPage(bufferPage, GETLENGTH(totalPages));
	bufferPage->EmptyItOut();
	isDirty = false;
}

int HeapFile::Close() {
	//If there are records in write buffer, write them to the file and then close
	if(isDirty)
		WritePage();
	mainFile->Close();

	//Deleting the memory
	delete mainFile;
	delete bufferPage;
	delete readBuffer;

	//Assigning the variables to NULL
	mainFile = NULL;
	bufferPage = NULL;
	readBuffer = NULL;

	//Making the index 0
	index = 0;

	return 1;
}

void HeapFile::MoveFirst() {
	//mainFile->moveFirst
	readBuffer->EmptyItOut();
	off_t numOfPages = mainFile->GetLength();
	//Check if the write buffer contain any record, if yes first write buffer into the file
	//if (numOfPages = (off_t) ZERO)
	if(isDirty)
	{
		WritePage();
	}
	//Get the first page of the file into read buffer
	mainFile->GetPage(readBuffer, (off_t)ZERO);
	index = ZERO;
}

void HeapFile::Add(Record &rec) {

	off_t numOfPages = mainFile->GetLength();
	isDirty = true;
	if ((bufferPage)->Append(&rec) == ZERO) {
		//Append Failed, write the bufferPage to disk, empty the bufferPage and append again
		mainFile->AddPage(bufferPage, GETLENGTH(numOfPages));
		if (numOfPages == ZERO)
		{
			index = ZERO;
			mainFile->GetPage(readBuffer, index);
		}
		bufferPage->EmptyItOut();
		bufferPage->Append(&rec);

		return;
	}
	return;
}

int HeapFile::GetNext(Record &fetchme) {
	//get file length
	off_t numOfPages = mainFile->GetLength();
	//using move first we got the 1st page in read buffer and hence skipping that step and getting the first record from the page subsequently deleting it
	if (readBuffer->GetFirst(&fetchme) == 1)
	{
		return 1;
	}
	//if above function return 0 then change  the page if index is still less than the page
	else if (++index < numOfPages-1)
	{	
		mainFile->GetPage(readBuffer, (off_t)index);
		return readBuffer->GetFirst(&fetchme);
	}
	//if index = page i.e. reached the last page so add the write buffer page in file if write buffer is not empty
	else if (isDirty)
	{
		WritePage();
		mainFile->GetPage(readBuffer, (off_t)index);
		return readBuffer->GetFirst(&fetchme);
	}
	return 0;
}


int HeapFile::GetNext(Record &fetchme, CNF &cnf, Record &literal)
{
	//first get the record on the file
	//Reset the readBuffer if there is no matching record in the rest of the file.
	ComparisonEngine comp;
	//while we have a record in the page
	while (GetNext(fetchme) != ZERO)
	{
		//if there is a match return the record and exit
		if (comp.Compare(&fetchme, &literal, &cnf))
		{
			return 1;
		}
	}
	//if reached end of file return 0
	return 0;
}