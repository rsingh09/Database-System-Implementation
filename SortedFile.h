#ifndef SORTEDFILE_H
#define SORTEDFILE_H

#define GETLENGTH(totalPages) ((totalPages == 0) ? 0 : totalPages - 1)
#define ZERO 0

#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "RawDBFile.h"
#include "BigQ.h"
#define PIPESIZE 100

typedef enum { Read, Write } Mode;
class SortedFile : public RawDBFile {
private:
	Pipe *inPipe;
	Pipe *outPipe;
	File *mainFile;
	char filePath[200];
	//Page *bufferPage; //this buffer contains page we are going to write in the file
	Page *readBuffer; //this buffer contains page we are reading from the file
	BigQ *bigQ;
	SortInfo *sortInfo;
	Mode fileMode;
	int currentPageNum;
	void MergeRecords();
public:
	SortedFile();
	SortedFile(void *startup);
	int Open(const char *fpath);
	int Create(const char *fpath, fType file_type, void *startup);
	int Close();

	//Loads the records in the file using a source.
	void Load(Schema &myschema, char *loadpath);

	//Moves the pointer to the 1st page 1st record of the file
	void MoveFirst();

	//Add a record in the write buffer, if buffer is full writes the buffer page into file, emty it out and then appends the record into empty buffer
	void Add(Record &addme);

	//Sequentially fetches the next record present in the file, return 0 if reached the end of the file
	int GetNext(Record &fetchme);

	//Fetches the next record based on the CNF passed, returns 0 if reched the end of the file
	int GetNext(Record &fetchme, CNF &cnf, Record &literal);

	//deletes the memory assigned to BigQ and the Pipes
	void deleteQ();

	//Searching for the records using the Binary search with the QueryOrderMaker, CNF Order and the sort order of the file
	int binarySearch(Record& fetchme, OrderMaker& cnfOrder, Record& literal, OrderMaker& sortOrder, OrderMaker& queryOrder);
	~SortedFile();
};
#endif