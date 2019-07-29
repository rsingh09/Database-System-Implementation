#ifndef HEAPFILE_H
#define HEAPFILE_H

#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "RawDBFile.h"
#include "DBFile.h"


class HeapFile : public RawDBFile {
private:
	File *mainFile;
	Page *bufferPage; //this buffer contains page we are going to write in the file
	Page *readBuffer; //this buffer contains page we are reading from the file
	bool isDirty; //identify if the buffer is empty or full
	int index;
	void WritePage(); //Writes the buffer page into file 

public:
	HeapFile();
	~HeapFile();

	int Create( const char *fpath, fType file_type, void *startup);
	int Open( const char *fpath);
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
};
#endif
