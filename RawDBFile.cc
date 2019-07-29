#include <iostream>
#include <stdlib.h>
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "RawDBFile.h"

RawDBFile::RawDBFile() {

}

RawDBFile::~RawDBFile() {

}

int RawDBFile::Create(const char *fpath, fType file_type, void *startup) {
	cerr << "RawDBFile Called";
	return 1;
}

int RawDBFile::Open(const char *fpath) {
	cerr << "RawDBFile Called";
	return 1;
}

int RawDBFile::Close() {
	cerr << "RawDBFile Called";
	return 1;
}

void RawDBFile::Load(Schema &myschema, char *loadpath) {
	cerr << "RawDBFile Called";
}

void RawDBFile::MoveFirst() {
	cerr << "RawDBFile Called";
}

void RawDBFile::Add(Record &addme) {
	cerr << "RawDBFile Called";
}

int RawDBFile::GetNext(Record &fetchme) {
	cerr << "RawDBFile Called";
	return 1;
}

int RawDBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
	cerr << "RawDBFile Called";
	return 1;
}