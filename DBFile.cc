#include <iostream>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <fstream>
#include "DBFile.h"
#include "Defs.h"


// stub file .. replace it with your own DBFile.cc

DBFile::DBFile() {

}

DBFile::~DBFile() {

}

int DBFile::Create(const char *f_path, fType f_type, void *startup) {
	char metaFileName[200];
	strcpy(metaFileName, (char *)f_path);
	strcat(metaFileName, "-meta.txt");

	ofstream metaFileStream;
	metaFileStream.open(metaFileName);
	if (f_type == fType::heap) {
		metaFileStream << "heap";
		rawDBFile = new HeapFile();
	}
	else if (f_type == fType::sorted)
	{
		metaFileStream << "sorted\n";
		SortInfo *si;
		si = (SortInfo*)startup;
		OrderMaker *om = si->myOrder;
		metaFileStream << si->runLength << endl;
		int attList[MAX_ANDS];
		Type attType[MAX_ANDS];
		/*Enter order maker values*/
		int numAttributes = om->GetNumOfAttributes();
		om->GetAttributes(attList, attType);
		metaFileStream << numAttributes << endl;
		rawDBFile = new SortedFile();
		for (int i = 0; i < numAttributes; i++)
		{
			metaFileStream << attList[i] << DELIMITERVAL;
			if (attType[i] == Int)
				metaFileStream << "Int" << endl;
			else if (attType[i] == Double)
				metaFileStream << "Double" << endl;
			else
				metaFileStream << "String" << endl;
		}
	}
	metaFileStream.close();
	return rawDBFile->Create(f_path, f_type, startup);
}

void DBFile::Load(Schema &f_schema, const char *loadpath) {
	rawDBFile->Load(f_schema, (char *)loadpath);
}

int DBFile::Open(const char *f_path) {
	char metaFileName[200];
	strcpy(metaFileName, (char *)f_path);
	strcat(metaFileName, "-meta.txt");

	ifstream metaFileStream;
	metaFileStream.open(metaFileName);
	string metaContent;
	if (metaFileStream.is_open()) {
		getline(metaFileStream, metaContent);
	}
	else {
		return 0;
	}
	//First line stores the type
	if (metaContent == "heap") {
		rawDBFile = new HeapFile();
	}
	else if (metaContent == "sorted")
	{
		OrderMaker *myOrder = new OrderMaker();
		SortInfo *si = new SortInfo();
		int attList[MAX_ANDS];
		Type attType[MAX_ANDS];
		int runLength, numAttributes;
		//if type is sorted then the 2nd line stores the runlength
		//Get runlength
		getline(metaFileStream, metaContent);
		runLength = atoi(metaContent.c_str());
		//Next line stores number of atrributes
		//Get num of attributes
		getline(metaFileStream, metaContent);
		numAttributes = atoi(metaContent.c_str());
		//Rest of the lines contains attributes and type seperated 
		//Get attributes and types
		for (int i = 0; i < numAttributes; i++)
		{
			string att, attTypeVal;
			getline(metaFileStream, metaContent);
			//cout << metaContent << endl;
			size_t position = metaContent.find(DELIMITERVAL);
			att = metaContent.substr(0,position);
			attList[i] = atoi(att.c_str());
			attTypeVal = metaContent.substr(position + 1);
			if (attTypeVal == "Int")
			{
				attType[i] = Int;
			}
			else if (attTypeVal == "Double")
				attType[i] = Double ;
			else
				attType[i] = String;
		}
		//Save the attributes of the order maker
		myOrder->SaveAttributes(numAttributes, attList, attType);
		//Assign the values of SortInfo and create an instance of Sorted file using it.
		si->runLength = runLength;
		si->myOrder = myOrder;
		rawDBFile = new SortedFile(si);
	}

	metaFileStream.close();
	return rawDBFile->Open(f_path);
}

void DBFile::MoveFirst() {
	rawDBFile->MoveFirst();
}

int DBFile::Close() {
	return rawDBFile->Close();
}

void DBFile::Add(Record &rec) {
	rawDBFile->Add(rec);
}

int DBFile::GetNext(Record &fetchme) {
	rawDBFile->GetNext(fetchme);
}

int DBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
	rawDBFile->GetNext(fetchme, cnf, literal);
}