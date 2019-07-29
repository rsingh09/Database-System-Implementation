#ifndef QUERYPLANTREE_H
#define QUERYPLANTREE_H

#include "Schema.h"
#include "Record.h"
#include "Pipe.h"
#include "Function.h"
#include "DBFile.h"
#include "SelectFile.h"
#include "SelectPipe.h"
#include "Join.h"
#include "GroupBy.h"
#include "Project.h"
#include "Sum.h"
#include "DuplicateRemoval.h"
#include "WriteOut.h"
#include <vector>
#include "Record.h"
#include "Function.h"

#define PIPE_SIZE 100
//Duplicate removal is distinct
//enum NodeType { SELECTPIPE, SELECTFILE, PROJECT, JOIN, SUM, GROUPBY, WRITEOUT, DISTINCT };
enum NodeType { SELECTPIPE, SELECTFILE, PROJECT, JOIN, DUPLICATEREMOVAL, SUM, GROUPBY, WRITEOUT };
//Get the table name from the attribute
string getTableName(string tblAttr);
extern struct NameList *attsToSelect;
struct QueryPipes
{
	Pipe *queryPipe;
	int pipeID;
};
class QueryPlanTree
{
public:
	NodeType type;
	QueryPlanTree *parent;
	QueryPlanTree *rightChild;
	QueryPlanTree *leftChild;
	QueryPipes leftPipe;
	QueryPipes rightPipe;
	QueryPipes outputPipes;
	Schema *nodeSchema;
	OrderMaker *order;
	string binPath;
	AndList *cnf;
	string path;
	Function *func;
	FuncOperator *funcOpr;

	SelectFile *sf;
	SelectPipe *sp;
	Join *j;
	GroupBy *gb;
	Project *p;
	Sum *s;
	DuplicateRemoval *d;
	WriteOut *h;
	

	DBFile *dbFile;
	//For project
	int numAttsIn;
	int numAttsOut;
	vector<int> aTK;
	int *attsToKeep;
	int numAttsToKeep;
	
	CNF *operatorCNF;
	Record *literal;

	void RunInOrder();
	void Run();
	void WaitUntilDone();

	QueryPlanTree();
	QueryPlanTree(QueryPlanTree * lChild, NodeType nodetype, int opPipeID);
	QueryPlanTree(QueryPlanTree * lChild, NodeType nodetype, FuncOperator *selFunc, int opPipeID);
	QueryPlanTree(QueryPlanTree * leftChild, NodeType nodetype, AndList *list, int opPipeID);
	QueryPlanTree(QueryPlanTree * leftChild, QueryPlanTree * rightChild, NodeType nodetype, AndList * list, int opPipeID);

	void mergeSchema(string schemaName);
	void PrintNode();
	void PrintPostorder();
	void PrintCNF();
	void setType(NodeType type);

	//void GenerateOrderMaker(int numAtts, int *whichAtts, Type *whichTypes);
	void GenerateOrderMaker(NameList * groupAttr);
	void GenerateSchema(NameList * selectAttr);
};

#endif