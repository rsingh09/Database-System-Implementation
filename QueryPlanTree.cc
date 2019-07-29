#include "QueryPlanTree.h"
#include <iostream>
#include <stdlib.h> 

using namespace std;

QueryPlanTree :: QueryPlanTree()
{
	parent = NULL;
	rightChild = NULL;
	leftChild = NULL;
	nodeSchema = NULL;
	operatorCNF = NULL;
	order = NULL;
	cnf = NULL;
	func = NULL;
	literal = new Record();
	funcOpr = NULL;
	leftPipe.pipeID = 0;
	leftPipe.queryPipe = NULL;

	rightPipe.pipeID = 0;
	rightPipe.queryPipe = NULL;

	outputPipes.pipeID = 0;
	outputPipes.queryPipe = NULL;
}

QueryPlanTree::QueryPlanTree(QueryPlanTree * lChild, NodeType nodetype, AndList *list, int opPipeID)
{
	leftPipe.pipeID = 0;
	leftPipe.queryPipe = NULL;

	rightPipe.pipeID = 0;
	rightPipe.queryPipe = NULL;

	outputPipes.pipeID = 0;
	outputPipes.queryPipe = NULL;
	operatorCNF = NULL;

	nodeSchema = lChild->nodeSchema;
	literal = new Record();
	cnf = list;
	setType(nodetype);
	cout << "assigned the cnf" << endl;
	PrintCNF();
	leftChild = lChild;
	rightChild = NULL;
	leftPipe.pipeID = lChild->outputPipes.pipeID;
	leftPipe.queryPipe = lChild->outputPipes.queryPipe; 

	outputPipes.pipeID = opPipeID;
	parent = NULL;
}

QueryPlanTree::QueryPlanTree(QueryPlanTree * lChild, NodeType nodetype, FuncOperator *opr, int opPipeID)
{

	leftPipe.pipeID = 0;
	leftPipe.queryPipe = NULL;

	rightPipe.pipeID = 0;
	rightPipe.queryPipe = NULL;

	outputPipes.pipeID = 0;
	outputPipes.queryPipe = NULL;

	nodeSchema = lChild->nodeSchema;
	operatorCNF = NULL;
	literal = new Record();
	cnf = NULL;
	setType(nodetype);
	funcOpr = opr;
	leftChild = lChild;
	leftPipe.pipeID = lChild->outputPipes.pipeID;
	leftPipe.queryPipe = lChild->outputPipes.queryPipe;

	outputPipes.pipeID = opPipeID;
	rightChild = NULL;
	
	func = new Function();
	func->GrowFromParseTree(funcOpr, *nodeSchema);
	parent = NULL;
}
QueryPlanTree::QueryPlanTree(QueryPlanTree * lChild, NodeType nodetype,  int opPipeID)
{
	leftPipe.pipeID = 0;
	leftPipe.queryPipe = NULL;

	rightPipe.pipeID = 0;
	rightPipe.queryPipe = NULL;

	outputPipes.pipeID = 0;
	outputPipes.queryPipe = NULL;
	operatorCNF = NULL;
	cnf = NULL;

	nodeSchema = lChild->nodeSchema;
	literal = new Record();
	if(nodetype != PROJECT)
		setType(nodetype);
	//funcOpr = opr;
	leftChild = lChild;
	leftPipe.pipeID = lChild->outputPipes.pipeID;
	leftPipe.queryPipe = lChild->outputPipes.queryPipe;

	outputPipes.pipeID = opPipeID;
	parent = NULL;
	rightChild = NULL;
}
QueryPlanTree :: QueryPlanTree(QueryPlanTree * lChild, QueryPlanTree * rChild, NodeType nodetype, AndList * list,int opPipeID)
{

	leftPipe.pipeID = 0;
	leftPipe.queryPipe = NULL;

	rightPipe.pipeID = 0;
	rightPipe.queryPipe = NULL;

	outputPipes.pipeID = 0;
	outputPipes.queryPipe = NULL;

	cnf = list;
	//type = nodetype;
	literal = new Record();
	leftPipe.pipeID = lChild->outputPipes.pipeID;
	rightPipe.pipeID = rChild->outputPipes.pipeID;
	leftPipe.queryPipe = lChild->outputPipes.queryPipe;
	rightPipe.queryPipe = rChild->outputPipes.queryPipe;

	operatorCNF = NULL;
	outputPipes.pipeID = opPipeID;

	leftChild = lChild;
	rightChild = rChild;
	parent = NULL;
	if (nodetype == JOIN){
		mergeSchema("Join");

			 string tbl1 = getTableName(cnf->left->left->left->value);
				string tbl2 = getTableName(cnf->left->left->right->value);
				//cout << "Joining the table " << tbl1 << " with table " << tbl2 << endl;
				//Schema sch("catalog", (char *) tbl1.c_str());
				//Schema sch1("catalog", (char *) tbl2.c_str());
				operatorCNF = new CNF();
				operatorCNF->GrowFromParseTree(cnf, leftChild->nodeSchema, rightChild->nodeSchema, *literal);


	}
	setType(nodetype);
}

void QueryPlanTree::RunInOrder(){
	if(NULL != leftChild){
		leftChild->RunInOrder();
	}
	if(NULL != rightChild){
		rightChild->RunInOrder();
	}
	Run();
}

void QueryPlanTree::Run(){

	switch (type){

		case SELECTFILE:
			cout << "Running select file" << endl;
			sf->Run(*dbFile, *outputPipes.queryPipe, *operatorCNF, *literal);
		    break;

		case SELECTPIPE:
			cout << "Running select pipe" << endl;
			sp->Run(*leftPipe.queryPipe, *outputPipes.queryPipe, *operatorCNF, *literal);
		    break;

		case PROJECT:
			cout << "Running project" << endl;
			p->Run(*leftPipe.queryPipe, *outputPipes.queryPipe, attsToKeep, numAttsIn, numAttsOut);
		    break;

		case JOIN:
			cout << "Running join" << endl;
			j->Run(*leftPipe.queryPipe, *rightPipe.queryPipe, *outputPipes.queryPipe, *operatorCNF, *literal);
		    break;

		case SUM:
			cout << "Running SUM " << endl;
			s->Run(*leftPipe.queryPipe, *outputPipes.queryPipe, *func);
		  break;

		case GROUPBY:
		cout << "RUnning Groupby" << endl;
		//void Run(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);
			gb->Run(*leftPipe.queryPipe, *outputPipes.queryPipe, *order, *func );
			break;

		case DUPLICATEREMOVAL:
			cout << "Running duplicate removal " << endl;
			d->Run(*leftPipe.queryPipe, *outputPipes.queryPipe, *nodeSchema);
		    break;

		case WRITEOUT:

		    break;
		} // end switch

}

void QueryPlanTree::WaitUntilDone(){
	switch (type){

		case SELECTFILE:
			sf->WaitUntilDone();
		    break;

		case SELECTPIPE:
			sp->WaitUntilDone();
		    break;

		case PROJECT:
			p->WaitUntilDone();
		    break;

		case JOIN:
			j->WaitUntilDone();
		    break;

		case SUM:
			s->WaitUntilDone();
		    break;

		case GROUPBY:
			gb->WaitUntilDone();
		  break;

		case DUPLICATEREMOVAL:
			d->WaitUntilDone();
		  break;

		case WRITEOUT:

		    break;
	} // end switch
}


void QueryPlanTree::setType(NodeType setter){

	type = setter;

	outputPipes.queryPipe = new Pipe(PIPE_SIZE);
	switch (type){

	    case SELECTFILE:
				dbFile = new DBFile();
				dbFile->Open((char*)(path.c_str()));

				sf = new SelectFile();
				sf->Use_n_Pages(PIPE_SIZE);	
				break;

	    case SELECTPIPE:
				sp = new SelectPipe();
				operatorCNF = new CNF();
				operatorCNF->GrowFromParseTree(cnf, nodeSchema, *literal);
				sp->Use_n_Pages(PIPE_SIZE);
	      break;

	    case PROJECT:
				p = new Project();
				numAttsToKeep = (int) aTK.size();
				attsToKeep = new int[numAttsToKeep]; //Need to set this to the, well, indicies of the attributes we're keeping
				for (int i = 0; i < numAttsToKeep; i++){
		  			attsToKeep[i] = aTK[i];
				}	
				p->Use_n_Pages(PIPE_SIZE);
				break;

	    case JOIN:

	      j = new Join();
				j->Use_n_Pages(PIPE_SIZE);
	      break;

	    case SUM:

	      s = new Sum();
				s->Use_n_Pages(PIPE_SIZE);
				break;

	    case GROUPBY:

	     	gb = new GroupBy();
				gb->Use_n_Pages(PIPE_SIZE);
	     	break;

	    case DUPLICATEREMOVAL:

	     	d = new DuplicateRemoval();
				d->Use_n_Pages(PIPE_SIZE);
	     	break;

	    case WRITEOUT:
	      break;
	  } // end switch
}


void QueryPlanTree::mergeSchema(string schemaName)
{
	int numOfAttr = leftChild->nodeSchema->GetNumAtts() + rightChild->nodeSchema->GetNumAtts();
	Attribute *leftAttr = leftChild->nodeSchema->GetAtts();
	Attribute *rightAttr = rightChild->nodeSchema->GetAtts();
	Attribute mergedAtt[numOfAttr];
	int index = 0;
	for (int i = 0; i < leftChild->nodeSchema->GetNumAtts(); i++)
		mergedAtt[index++] = leftAttr[i];
	for (int i = 0; i < rightChild->nodeSchema->GetNumAtts(); i++)
		mergedAtt[index++] = rightAttr[i];
	nodeSchema = new Schema((char*)schemaName.c_str(), numOfAttr, mergedAtt);
}

void QueryPlanTree::PrintNode(){
  cout << " ************NODE START*********** " << endl;
  //cout << GetTypeName() << " operation" << endl;

  switch (type){

    case SELECTFILE:
		cout << "SELECT FILE OPERATION" << endl;
		cout << "INPUT PIPE " << leftPipe.pipeID << endl;
		cout << "OUTPUT PIPE " << outputPipes.pipeID << endl;
		cout << "OUTPUT SCHEMA: " << endl;
		nodeSchema->Print();
		//cout << "SELECT FILE CNF:" << endl;
		PrintCNF();
    	break;

    case SELECTPIPE:
		cout << "SELECT PIPE OPERATION" << endl;
		cout << "INPUT PIPE " << leftPipe.pipeID << endl;
		cout << "OUTPUT PIPE " << outputPipes.pipeID << endl;
		cout << "OUTPUT SCHEMA: " << endl;
		nodeSchema->Print();
		cout << "SELECTION CNF :" << endl;
		PrintCNF();
    	break;

    case PROJECT:
		cout << "PROJECT" << endl;
		cout << "INPUT PIPE " << leftPipe.pipeID << endl;
		cout << "OUTPUT PIPE "<< outputPipes.pipeID << endl;
		cout << "OUTPUT SCHEMA: " << endl;
		nodeSchema->Print();
     	break;

    case JOIN:
		cout << "JOIN" << endl;
		cout << "LEFT INPUT PIPE " << leftPipe.pipeID << endl;
		cout << "RIGHT INPUT PIPE " << rightPipe.pipeID << endl;
		cout << "OUTPUT PIPE " << outputPipes.pipeID << endl;
		cout << "OUTPUT SCHEMA: " << endl;
		nodeSchema->Print();
		cout << endl << "JOIN CNF: " << endl;
		PrintCNF();
		cout << endl;
    	break;

    case SUM:
		cout << "SUM" << endl;
		cout << "LEFT INPUT PIPE " << leftPipe.pipeID << endl;
		cout << "OUTPUT PIPE " << outputPipes.pipeID << endl;
		cout << "OUTPUT SCHEMA: " << endl;
		nodeSchema->Print();
		cout << endl << "FUNCTION: " << endl;
		func->Print();
    	break;

    case DUPLICATEREMOVAL:
		cout << "DISTINCT" << endl;
		cout << "LEFT INPUT PIPE " << leftPipe.pipeID << endl;
		cout << "OUTPUT PIPE " << rightPipe.pipeID << endl;
		cout << "OUTPUT SCHEMA: " << endl;	
		nodeSchema->Print();
		cout << endl << "FUNCTION: " << endl;
		func->Print();
		break;

    case GROUPBY:
		cout << "GROUPBY" << endl;
		cout << "LEFT INPUT PIPE " << leftPipe.pipeID << endl;
		cout << "OUTPUT PIPE " << outputPipes.pipeID << endl;
		cout << "OUTPUT SCHEMA: " << endl;	
		nodeSchema->Print();
		cout << endl << "GROUPING ON " << endl;
		order->Print();
		cout << endl << "FUNCTION " << endl;
		func->Print();
    	break;

    case WRITEOUT:
		cout << "WRITE" << endl;
		cout << "LEFT INPUT PIPE " << leftPipe.pipeID << endl;
		cout << "OUTPUT FILE " << path << endl;
    	break;

  } // end switch type

  cout << " ************NODE END*********** " << endl;
} 


void QueryPlanTree::PrintPostorder()
{
	if(NULL != leftChild){
		leftChild->PrintPostorder();
	}
	if(NULL != rightChild){
		rightChild->PrintPostorder();
	}
	PrintNode();
}

/*void QueryPlanTree::GenerateOrderMaker(int numAtts, int* whichAtts, Type* whichTypes)
{
	order = new OrderMaker();
	order->SaveAttributes(numAtts, whichAtts, whichTypes);
}*/

void QueryPlanTree::GenerateOrderMaker(NameList *groupAttr)
{
	int numAttsToGroup = 0;
	vector<int> attsToGroup;
	vector<Type> whichType;
	while (groupAttr) {

		numAttsToGroup++;

		attsToGroup.push_back(nodeSchema->Find(groupAttr->name));
		whichType.push_back(nodeSchema->FindType(groupAttr->name));
		cout << "GROUPING ON " << groupAttr->name << endl;
		groupAttr = groupAttr->next;
	}

	order = new OrderMaker();

	order->SaveAttributes(numAttsToGroup, &attsToGroup[0], &whichType[0]);
}
void QueryPlanTree::GenerateSchema(NameList * selectAttr)
{
	vector<int> indexOfAttsToKeep;
	string attribute;
	while (selectAttr != 0) {
		attribute = selectAttr->name;
		//nodeSchema->Print();
		indexOfAttsToKeep.push_back(nodeSchema->Find(const_cast<char*>(attribute.c_str())));
		selectAttr = selectAttr->next;
		
	}
	Attribute *attr = nodeSchema->GetAtts();
	Attribute *newAttr = new Attribute[indexOfAttsToKeep.size()];
	for (int i = 0; i < indexOfAttsToKeep.size(); i++)
	{
		cout << i << endl;
		newAttr[i] = attr[indexOfAttsToKeep[i]];
	}
	nodeSchema = new Schema("Distinct", indexOfAttsToKeep.size(), newAttr);
}

void QueryPlanTree::PrintCNF()
{
	if(cnf)
	{
		struct AndList *curAnd = cnf;
		struct OrList *curOr;
		struct ComparisonOp *curOp;

		//For each AND
		while(curAnd)
		{
			curOr = curAnd->left;

			if(curAnd->left)
				cout << "(";
			//For each OR
			while(curOr)
			{
				curOp = curOr->left;
				if(curOp)
				{
					if(curOp->left)
						cout << curOp->left->value;

					switch ( curOp->code)
					{
						case LESS_THAN:
							cout << " < ";
							break;
						
						case GREATER_THAN:
							cout << " > ";
							break;
						
						case EQUALS:
							cout << " = ";
							break;
					}//End switch

					if(curOp->right)
						cout << curOp->right->value;
				}//End curOp

				if(curOr->rightOr)
					cout << " OR ";

				curOr = curOr->rightOr;

			}//End while curOr

			if(curAnd->left)
			{
				cout << ")";
			}

			if(curAnd->rightAnd)
			{
				cout << " AND ";
			}
			curAnd = curAnd->rightAnd;
		}//end while curAnd
	}//if cnf;

	cout << endl;
}
