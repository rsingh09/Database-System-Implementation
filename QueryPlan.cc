
#include <iostream>
#include "Statistics.h"
#include "ParseTree.h"
#include "QueryPlanTree.h"
#include "DbUtil.h"

using namespace std;

extern "C" {
	int yyparse(void);   // defined in y.tab.c
}

Attribute IntA = {"int",Int};
Attribute DobA = {"double",Double};
Attribute StrA = {"string",String};

extern struct FuncOperator *finalFunction;
extern struct TableList *tables;
extern struct AndList *boolean;
extern struct NameList *groupingAtts;
extern struct NameList *attsToSelect;
extern int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query 
extern int distinctFunc;// 1 if there is a DISTINCT in an aggregate query

using namespace std;

extern char *dbfile_dir;

void GetLoadPath(char *loadPath, char *table, char *prefix, char *extension);

int clear_pipe (Pipe &in_pipe, Schema *schema, bool print) {
	Record rec;
	int cnt = 0;
	while (in_pipe.Remove (&rec)) {
		if (print) {
			rec.Print (schema);
		}
		cnt++;
	}
	return cnt;
}

int clear_pipe (Pipe &in_pipe, Schema *schema, Function &func, bool print) {
	Record rec;
	int cnt = 0;
	double sum = 0;
	while (in_pipe.Remove (&rec)) {
		if (print) {
			rec.Print (schema);
		}
		//int ival = 0; double dval = 0;
		//func.Apply (rec, ival, dval);
		//sum += (ival + dval);
		cnt++;
	}
	//cout << " Sum: " << sum << endl;
	return cnt;
}


//Get the table name from the attribute
string getTableName(string tblAttr)
{
	//string tableName = op->value;
	size_t pos = tblAttr.find(".");
	return tblAttr.substr(0, pos);
}

void getJoinsAndWhereClauses(vector<AndList> &joinList, vector<AndList> &havingList, vector<AndList> &whereList)
{
	
	OrList *tblOrlist;
	while (boolean != 0)
	{
		//cout << "Here" <<endl;
		AndList tmpList = *boolean;
		tblOrlist = boolean->left;
		if (tblOrlist && tblOrlist->left->code == EQUALS && tblOrlist->left->left->code == NAME
			&& tblOrlist->left->right->code == NAME)
		{
			tmpList = *boolean;
			tmpList.rightAnd = 0;
			joinList.push_back(tmpList);
			//cout << "Value: " << endl;
		}
		else if (tblOrlist->left == 0)
		{
			tmpList.rightAnd = 0;
			whereList.push_back(tmpList);
		}
		else
		{
			vector<string> orTables;
			while (tblOrlist != 0)
			{
				Operand *op = tblOrlist->left->left;
				if (op->code != NAME)
					op = tblOrlist->left->right;
				string tableName = getTableName(op->value);
				if (find(orTables.begin(), orTables.end(), tableName) == orTables.end())
					orTables.push_back(tableName);
				tblOrlist = tblOrlist->rightOr;
			}
			if (orTables.size() > 1) {
				AndList newAnd = *boolean;
				newAnd.rightAnd = 0;
				havingList.push_back(newAnd);
			}
			else {
				AndList newAnd = *boolean;
				newAnd.rightAnd = 0;
				whereList.push_back(newAnd);
			}
		}
		boolean = boolean->rightAnd;
	}

}

//Join Optimization - Currently using the leftist-tree optimization
vector<AndList> joinOptimization(vector<AndList> joins, Statistics *stats)
{
	//The new list
	vector<AndList> optimizedList;

	//Reserve a vector of size joins.size()
	optimizedList.reserve(joins.size());
	int counter = 0;
	
	int selectedItr = 0;	
	//Loop until we get n-1 smallest join estimates
	while (joins.size() > 1)
	{	
		double currCost = -1;
		//iterate over all the joins, to get the smallest one.
		for (int i = 0; i < joins.size(); i++)
		{
			double newCost = 0.0;
			string tbl1 = getTableName(joins[i].left->left->left->value);
			string tbl2 = getTableName(joins[i].left->left->right->value);
			char* tblList[] = { (char*)tbl1.c_str(), (char*)tbl2.c_str() };
			newCost = stats->Estimate(&joins[i], tblList, 2);

			//If it is the first element or a smaller element, store the index
			if (currCost == -1 || currCost > newCost)
			{
				currCost = newCost;
				selectedItr = i;
			}
		}
		//Get the smallest index from the current join list
		optimizedList.push_back(joins[selectedItr]);
		joins.erase(joins.begin() + selectedItr);
		counter++;
	}
	//Insert the remaining node, which is the highest node, thereby inserting at the last
	optimizedList.insert(optimizedList.begin()+counter, joins[0]);
	return optimizedList;
}

void getPlan(int output, string outputFile)
{
    int pipeID = 1;
	cout << "Enter the Query followed by Ctrl + D:" << endl;
	Statistics *stats = new Statistics();
	stats->Read("Statistics.txt");
	//yyparse();
	cout << "After statistics" << endl;

	//Get the list of the table and create leaf nodes
	TableList *tempTblList = tables;
	vector<string> tableList;
	map<string, QueryPlanTree*> leafNodes;
	QueryPlanTree *lastInserted = NULL;

	while (tempTblList)
	{
		
		//Create a select file node for each table in the query
		QueryPlanTree *tmpNode = NULL;
		if (tempTblList->aliasAs != 0)
		{
			tableList.push_back(tempTblList->aliasAs);
			leafNodes.insert(std::pair<string, QueryPlanTree*>(tempTblList->aliasAs, new QueryPlanTree()));
			stats->CopyRel(tempTblList->tableName, tempTblList->aliasAs);
		}
		else
		{
			tableList.push_back(tempTblList->tableName);
			leafNodes.insert(std::pair<string, QueryPlanTree*>(tempTblList->tableName, new QueryPlanTree()));
		}
		tmpNode = leafNodes[tempTblList->aliasAs];


		tmpNode->nodeSchema = new Schema("catalog", tempTblList->tableName);
		if(tempTblList->aliasAs != 0)
		{
			tmpNode->nodeSchema->updateName(string(tempTblList->aliasAs));
		}

		char binPath[100];
		GetLoadPath(binPath, tempTblList->tableName, dbfile_dir, "bin");	

		tmpNode->path = binPath;

		tmpNode->outputPipes.pipeID = pipeID++;
		cout << "Settign the path " << binPath << endl;
		tmpNode->setType(SELECTFILE);
		tempTblList = tempTblList->next;
		lastInserted = tmpNode;

	}
	//Generated leaf nodes
	/*
	Now get the list of and and or and joins
	*/
	vector<AndList> joinlist;
	vector<AndList> havingList;
	vector<AndList> whereList;

	getJoinsAndWhereClauses(joinlist, havingList, whereList);

	//Filled every thing
	//Start building the tree adding the where
	for (unsigned i = 0; i < whereList.size(); i++)
	{
		cout << "The size of where list is " << whereList.size() << endl;
		QueryPlanTree *tmpNode;
		string tablename;
		if (whereList[i].left->left->left->code == NAME)
		{
			tablename = getTableName(whereList[i].left->left->left->value);
		}
		else
			tablename = getTableName(whereList[i].left->left->right->value);
		tmpNode = leafNodes[tablename];
		while (tmpNode->parent != NULL)
		{
			tmpNode = tmpNode->parent;
		}
		QueryPlanTree *newNode = new QueryPlanTree(tmpNode, SELECTPIPE, &whereList[i], pipeID++);
		tmpNode->parent = newNode;
		char *statTbl = strdup(tablename.c_str());
		stats->Apply(&whereList[i], &statTbl, 1);
		//topnode
		lastInserted = newNode;
	}
	if (joinlist.size() > 1) {
		joinlist = joinOptimization(joinlist, stats);
	}

	// Add the joins as node
	AndList curJoin;
	for (int i = 0; i < joinlist.size(); i++)
	{
		QueryPlanTree *leftNode;
		QueryPlanTree *rightNode;
		//string tbl1;
		//string tbl2;
		//stats->GetRelation(joinlist[i].left->left->left, tbl1);
		//stats->GetRelation(joinlist[i].left->left->right, tbl2);
		string tbl1 = getTableName(joinlist[i].left->left->left->value);
		string tbl2 = getTableName(joinlist[i].left->left->right->value);

		leftNode = leafNodes[tbl1];
		rightNode = leafNodes[tbl2];
		while (leftNode->parent != NULL)
			leftNode = leftNode->parent;
		while (rightNode->parent != NULL){
			rightNode = rightNode->parent;
		}

		QueryPlanTree *newNode = new QueryPlanTree(leftNode, rightNode, JOIN, &joinlist[i], pipeID++);
	
		//newNode->leftChild = leftNode;
		//newNode->rightChild = rightNode;
		leftNode->parent = newNode;
		rightNode->parent = newNode;
		//topnode
		lastInserted = newNode;

	}
	// add having clause part
	for (int i = 0; i < havingList.size(); i++)
	{
		cout << "Adding the having list " << endl;
		QueryPlanTree *tmpNode = lastInserted;
		QueryPlanTree *newNode = new QueryPlanTree(tmpNode, SELECTPIPE, &havingList[i], pipeID++);
		tmpNode->parent = newNode;
		lastInserted = newNode;
	}
	if (finalFunction != NULL)
	{
		//Check for  duplicate removal
		//Distinct in an aggregate query
		if (distinctFunc == 1)
		{
			QueryPlanTree *tmpNode = lastInserted;
			QueryPlanTree *newNode = new QueryPlanTree(tmpNode, DUPLICATEREMOVAL, pipeID++);
			tmpNode->parent = newNode;
			lastInserted = newNode;
		}

		if (groupingAtts == 0)
		{
			cout << " Creating the sum node " << endl;
			QueryPlanTree *tmpNode = lastInserted;
			QueryPlanTree *newNode = new QueryPlanTree(tmpNode, SUM, finalFunction, pipeID++);
			tmpNode->parent = newNode;
			Schema *sch = new Schema("sum_sch", 1, &DobA);
			newNode->nodeSchema = sch;

			lastInserted = newNode;
		}
		else
		{
			cout << "Creating the GroupBy Node" << endl;
			QueryPlanTree *tmpNode = lastInserted;
			QueryPlanTree *newNode = new QueryPlanTree(tmpNode, GROUPBY, finalFunction, pipeID++);
			tmpNode->parent = newNode;
			newNode->GenerateOrderMaker(groupingAtts);
			lastInserted = newNode;
		}
	}
	//Creating distinct nodes
	if (distinctAtts != 0)
	{
		QueryPlanTree *tmpNode = lastInserted;
		QueryPlanTree *newNode = new QueryPlanTree(tmpNode, DUPLICATEREMOVAL, pipeID++);
		tmpNode->parent = newNode;
		lastInserted = newNode;
	}

	//Creating Project nodes
	if (attsToSelect != NULL) {

		QueryPlanTree *tmpNode = lastInserted;

		cout << " Creating the PROJECT  NOde " << endl;
		QueryPlanTree *newNode = new QueryPlanTree(tmpNode, PROJECT, pipeID++);
		tmpNode->parent = newNode;
		newNode->GenerateSchema(attsToSelect);
		NameList *attsTraverse = attsToSelect;
		string attribute;
		vector<int> indexOfAttsToKeep;
		Schema *oldSchema = tmpNode->nodeSchema;

		while(attsTraverse != 0)
		{
			attribute = attsTraverse->name;

			indexOfAttsToKeep.push_back(oldSchema->Find(const_cast<char*>(attribute.c_str())));
			attsTraverse = attsTraverse->next;
		}

		Schema *newSchema = new Schema(oldSchema, indexOfAttsToKeep);
		newNode->nodeSchema = newSchema;

		newNode->numAttsIn = tmpNode->nodeSchema->GetNumAtts();
		newNode->numAttsOut = newNode->nodeSchema->GetNumAtts();
		newNode->aTK = indexOfAttsToKeep;

		newNode->setType(PROJECT);

		lastInserted = newNode;
	}


	//Finally create the write node to get the records from the pipe

    /*
	if(lastInserted != NULL)
	{
		lastInserted->PrintPostorder();
	}
    */

   if(output == PIPE_NONE)
   {
	   if(lastInserted != NULL)
	   {
		   lastInserted->PrintPostorder();
	   }
   }
	else if(output == PIPE_STDOUT)
	{
		lastInserted->PrintPostorder();
		cout << "*************************************************" << endl;
		cout << "ATTEMPTING TO RUN TREE" << endl;
		lastInserted->RunInOrder();
		int count = 0;
	
		cout << "Schema being returned is " << endl;
		lastInserted->nodeSchema->Print();
		if(lastInserted->type == SUM)
			count = clear_pipe(*(lastInserted->outputPipes.queryPipe), lastInserted->nodeSchema, *lastInserted->func, true);
		else
			count = clear_pipe(*(lastInserted->outputPipes.queryPipe), lastInserted->nodeSchema, true);
		lastInserted->WaitUntilDone();
		clog << "Number of returned rows: " << count << endl;
	}
	else if(output == PIPE_FILE)
	{
	   //Write to the file
	}

   // return lastInserted;	
}



