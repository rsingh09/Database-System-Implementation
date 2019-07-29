#include "Join.h"

Join::Join()
{

}

Join::~Join()
{

}

void Join::Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal)
{
    inPipe_L = &inPipeL;
    inPipe_R = &inPipeR;
    outPipe_ = &outPipe;
    selOp_ = &selOp;
    literal_ = &literal;

    pthread_create(&workerThread, NULL, RunWorker, (void *) this);
}

void* Join::RunWorker(void *op)
{
    Join *join = static_cast<Join*>(op);
    join->DoWork();
    join->outPipe_->ShutDown();

    pthread_exit(NULL);
}

void Join::DoWork()
{
    OrderMaker *left = new OrderMaker();
    OrderMaker *right = new OrderMaker();

    //cout << "Started the Join thread" << endl;
    if(selOp_->GetSortOrders(*left, *right) == 0)
    {
        //block-nested loops join
        BlockNestedLoopJoin();
    }
    else
    {
        //sort merge join
        SortMergeJoin(left, right);
    }

    delete left;
    delete right;
    
}


void Join::SortMergeJoin(OrderMaker *left, OrderMaker *right)
{
    //OutPipes for BigQ
    Pipe pipeL(1000);
    Pipe pipeR(1000);

    //Check if the pipes empty
    bool isLEmpty = false;
    bool isREmpty = false;

    //Constructing BigQ to sort the input pipes
    BigQ bigL(*inPipe_L, pipeL, *left, runlen);
    BigQ bigR(*inPipe_R, pipeR, *right, runlen);

    //Memory to hold the current records
    Record *leftRec = new Record;
    Record *rightRec = new Record;

    //Initialising the empty vars
    isLEmpty = (0 == pipeL.Remove(leftRec));
    isREmpty = (0 == pipeR.Remove(rightRec));

    vector<Record*> leftVec;
    vector<Record*> rightVec;

    //ComparisonEngine to compare the records
    ComparisonEngine comp;
    while(!isLEmpty && !isREmpty)
    {
        int result = comp.Compare(leftRec, left, rightRec, right);
        if(result < 0)
        {
            isLEmpty = (0 == pipeL.Remove(leftRec));
        }
        else if(result > 0)
        {
            isREmpty = (0 == pipeR.Remove(rightRec));
        }
        else
        {
            
            //Logic to join and merge the records
            isLEmpty = GetCommonRecords(pipeL, leftRec, leftVec, left);//Assign return value to isLEmpty
            isREmpty = GetCommonRecords(pipeR, rightRec, rightVec, right);

            for(int i=0; i < leftVec.size(); i++)
            {
                for(int j=0; j<rightVec.size(); j++)
                {
                   CreateAndInsertRecords(leftVec[i], rightVec[j]);
                }
            }
        }
        
    }

    Record temp;
    while(pipeL.Remove(&temp));
    while(pipeR.Remove(&temp));
}


bool Join::GetCommonRecords(Pipe &pipe, Record *rec, vector<Record*> &vecOfRecs, OrderMaker *sortOrder)
{
    for(int i = 0 ; i < vecOfRecs.size(); i++)
        delete vecOfRecs[i];

    vecOfRecs.clear();
    ComparisonEngine comp;

    vecOfRecs.push_back(new Record);
    vecOfRecs[vecOfRecs.size()-1]->Consume(rec);

    bool isEmpty = (0 == pipe.Remove(rec));
    //Handle case where pipe becomes empty, return True and assign it to isLEmpty
    while(!isEmpty && comp.Compare(vecOfRecs[0], rec, sortOrder) == 0)
    {
        vecOfRecs.push_back(new Record);
        vecOfRecs[vecOfRecs.size()-1]->Consume(rec);
        isEmpty = (0 == pipe.Remove(rec));  
    }

    return isEmpty;
}

void Join::CreateAndInsertRecords(Record *left, Record *right)
{
    Record *toPipe = new Record;

    int numAttsLeft = left->GetNumAtts();
    int numAttsRight = right->GetNumAtts();
    int numAttsTotal = numAttsLeft + numAttsRight;

    int * attsToKeep = new int[numAttsTotal];
    int index = 0;

    for (int i = 0; i < numAttsLeft; i++){
        attsToKeep[index++] = i;
    }

    for (int i = 0; i < numAttsRight; i++){
        attsToKeep[index++] = i;
    }
    // Merge the left and right records
    toPipe->MergeRecords(left, right, numAttsLeft, numAttsRight,
	    	       attsToKeep, numAttsTotal, numAttsLeft);

    delete[] attsToKeep;

    outPipe_->Insert(toPipe);

}

void Join::BlockNestedLoopJoin()
{
    DBFile lFile;

    //Creating a heap file to store the records from the Left Pipe
    char *path = "temp.bin";
    lFile.Create(path, heap, NULL);

    //Insert all the records from the Left Pipe into the DBFile
    Record *recToFile = new Record;
    while(inPipe_L->Remove(recToFile))
    {
        lFile.Add(*recToFile);
    }

    Record recFromFile;
    Record recFromPipe;
    ComparisonEngine comp;
    while(inPipe_R->Remove(&recFromPipe))
    {
        while(lFile.GetNext(recFromFile))
        {
            if(!comp.Compare(&recFromPipe, &recFromFile, literal_, selOp_))
            {
                CreateAndInsertRecords(&recFromPipe, &recFromFile);
            }
        }

        lFile.MoveFirst();
    }

    remove(path);
    return;

}


void Join::Use_n_Pages (int n)
{
    runlen = n;
}

void Join::WaitUntilDone ()
{
    pthread_join(workerThread, NULL);
}