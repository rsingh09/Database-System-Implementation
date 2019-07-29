#include "Sum.h"
Sum::Sum (){

}

Sum::~Sum (){

}

void Sum::Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe){

  inPipe_ = &inPipe;
  outPipe_ = &outPipe;
  computeMe_ = &computeMe;

  pthread_create(&workerThread, NULL, RunWorker, (void*) this);

}

void* Sum::RunWorker (void *op){

  Sum *sum = static_cast<Sum*>(op);

  sum->DoWork();

  pthread_exit(NULL);

}

void Sum::DoWork()
{

    Type type;
    Record inRec;// = new Record;
    Record *outRec = new Record;

    int intResult = 0;
    double doubleResult = 0.0;

    int intSum = 0;
    double doubleSum = 0.0;
    while(inPipe_->Remove(&inRec))
    {
        type = computeMe_->Apply(inRec, intResult, doubleResult);

        intSum += intResult;
        doubleSum += doubleResult;
    }
    //Got the sum, construct the record
    //Create a new Schema - SUM and construct the record on it.
    Attribute *sum_att = new Attribute;
    sum_att->name = "sum_sch";
    sum_att->myType = type;
    char *fileName = "tempSchemaFile";
    Schema sum_schema(fileName, 1, sum_att );

    //Creating the string version of the record
    std::stringstream s;
    if(type == INT)
        s << intSum;
    else
        s << doubleSum;

    s << "|";

    outRec->ComposeRecord(&sum_schema, s.str().c_str());

    outPipe_->Insert(outRec);
    outPipe_->ShutDown();

    delete outRec;
    //delete inRec;

    delete sum_att;

}

void Sum::WaitUntilDone () {
  pthread_join (workerThread, NULL);
}