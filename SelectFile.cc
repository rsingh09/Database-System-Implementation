#include "SelectFile.h"
SelectFile::SelectFile (){

}

SelectFile::~SelectFile(){

}

void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {

  inFile_ = &inFile;
  outPipe_ = &outPipe;
  selOp_ = &selOp;
  literal_ = &literal;

  int num = pthread_create(&workerThread, NULL, RunWorker, (void*) this);

}

void * SelectFile::RunWorker(void *op)
{
    SelectFile *select = static_cast<SelectFile*>(op);
    select->DoWork();

    pthread_exit(NULL);
}


void SelectFile::DoWork()
{
   Record *readIn = new Record;
  Record *toPipe = new Record;
    // Put records into pipe so long as records are accepted
  if(!selOp_)
  {
    while(inFile_->GetNext(*readIn))
    {
      toPipe->Consume(readIn);
      outPipe_->Insert(toPipe);
    }
  }
  else
  {
    while(inFile_->GetNext(*readIn, *selOp_, *literal_)){
      toPipe->Consume(readIn);
      outPipe_->Insert(toPipe);
  } // end while outPipe  
  }
  


    // Clean up temp variables 
  delete readIn;
  delete toPipe;

    // Signal that pipe no longer is taking records
  outPipe_->ShutDown();
}

void SelectFile::WaitUntilDone () {
  pthread_join (workerThread, NULL);
}
