#include "WriteOut.h"

WriteOut::WriteOut (){

}

WriteOut::~WriteOut (){

}

void WriteOut::Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) {

  inPipe_ = &inPipe;
  outFile_ = outFile;
  mySchema_ = &mySchema;

  pthread_create(&workerThread, NULL, RunWorker, (void*) this);

}

void* WriteOut::RunWorker (void *op){

  WriteOut *print = static_cast<WriteOut*>(op);

  print->DoWork();

  pthread_exit(NULL);

}

void WriteOut::DoWork (){

    if(outFile_ == NULL)
        exit(1);

    std::stringstream s;
    Record *inRec = new Record;
    while(inPipe_->Remove(inRec) && !feof(outFile_) && !ferror(outFile_))
    {
        //write the record to the stringstream
        inRec->Print(mySchema_, s);

        //Move the content from the stream to the file
        fputs(s.str().c_str(), outFile_);

        //resetting the stream
        s.str("");
    }

    delete inRec;
}


void WriteOut::WaitUntilDone () {
  pthread_join (workerThread, NULL);
}