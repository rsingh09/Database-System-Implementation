#include <stdio.h>
#include "DbUtil.h"
#include "DBFile.h"
#include <fstream>
#include <iostream>
#include <cstdlib>
//#include "QueryPlan.h"

using namespace std;

extern "C" {
    int yyparse(void);
}


extern struct TableList *tables;
extern struct AndList *boolean;
extern struct NameList *groupingAtts;
extern struct NameList *attsToSelect;
extern struct NameList *sortAtts;
extern int distinctAtts;
extern int distinctFunc;

extern struct InOutPipe *io;

extern string createTableName;
extern string createTableType;
extern vector<string> atts;
extern vector<string> attTypes;

extern int queryType;

// donot change this information here
char *catalog_path, *dbfile_dir, *tpch_dir = NULL;

void getPlan(int output, string outputfile);

void ReadFileLocations()
{
    const char *locations = "test.cat";
    //Open the locations to read the folder paths
    FILE *fp = fopen(locations, "r");


    if(fp)
    {
        //Create memory 80 bytes for each line to store the folder paths
        //for catalog, dbfile, tpch
        char *mem = (char *) malloc(80 * 3);
        catalog_path = &mem[0];
		dbfile_dir = &mem[80];
		tpch_dir = &mem[160];
		char line[80];

        fgets (line, 80, fp);
		sscanf (line, "%s\n", catalog_path);
		fgets (line, 80, fp);
		sscanf (line, "%s\n", dbfile_dir);
		fgets (line, 80, fp);
		sscanf (line, "%s\n", tpch_dir);
		fclose (fp);
		if (! (catalog_path && dbfile_dir && tpch_dir)) {
			cerr << " Test settings file 'test.cat' not in correct format.\n";
			free (mem);
			exit (1);
		}
    }
    else
    {
        cerr << " Test settings files 'test.cat' missing \n";
		exit (1);
    }
    cout << " \n** PLEASE VERIFY THE BELOW INFORMATION **\n";
	cout << " catalog location: \t" << catalog_path << endl;
	cout << " tpch files dir: \t" << tpch_dir << endl;
	cout << " bin files dir: \t" << dbfile_dir << endl;
	cout << " \n\n";
}

void GetLoadPath(char *loadPath, char *table, char *prefix, char *extension){
    sprintf(loadPath, "%s%s.%s", prefix, table, extension);
}



void RemoveTableFromCatalog(string table)
{
    bool copyData = true;

    string line;
    string accumulator = "";
    ifstream catalog("catalog");
    ofstream new_catalog("new_catalog");


    if(catalog.is_open())
    {
        while(catalog.good())
        {
            getline(catalog, line);

            if (line.compare("BEGIN") == 0 && accumulator.compare("") != 0)
            {
                if(new_catalog.good() && copyData)
                {
                    new_catalog << accumulator;
                }

                accumulator = "";
                accumulator.append(line);
                accumulator.append("\n");
            }
            else if(copyData == false && line.compare("END") == 0)
            {
                copyData = true;
                continue;
            }
            else if(line.compare(table) == 0)
            {
                accumulator = "";
                copyData = false;
            }
            else if(copyData)
            {
                accumulator.append(line);
                accumulator.append("\n");
            }
        }//While catalog


        if(new_catalog.good() && copyData)
        {
            new_catalog << accumulator;
        }

        rename("new_catalog", "catalog");
        catalog.close();

        char *binary = new char[100];
        GetLoadPath(binary, (char*)table.c_str(), dbfile_dir, "bin");
        remove(binary);
        delete[] binary;
    }
}


void createTable()
{
    string schema;
    string type;
    int size = (int) atts.size();

    ofstream create("catalog", ios::out | ios::app);

    //Initial lines in the catalog
    schema = "BEGIN\n";

    //The first two lines
    //TableName
    //TableName.bin
    schema.append(createTableName.c_str());
    schema.append("\n");
    schema.append(createTableName.c_str());
    schema.append(".tbl\n");

    //Attribute AttrType
    for (int i = 0; i < size; i++)
    {
        schema.append(atts[i]);

        if (attTypes[i].compare("INTEGER") == 0){
            type = " Int";
        }
        else if (attTypes[i].compare("DOUBLE") == 0){
            type = " Double";
        }
        else {
            type = " String";
        }

    schema.append(type);
    schema.append("\n");

  }

  schema.append("END\n\n");

    //Write to the catalog
  if(create.is_open() && create.good())
  {
      create << schema;
  }

  create.close();
  
  char *binary = new char[100];
  cout << "table Type:" << (char*)createTableType.c_str() << endl;
  DBFile dbFile;
  //Load the path to the binary file
     GetLoadPath(binary, (char*) createTableName.c_str(), dbfile_dir, "bin");
	 struct NameList *sortkeys = sortAtts;
     fType ftype = heap;

     if(sortAtts != NULL && sortAtts != 0)
     {
		 int i = 0;
		 OrderMaker om = OrderMaker();
		 int attList[MAX_ANDS];
		 Type attType[MAX_ANDS];
		 cout << "in sorted" <<endl;
		 Schema *s = new Schema("catalog", (char*)createTableName.c_str());
		 while (sortkeys != NULL)
		 {
			 int res = s->Find(sortkeys->name);
			 int restype = s->FindType(sortkeys->name);
			 if (res != -1)
			 {
				 attList[i] = res;
				 attTypes[i] = restype;
				 i++;
			 }
			 sortkeys = sortkeys->next;
		 }
		 om.SaveAttributes(i, attList, attType);
		 struct { OrderMaker *o; int l; } startup = { &om, 5 };
         ftype = sorted;
		 dbFile.Create(binary, ftype, &startup);
     }
	 else
		 dbFile.Create(binary, ftype, NULL);
     dbFile.Close();

     delete[] binary;
}

int main()
{
    bool quit = false;

    int output = PIPE_STDOUT;
    string outputFile = "";

    DBFile table;
    char *loadpath;

    //Initialize the table and bin folders
    ReadFileLocations();

    cout << "Done reading the catalog, bin and tpch file locations" << endl;

   // while(!quit)
   // {
        cout << "Please enter a query and press Ctrl+D to execute" << endl;
		//cin;
		cin.clear();
		fflush(stdin);
		cin.sync();
        //Parse the input
	
        yyparse();


        //If the query is select
        if(queryType == QUERY_CREATE)
        {
            createTable();
            cout << "Table " << createTableName.c_str() << " created " << endl;
            atts.clear();
            attTypes.clear();
			//continue;
        }

        else if(queryType == QUERY_SELECT)
        {
			cout << "in select " << endl;
            getPlan(output, outputFile);
        }

        else if(queryType == QUERY_DROP)
        {
            RemoveTableFromCatalog(tables->tableName);

            cout << " Table " << tables->tableName << " is dropped " << endl;
            free(tables);
        }
        else if(queryType == QUERY_SET)
        {
            output = io->type;
            if(output == PIPE_FILE)
            {
                outputFile = io->file;
            }

            free(io);
        }
		else if (queryType == QUERY_INSERT)
		{
			loadpath = new char[100];
			char *file = new char[100];
			GetLoadPath(loadpath,io->src,dbfile_dir,"bin");

			Schema toLoad("catalog", io->src);
			ifstream newFile(io->file);

            cout << "io->file is " << io->file << endl;
            cout << "load path is" << loadpath << endl;
			if (newFile.is_open())
			{
				table.Open(loadpath);
                cout << "OPened the load path file " << loadpath << endl;
				table.Load(toLoad, io->file);
				table.Close();
				cout << "Data loaded: " << io->src << " From: " << io->file << "." << endl;
			}
			else
			{
				cout << "File is missing" << endl;
			}
			newFile.close();
			free(io);
			delete[] loadpath;
			delete[] file;
		}
        else if(queryType == QUERY_QUIT)
        {
            quit = true;
            cout << "Shutting down the database" << endl;
        }
   // }//end while

    return 0;

}