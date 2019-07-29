#ifndef RELATION_H
#define RELATION_H

#include <string>
#include <map>
#include <iterator>
#include <sstream>
#include <cstdlib>
#include <iostream>
#include <fstream>
#include <sstream>

using std::map;
using std::endl;
using std::string;
using std::ofstream;
using std::ifstream;
using std::stringstream;




using namespace std;

class Relation
{
	public:
	Relation()
	{

	}
	Relation(int tuples)
	{
		numTuples = tuples;
	}

	int getNumTuples(){
		return numTuples;
	}

	int getAttr(std::string attr)
	{
		return relAttributes[attr];
	}

	void setNumTuples(int tuples)
	{
	    numTuples = tuples;
	}

    int size()
    {
        return relAttributes.size();
    }

	void addAttr(string attr, int tuples)
	{
        if(tuples == -1)
            tuples = numTuples;
		relAttributes[attr] = tuples;
	}
	
    map<string, int> getAttributes()
    {
        return relAttributes;
    }

    void copy(Relation newRelation)
    {
        this->numTuples = newRelation.getNumTuples();
        this->relAttributes.clear();
        this->relAttributes = newRelation.getAttributes();
    }

	void writeToFile(ofstream &writer)
	{
		//First write the number of tuples to the file
		writer << numTuples << endl;
		writer << relAttributes.size() << endl;
		map<string,int>::iterator iter;
		//Iterate over the attrs and write each one to the file
		for(iter = relAttributes.begin(); iter != relAttributes.end(); iter++)
		{
			writer << iter->first << endl << iter->second << endl;
		}
	}

	void readFromFile(ifstream &reader)
	{
		string line;
		int numAtts;
		//if the line read is a new line, read the next line
		stringstream strStream("");
		//Get the first line
		getline(reader, line);
		strStream.str(line);

		if(!(strStream >> numTuples))
		{
			numTuples = 0;
		}

		strStream.clear(); strStream.str("");

		//Now read the number of attributes.
		getline(reader, line);
		strStream.str(line);

		if(!(strStream >> numAtts))
		{
			numAtts = 0;
		}

		strStream.clear(); strStream.str("");


		string attName;
		int distincts;

		for(int i=0; i < numAtts; i++)
		{
			//Get the attribute name
			getline(reader, line);
			attName = line;

			//Get the number of distincts for the attribute
			getline(reader, line);
			strStream.str(line);
			if(!(strStream >> distincts))
			{
				distincts = 0;
			}

			strStream.clear(); strStream.str("");

			//Store the data in the map
			relAttributes[attName] = distincts;
		}
	}

	private:
	int numTuples;
	//Map from the attribute name to the number of the distincts in the relation under this attribute
	map<string, int> relAttributes;
};

#endif