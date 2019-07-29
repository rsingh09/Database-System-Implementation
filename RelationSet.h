#ifndef RELATION_SET_H
#define RELATION_SET_H

#include <set>
#include <vector>
#include <iterator>
#include <string>
#include <sstream>
#include <cstdlib>
#include <iostream>
#include <fstream>
#include <sstream>

using std::endl;
using std::string;
using std::ofstream;
using std::ifstream;
using std::stringstream;



using namespace std;
//RelationSet is a set of all the string joined and the total number of the tuples in the join
class RelationSet
{
	public:
	RelationSet()
	{}


    RelationSet(string relation, double tuples)
	{
		joinedRelations.insert(relation);
		numOfTuples = tuples;
	}


	int intersect(RelationSet s)
	{
		RelationSet temp;
		int matches = 0;
		set<string>::iterator it = s.joinedRelations.begin();
		for(; it != s.joinedRelations.end(); it++)
		{
			if(this->joinedRelations.find(*it) != this->joinedRelations.end())
			{
				temp.joinedRelations.insert(*it);
			}
		}

		if(temp.joinedRelations.size() != 0 && (temp.joinedRelations.size() != this->joinedRelations.size()))
		{
			return -1;
		}
		return temp.joinedRelations.size();
	}

	int size()
	{
		return joinedRelations.size();
	}

	void updateTuples(double tuples)
	{
		numOfTuples = tuples;
	}

	void insertToSet(string s)
	{
		joinedRelations.insert(s);
	}

	double getNumOfTuples()
	{
		return numOfTuples;
	}

    set<string> getJoinedRelations()
    {
        return joinedRelations;
    }

    void copy(set<string> joined)
    {
        this->joinedRelations.clear();
        set<string>::iterator iter;
        for(iter =  joined.begin(); iter != joined.end() ; iter++)
        {
            this->joinedRelations.insert(*iter);
        }
    }

	void getRelations(vector<string> &relations)
	{
		set<string>::iterator it = joinedRelations.begin();
		for(; it != joinedRelations.end(); it++)
		{
			relations.push_back(*it);
		}
	}

	void writeToFile(ofstream &writer)
	{
		writer << numOfTuples << endl;
		writer << joinedRelations.size() << endl;

		set<string>::iterator iter;
		for(iter = joinedRelations.begin(); iter != joinedRelations.end(); iter++)
		{
			writer << *iter << endl;
		}
	}

    void readFromFile(ifstream &reader)
    {
        string line;
        std::stringstream strStream;

        int setSize = 0;

        //First get the number of tuples
        getline(reader, line);
        strStream.str(line);

        if(!(strStream >> numOfTuples))
        {
            numOfTuples = 0;
        }
        strStream.clear(); strStream.str("");

        //Now read the size of the set
        getline(reader, line);
        strStream.str(line);

        if(!(strStream >> setSize))
        {
            setSize = 0;
        }
        strStream.clear(); strStream.str("");
        //now read the relations set
        for(int i=0; i < setSize; i++)
        {
            getline(reader, line);
            joinedRelations.insert(line);
        }

        
    }

	private:
		set<string> joinedRelations;
		double numOfTuples;
};

#endif