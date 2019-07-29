#ifndef STATISTICS_H
#define STATISTICS_H

#include<bits/stdc++.h> 
#include "ParseTree.h"
#include <unistd.h>
#include <iterator> 
#include <map>
#include <vector>
#include <string>
#include <cstdlib>
#include <iostream>
#include <fstream>
#include <sstream>

#include "Relation.h"
#include "RelationSet.h"

using std::map;
using std::endl;
using std::vector;
using std::string;
using std::ofstream;
using std::ifstream;
using std::stringstream;



using namespace std;

//Statistics object contains sets of relation sets. Each set is a set of relations joined and the number of tuples in the join.
class Statistics
{
public:
	Statistics();
	Statistics(Statistics &copyMe);	 // Performs deep copy
	~Statistics();


	void AddRel(char *relName, int numTuples);
	void AddAtt(char *relName, char *attName, int numDistincts);
	void CopyRel(char *oldName, char *newName);
	
	void Read(char *fromWhere);
	void Write(char *fromWhere);

	/*
		Number of tuples after joining on a relation
		T(R join S) = (T(R)*T(S))/max(V(R,Y), V(S,Y))

		Cost Estimation of the Join

	*/
	void  Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
	double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);
	bool checkSets(RelationSet set, vector<int> &indices);

	//Given an operand, the function gives the table used in the operand
	void GetRelation(struct Operand *op, string &relation);

private:
	
	//Map between a relation and the set it belongs to
	map<string, RelationSet> m_relationSets;

	//map between a relation and its attributes
	map<string, Relation> m_relations;

	//all the sets in the statistics object
	vector<RelationSet> v_relationSets;

	//Map between the indices and the relations.
	//This is mainly because that attrubutes are like l_orderkey. 
	//This map tells us that l stands for lineitem.
	map<string, string> tables;

	double parseAndList(struct AndList *parseTree);
	void GetRelationAndAttr(string &rel1, string &attr1, struct Operand *op);
	bool isIndependentOr(struct OrList *orList);
	double EstimateCost(struct AndList *parseTree, double numOfTuples);
	//void GetRelation(struct Operand *op, string &relation);

};

#endif