#include "Statistics.h"

Statistics::Statistics()
{
	tables["n"] = "nation";
  	tables["r"] = "region";
  	tables["p"] = "part";
  	tables["s"] = "supplier";
  	tables["ps"] = "partsupp";
  	tables["c"] = "customer";
  	tables["o"] = "orders";
  	tables["l"] = "lineitem";
	
}
Statistics::Statistics(Statistics &copyMe)
{
	m_relations = copyMe.m_relations;
	m_relationSets = copyMe.m_relationSets;
  	v_relationSets = copyMe.v_relationSets;
  	tables = copyMe.tables;
}
Statistics::~Statistics()
{
	
}

void Statistics::AddRel(char *relName, int numTuples)
{
	string relation(relName);
	//If the relation already exists in the m_relationSets
	//Set the numTuples to the existing relation
	if(m_relations.find(relation) != m_relations.end())
	{
		m_relations[relation].setNumTuples(numTuples);
	}
	else
	{
		//The relation does not exist, so creating a new set in the statistics object
		//Attributes of the relation and the numOfTuples.
		Relation rel(numTuples);

		//The set to which the relation belongs to
		RelationSet relSet(relation, numTuples);

		//Add the relation set to the vector of sets.
		v_relationSets.push_back(relSet);

		//Map the relation to its set
		m_relationSets[relation] = relSet;

		//Map the relation to its attributes.
		m_relations[relation] = rel;

	}
}

void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{		
	string relation(relName);
	string attr(attName);

	//Only if the relation exists
	if(m_relations.find(relation) != m_relations.end())
	{
		m_relations[relation].addAttr(attName, numDistincts);
	}
}

void Statistics::CopyRel(char *oldName, char *newName)
{
	string oldRelation(oldName);
	string newRelation(newName);

	if(m_relations.find(oldRelation) != m_relations.end())
	{
		//Create a new relation
		Relation newRel;
		newRel.copy(m_relations[oldRelation]);
		m_relations[newRelation] = newRel;

		//Add a new RelationSet to the statistics object
		RelationSet relSet(newRelation, newRel.getNumTuples());
		//relSet.copy(m_relationSets[newRelation].getJoinedRelations());
		m_relationSets[newRelation] = relSet;
		v_relationSets.push_back(relSet);
	}
}

void Statistics::Read(char *fromWhere)
{
	//Input stream reader
	cout << "Started reading stats " << endl;
	ifstream reader(fromWhere);
	if(reader.is_open())
	{
		//String stream to the file
		stringstream strStream;
		string line;
		string relation;

		//First read the number of relations
		getline(reader, line);
		strStream.str(line);

		int numRelation = 0;

		if(!(strStream >> numRelation))
		{
			numRelation = 0;
		}
		strStream.clear(); strStream.str("");

		//read the numRelationSets relation sets from the file
		for(int i=0; i < numRelation; i++)
		{
			cout << "In for " << endl;
			//Read the relation name
			Relation rel;
			getline(reader, line);
			if(line.compare("") == 0)
			{
				//This should be the relation name
				getline(reader, line);
			}
			relation = line;
			//Now read the attributes of the relation
			rel.readFromFile(reader);

			m_relations[relation] = rel;
		}

		//Done reading the relations from the file, now read the relation sets
		getline(reader, line);
		while(line.compare("###") != 0)
		{
			cout << "." << endl;
			getline(reader, line);
		}

		getline(reader, line);
		strStream.str(line);

		int numSets = 0;
		if(!(strStream >> numSets))
		{
			numSets = 0;
		}


		//GEt the next new line
		getline(reader, line);

		//Now read the relation sets.
		for(int i=0; i < numSets; i++)
		{
			RelationSet relSet;
			vector<string> setRelations;

			relSet.readFromFile(reader);
			//Push the relation set to the vector
			v_relationSets.push_back(relSet);

			//GEt the vector of relations
			relSet.getRelations(setRelations);

			vector<string>::iterator iter;
			for(iter = setRelations.begin(); iter != setRelations.end(); iter++)
			{
				m_relationSets[*iter] = relSet;
			}

			//Add the set to the Vector of sets
			
		}
	}

	reader.close();
}
void Statistics::Write(char *fromWhere)
{
	ofstream writer(fromWhere);

	//Write only if there are relations in the sets
	if(writer.good() && v_relationSets.size() > 0)
	{
		//Iterator to the relations;
		map<string,Relation>::iterator relationIterator;

		//Write the number of relations in the object
		writer << m_relations.size() << endl << endl;

		//Iterator over the relations and write each relation to the output stream
		for(relationIterator = m_relations.begin(); relationIterator != m_relations.end(); relationIterator++)
		{
			if(writer.good())
			{
				//relation name
				writer << relationIterator->first << endl;// << relationIterator->second << endl;
				//Attributes and tuples of the relation
				relationIterator->second.writeToFile(writer);
			}
		}

		//Done writing the relations to the file. Adding the delimiter
		writer << "###" << endl;

		//Now write the number of sets to the file
		writer << v_relationSets.size() << endl << endl;

		//For each relation set in the vector
		for(vector<RelationSet>::iterator it = v_relationSets.begin(); it != v_relationSets.end(); it++)
		{
			//*it is the current relation set. write that to the file
			it->writeToFile(writer);
		}

		//Done writing the sets
	}
	writer.close();
}

void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
	//Create a new Relationset for this join
	RelationSet set;

	//Estimated cost for the join
	double estimate = 0.0;

	//Indices of the sets we are merging in this join
	vector<int> indices;

	//New set of the relations after the join
	vector<RelationSet> copy;

	//Creating the new relation set from the given relation names
	for(int i = 0; i < numToJoin; i++)
	{
		set.insertToSet(relNames[i]);
	}

	//Parse the AndList to get the estimated number of the tuples after the join
	double estimatedJoin = parseAndList(parseTree);
	//If the given set can be found in any of the sets in the object,
	//calculate an estimate of the join
	if(checkSets(set, indices))
	{
		estimate = EstimateCost(parseTree, estimatedJoin);
	}

	int index = 0;
	int oldSize = v_relationSets.size();
	int newSize = oldSize - (int) indices.size() + 1;

	//If the input relations span multiple sets
	if(indices.size() > 1)
	{
		//Copy the sets not spanned by the join
		for(int i=0; i < oldSize; i++)
		{
			if(i == indices[index])
			{
				index++;
			}
			else
			{
				copy.push_back(v_relationSets[i]);
			}
		}

		//Make the global vector as the current vector
		v_relationSets.clear();
		for(int i=0; i<newSize -1; i++)
		{
			v_relationSets.push_back(copy[i]);
		}

		//The number of tuples is now changed
		set.updateTuples(estimate);
		v_relationSets.push_back(set);

		copy.clear();
		//Get the relations in the current join set
		vector<string> relations;
		set.getRelations(relations);

		//For each relation in the current join set, update the set 
		//to which it belongs to 
		for(int i=0; i<set.size(); i++)
		{
			m_relationSets[relations[i]] = set;
		}

	}


}

double Statistics::parseAndList(struct AndList *parseTree)
{
	double result = 0.0;
	if(parseTree)
	{
		struct AndList *currAnd = parseTree;
		struct OrList *currOr;
		struct ComparisonOp *oper;

		bool foundJoin = false;

		//Relations and their corresponding attributes
		string rel1, rel2, attr1, attr2;

		//Relation of the two attributes in the join
		Relation r1, r2;
		//Loop until we find the join or end of the And list
		while(currAnd && !foundJoin)
		{
			currOr = currAnd->left;

			while(currOr && !foundJoin)
			{
				oper = currOr->left;

				//If the current operation is not null
				if(oper)
				{
					//Check if this operation is a join
					if(oper->code == EQUALS && oper->left->code == oper->right->code)
					{
						foundJoin = true;
						//Get the relation and the attributes of the join
						GetRelationAndAttr(rel1, attr1, oper->left);
						GetRelationAndAttr(rel2, attr2, oper->right);

						r1 = m_relations[rel1];
						r2 = m_relations[rel2];

						//Get the number of the rows in the correponding set
						result = m_relationSets[rel1].getNumOfTuples() * m_relationSets[rel2].getNumOfTuples();
 						if(r1.getNumTuples() > r2.getNumTuples())
						{
							result /= (double) r1.getAttr(attr1);
						}
						else
						{
							result /= (double) r2.getAttr(attr2);
						}

					}
				}
				currOr = currOr->rightOr;
			} //end while(currOr)
			currAnd = currAnd -> rightAnd;
		} //end while(currAnd)
	}

	return result;
}

void Statistics::GetRelationAndAttr(string &relation, string &attr, struct Operand *op)
{
	//attribute looks like l_orderkey or l.l_orderkey
	string value(op->value);
	string rel;
	stringstream s;

	int i = 0;

	//Loop till we get the '.' and the remaining to the right is the attribute
	while(value[i] != '_')
	{
		if(value[i] == '.')
		{
			relation = s.str();
			break;
		}

		s << value[i++];
	}
	//If we broke out of the loop at '.'
	if(value[i] == '.')
	{
		attr = value.substr(i+1);
	}
	else
	{
		attr = value;
		rel = s.str();
		relation = tables[rel];
	}

}

bool Statistics::checkSets(RelationSet set, vector<int> &indices)
{
	int index = 0;
	int numRelations = 0;
	for(; index < v_relationSets.size(); index++)
	{
		int intersectedSets = v_relationSets[index].intersect(set);
		//join is not feasible. The set should either span the subsets or not present at all
		if(intersectedSets == -1)
		{
			indices.clear();
			numRelations = 0;
			break;
		}
		//If the set spans a subset in the vector of sets
		else if(intersectedSets > 0)
		{
			numRelations += intersectedSets;
			indices.push_back(index);

			if(numRelations == set.size())
				break;
		}
	}	
	return (numRelations > 0);
}

bool Statistics::isIndependentOr(struct OrList *orList)
{
	struct OrList *currOr = orList;
	string lName;
	vector<string> vec;
	while(currOr)
	{
		if(vec.size() == 0)
		{
			vec.push_back(currOr->left->left->value);
		}
		else
		{
			if(vec[0].compare(currOr->left->left->value) != 0)
				vec.push_back(currOr->left->left->value);
		}

		currOr = currOr->rightOr;
	}
	//If there are no dependent ORs, then the vec size shoule be > 1
	return (vec.size() > 1);

}


double Statistics::EstimateCost(struct AndList *parseTree, double joinEstimate)
{
	double estimate = 1.0;
	if(joinEstimate > 0)
	{
		estimate = joinEstimate;
	}

	struct AndList *currAnd = parseTree;
	struct OrList *currOr;
	struct ComparisonOp *currOp;

	Relation relation;
	string rel1;
	string attr1;
	bool hasJoin = false;

	long numOfTuples = 0.0l;
	while(currAnd)
	{
		currOr = currAnd->left;
		bool independentOr = isIndependentOr(currOr);
		bool singleOr = (currOr->rightOr == NULL);
		double tempEstimate = 0.0;
		if(independentOr)
			tempEstimate = 1.0;

		while(currOr)
		{
			currOp =  currOr->left;
			Operand *op = currOp->left;
			//If left is not a name, then right must be a name;
			if(op->code != NAME)
				op = currOp->right;

			GetRelationAndAttr(rel1, attr1, op);
			relation = m_relations[rel1];
			//If the operand is EQUALS
			if(currOp->code == EQUALS)
			{
				if(singleOr)
				{
					if(currOp->right->code == NAME && currOp->left->code == NAME)
					{
						hasJoin = true;
						tempEstimate = 1.0;
					}
					else
					{
						double const calc = ( 1.0l / relation.getAttr(attr1));
						tempEstimate += calc;
					}
				}
				else
				{
					if(independentOr)
					{
						double const calc = 1.0l - (1.0l /relation.getAttr(attr1));
						tempEstimate *= calc;
					}
					else
					{
						double const calc = (1.0l / relation.getAttr(attr1));
						tempEstimate += calc;
					}
				}
			}
			else
			{
				
				if(singleOr){
					double const calc = 1.0l / 3.0l;
					tempEstimate += calc;	
				}

				else{
					if(independentOr){
						double const calc = 1.0l - (1.0l / 3.0l);
						tempEstimate *= calc;	
					}
					else{
						double const calc = 1.0l / 3.0l;
						tempEstimate += calc;	
					}
				}
				
			}
			if(!hasJoin)
			{
				/*if(m_relationSets[rel1].getNumOfTuples() == -1)
				{
					numOfTuples = relation.getNumTuples();
				}
				else
				{
					numOfTuples = m_relationSets[rel1].getNumOfTuples();
				} 
				*/
				numOfTuples = m_relationSets[rel1].getNumOfTuples();
				
			}
			currOr = currOr->rightOr;
		}//End of while currOr
		if(singleOr){
			estimate *= tempEstimate;
		}
		else{
			if(independentOr){
				estimate *= (1 - tempEstimate);
			}
			else{
				estimate *= tempEstimate;
			}
		}
		currAnd = currAnd->rightAnd;
	}//End of while currAnd
	if(!hasJoin)
	{
		estimate = numOfTuples * estimate;
	}

	return estimate;
}


double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{

	double estimate = 0.0;

	RelationSet relSet;
	vector<int> indices;

	for(int i=0; i < numToJoin; i++)
	{
		relSet.insertToSet(relNames[i]);
	}

	double joinEstimate = 0.0;

	joinEstimate = parseAndList(parseTree);
	if(checkSets(relSet, indices))
	{
		estimate = EstimateCost(parseTree, joinEstimate);
	}

	return estimate;
}

/* Not needed here
void Statistics::GetRelation(struct Operand *op, string &relation)
{
	string value(op->value);
	string rel;
	stringstream s;

	int i=0;

	while(value[i] != '_')
	{
		if(value[i] = '.')
		{
			relation = s.str();
			return;
		}

		s << value[i];
		i++;
	}
}*/