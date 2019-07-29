#include <iostream>
#include <gtest/gtest.h>
#include <map>
#include <vector>
#include <iostream>
#include <assert.h>
#include <string.h>

#include "QueryPlanTree.h"
#include "ParseTree.h"
#include "Statistics.h"
#include "QueryPlan.h"

extern "C"
{
	int yyparse(void);
	struct YY_BUFFER_STATE* yy_scan_string(const char*);

}
extern struct FuncOperator* finalFunction;
extern struct TableList* tables;
extern struct AndList* boolean;
extern struct NameList* groupingAtts;
extern struct NameList* attsToSelect;
extern int distinctAtts;
extern int distinctFunc;

Statistics* s = new Statistics();

int main(int argc, char **argv) {
	::testing::InitGoogleTest(&argc, argv);
	s->Read("Statistics.txt");
	return RUN_ALL_TESTS();
}

//Test case to verify if the sql queries are parsed correctly
TEST(TestCase1, SubTest1) {
	char* cnf = "SELECT c.c_name FROM customer AS c, nation AS n, region AS r WHERE(c.c_nationkey = n.n_nationkey) AND (n.n_regionkey = r.r_regionkey)";
	yy_scan_string(cnf);
	yyparse();
	QueryPlan *plan = new QueryPlan();

	vector<AndList> joinlist;
	vector<AndList> havingList;
	vector<AndList> whereList;

	plan->getJoinsAndWhereClauses(joinlist, havingList, whereList);

	ASSERT_TRUE(joinlist.size() == 2 && whereList.size() == 0);
	delete plan;
}


//Check if the table name is parsed correctly from the attribute
TEST(TestCase2, SubTest2)
{
	QueryPlan *plan = new QueryPlan();
	string table = plan->getTableName("r.r_regionkey");
	ASSERT_TRUE("r" == table);
	delete plan;
}

TEST(TestCase3, SubTest3)
{
	char* cnf = "SELECT c.c_name FROM customer AS c, nation AS n, region AS r WHERE(c.c_nationkey = n.n_nationkey) AND (n.n_regionkey = r.r_regionkey)";
	yy_scan_string(cnf);
	yyparse();
	
	QueryPlan *plan = new QueryPlan();

	vector<AndList> joinlist;
	vector<AndList> havingList;
	vector<AndList> whereList;

	plan->getJoinsAndWhereClauses(joinlist, havingList, whereList);
	ASSERT_TRUE(plan->joinOptimization(joinlist, s).size() == joinlist.size());

	delete plan;
}

TEST(TestCase4, SubTest4)
{
	char* cnf = "SELECT c.c_name FROM customer AS c, nation AS n, region AS r WHERE(c.c_nationkey = n.n_nationkey) AND (n.n_regionkey = r.r_regionkey)";
	yy_scan_string(cnf);
	//yyparse();
	
	QueryPlan *plan = new QueryPlan();
	QueryPlanTree *node = plan->getPlan();

	ASSERT_TRUE(node!=NULL);
	delete plan;
}

/*
//Test case to verify tables parsing
TEST(TestCase2, SubTest2) {
	char* cnf = "SELECT l.l_tax FROM lineitem AS l, orders AS o, part AS p WHERE(l.l_orderkey = o.o_orderkey) AND (p.p_partkey = l.l_partkey)";
	yy_scan_string(cnf);
	yyparse();

	Statistics* s = new Statistics();

	vector<string> relations;
	vector<AndList> joins;
	vector<AndList> selects;
	vector<AndList> joinDepSels;
	string projectStart;

	GetTables(relations);
	ASSERT_TRUE(relations.size() == 3);


	GetJoinsAndSelects(joins, selects, joinDepSels, *s);
	ASSERT_TRUE(joins.size() == 2);
	ASSERT_TRUE(selects.size() == 0);
	ASSERT_TRUE(joinDepSels.size() == 0);


	if (joins.size() > 1)
	{
		joins = optimizeJoinOrder(joins, s);
		ASSERT_TRUE(joins.size() == 2);
	}
}

*/