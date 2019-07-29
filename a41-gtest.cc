#include <iostream>
#include<gtest/gtest.h>
#include <stdlib.h>
#include "Statistics.h"
#include "ParseTree.h"
#include <math.h>
extern "C" struct YY_BUFFER_STATE* yy_scan_string(const char*);
extern "C" int yyparse(void);
extern struct AndList* final;

char* fileName = "Statistics.txt";

int main(int argc, char **argv) {
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}

//Test case to test Estimate
TEST(TestEstimate, SubTest1) {
	bool res = true;
	Statistics s;
	char* relName[] = { "supplier","partsupp" };


	s.AddRel(relName[0], 10000);
	s.AddAtt(relName[0], "s_suppkey", 10000);

	s.AddRel(relName[1], 800000);
	s.AddAtt(relName[1], "ps_suppkey", 10000);

	char* cnf = "(s_suppkey = ps_suppkey)";

	yy_scan_string(cnf);
	yyparse();
	double result = s.Estimate(final, relName, 2);
	if (result != 800000)
		res = false;

	s.Apply(final, relName, 2);
	// test write and read
	s.Write(fileName);
	//reload the statistics object from file
	Statistics s1;
	s1.Read(fileName);
	cnf = "(s_suppkey>1000)";
	yy_scan_string(cnf);
	yyparse();

	double dummy = s1.Estimate(final, relName, 2);
	if (fabs(dummy * 3.0 - result) > 0.1)
	{
		res = false;
	}
	ASSERT_TRUE(res);
}

//Test to test Apply, Estimate, Read, Write
TEST(TestApply, SubTest2) {
	Statistics s;
	bool res = true;
	char* relName[] = { "partsupp", "supplier", "nation" };
	s.AddRel(relName[0], 800000);
	s.AddAtt(relName[0], "ps_suppkey", 10000);

	s.AddRel(relName[1], 10000);
	s.AddAtt(relName[1], "s_suppkey", 10000);
	s.AddAtt(relName[1], "s_nationkey", 25);

	s.AddRel(relName[2], 25);
	s.AddAtt(relName[2], "n_nationkey", 25);
	s.AddAtt(relName[2], "n_name", 25);

	char* cnf = " (s_suppkey = ps_suppkey) ";
	yy_scan_string(cnf);
	yyparse();
	s.Apply(final, relName, 2);

	cnf = " (s_nationkey = n_nationkey)  AND (n_name = 'AMERICA')   ";
	yy_scan_string(cnf);
	yyparse();

	double result = s.Estimate(final, relName, 3);
	if (fabs(result - 32000) > 0.1)
		res = false;
	s.Apply(final, relName, 3);

	s.Write(fileName);
	ASSERT_TRUE(res);

}