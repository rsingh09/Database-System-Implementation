CC = g++ -O2 -Wno-deprecated 

tag = -i

ifdef linux
tag = -n
endif


a1.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o HeapFile.o RawDBFile.o DBFile.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o Pipe.o BigQ.o SortedFile.o RelOp.o SelectFile.o Join.o Sum.o WriteOut.o SelectPipe.o Project.o DuplicateRemoval.o GroupBy.o Function.o a1.o
	$(CC) -o a1.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o HeapFile.o RawDBFile.o DBFile.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o Pipe.o BigQ.o SortedFile.o RelOp.o SelectFile.o Join.o Sum.o WriteOut.o SelectPipe.o Project.o DuplicateRemoval.o GroupBy.o Function.o a1.o -lfl -lpthread -DDEBUG

a3.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o HeapFile.o RawDBFile.o DBFile.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o Pipe.o BigQ.o SortedFile.o RelOp.o SelectFile.o Join.o Sum.o WriteOut.o SelectPipe.o Project.o DuplicateRemoval.o GroupBy.o Function.o test.o
	$(CC) -o a3.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o HeapFile.o RawDBFile.o DBFile.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o Pipe.o BigQ.o SortedFile.o RelOp.o SelectFile.o Join.o Sum.o WriteOut.o SelectPipe.o Project.o DuplicateRemoval.o GroupBy.o Function.o test.o -lfl -lpthread -DDEBUG

a3-gtest.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o HeapFile.o RawDBFile.o DBFile.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o Pipe.o BigQ.o SortedFile.o RelOp.o SelectFile.o Join.o Sum.o WriteOut.o SelectPipe.o Project.o DuplicateRemoval.o GroupBy.o Function.o a3-gtest.o
	$(CC) -o a3-gtest.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o HeapFile.o RawDBFile.o DBFile.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o Pipe.o BigQ.o SortedFile.o RelOp.o SelectFile.o Join.o Sum.o WriteOut.o SelectPipe.o Project.o DuplicateRemoval.o GroupBy.o Function.o a3-gtest.o -lfl -lgtest -lpthread -DDEBUG

a41-gtest.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o HeapFile.o RawDBFile.o DBFile.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o Pipe.o BigQ.o SortedFile.o RelOp.o SelectFile.o Join.o Sum.o WriteOut.o SelectPipe.o Project.o DuplicateRemoval.o GroupBy.o Function.o a41-gtest.o Statistics.o
	$(CC) -o a41-gtest.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o HeapFile.o RawDBFile.o DBFile.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o Pipe.o BigQ.o SortedFile.o RelOp.o SelectFile.o Join.o Sum.o WriteOut.o SelectPipe.o Project.o DuplicateRemoval.o GroupBy.o Function.o a41-gtest.o Statistics.o -lfl -lgtest -lpthread -DDEBUG

a42-gtest.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o HeapFile.o RawDBFile.o DBFile.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o Pipe.o BigQ.o SortedFile.o RelOp.o SelectFile.o Join.o Sum.o WriteOut.o SelectPipe.o Project.o DuplicateRemoval.o GroupBy.o Function.o a42-gtest.o Statistics.o QueryPlanTree.o QueryPlan.o
	$(CC) -o a42-gtest.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o HeapFile.o RawDBFile.o DBFile.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o Pipe.o BigQ.o SortedFile.o RelOp.o SelectFile.o Join.o Sum.o WriteOut.o SelectPipe.o Project.o DuplicateRemoval.o GroupBy.o Function.o a42-gtest.o Statistics.o QueryPlanTree.o QueryPlan.o -lfl -lgtest -lpthread -DDEBUG

a4.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o HeapFile.o RawDBFile.o DBFile.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o Pipe.o BigQ.o SortedFile.o RelOp.o SelectFile.o Join.o Sum.o WriteOut.o SelectPipe.o Project.o DuplicateRemoval.o GroupBy.o Function.o test.o Statistics.o
	$(CC) -o a4.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o HeapFile.o RawDBFile.o DBFile.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o Pipe.o BigQ.o SortedFile.o RelOp.o SelectFile.o Join.o Sum.o WriteOut.o SelectPipe.o Project.o DuplicateRemoval.o GroupBy.o Function.o test.o Statistics.o -lfl -lpthread -DDEBUG

main_4-2.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o HeapFile.o RawDBFile.o DBFile.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o Pipe.o BigQ.o SortedFile.o RelOp.o SelectFile.o Join.o Sum.o WriteOut.o SelectPipe.o Project.o DuplicateRemoval.o GroupBy.o Function.o  Statistics.o QueryPlanTree.o QueryPlan.o main.o
	$(CC) -o main.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o HeapFile.o RawDBFile.o DBFile.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o Pipe.o BigQ.o SortedFile.o RelOp.o SelectFile.o Join.o Sum.o WriteOut.o SelectPipe.o Project.o DuplicateRemoval.o GroupBy.o Function.o Statistics.o QueryPlanTree.o QueryPlan.o main.o -lfl -lpthread -DDEBUG

main.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o HeapFile.o RawDBFile.o DBFile.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o Pipe.o BigQ.o SortedFile.o RelOp.o SelectFile.o Join.o Sum.o WriteOut.o SelectPipe.o Project.o DuplicateRemoval.o GroupBy.o Function.o  Statistics.o QueryPlanTree.o QueryPlan.o main.o
	$(CC) -o main.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o HeapFile.o RawDBFile.o DBFile.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o Pipe.o BigQ.o SortedFile.o RelOp.o SelectFile.o Join.o Sum.o WriteOut.o SelectPipe.o Project.o DuplicateRemoval.o GroupBy.o Function.o Statistics.o QueryPlanTree.o QueryPlan.o main.o -lfl -lpthread -DDEBUG

a3-gtest.o: a3-gtest.cc
	$(CC) -g -c a3-gtest.cc

a41-gtest.o: a41-gtest.cc
	$(CC) -g -c a41-gtest.cc

QueryPlan.o: QueryPlan.cc
	$(CC) -g -c QueryPlan.cc

SelectPipe.o: SelectPipe.cc
	$(CC) -g -c SelectPipe.cc
	
Project.o: Project.cc
	$(CC) -g -c Project.cc

QueryPlanTree.o: QueryPlanTree.cc
	$(CC) -g -c QueryPlanTree.cc
	
DuplicateRemoval.o: DuplicateRemoval.cc
	$(CC) -g -c DuplicateRemoval.cc
	
GroupBy.o: GroupBy.cc
	$(CC) -g -c GroupBy.cc
	
RelOp.o: RelOp.cc
	$(CC) -g -c RelOp.cc

SelectFile.o: SelectFile.cc
	$(CC) -g -c SelectFile.cc

WriteOut.o: WriteOut.cc
	$(CC) -g -c WriteOut.cc

Function.o: Function.cc
	$(CC) -g -c Function.cc

RawDBFile.o: RawDBFile.cc
	$(CC) -g -c RawDBFile.cc

Join.o: Join.cc
	$(CC) -g -c Join.cc

Statistics.o: Statistics.cc
	$(CC) -g -c Statistics.cc

BigQ.o: BigQ.cc
	$(CC) -g -c BigQ.cc

SortedFile.o: SortedFile.cc
	$(CC) -g -c SortedFile.cc

HeapFile.o: HeapFile.cc
	$(CC) -g -c HeapFile.cc

test.o: test.cc
	$(CC) -g -c test.cc

a1.o: a1.cc
	$(CC) -g -c a1.cc

main.o: main.cc
	$(CC) -g -c main.cc
	
Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc
	
ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc
	
DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc

File.o: File.cc
	$(CC) -g -c File.cc

Record.o: Record.cc
	$(CC) -g -c Record.cc

Schema.o: Schema.cc
	$(CC) -g -c Schema.cc

Pipe.o: Pipe.cc
	$(CC) -g -c Pipe.cc
	

y.tab.o: Parser.y
	yacc -d Parser.y
	sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -g -c y.tab.c

yyfunc.tab.o: ParserFunc.y
	yacc -p "yyfunc" -b "yyfunc" -d ParserFunc.y
	sed $(tag) yyfunc.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c yyfunc.tab.c

lex.yy.o: Lexer.l
	lex  Lexer.l
	gcc  -c lex.yy.c

lex.yyfunc.o: LexerFunc.l
	lex -Pyyfunc LexerFunc.l
	gcc  -c lex.yyfunc.c

clean: 
	rm -f *.o
	rm -f *.out
	rm -f y.tab.*
	rm -f yyfunc.tab.*
	rm -f lex.yy.*
	rm -f lex.yyfunc*
