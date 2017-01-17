/* 

   @Author: RUAN0007
   @Date:   2017-01-11 09:22:13
   @Last_Modified_At:   2017-01-17 15:06:37
   @Last_Modified_By:   RUAN0007

*/


#include <gtest/gtest.h>

#include <algorithm>    // std::find_if

#include "relation.h"

#include "client.h"
#include "type.h"
#include "field.h"
#include "tuple.h"
#include "page.h"
#include "predicate.h"

#include "debug.h"

using namespace std;
using namespace ustore::relation;
using namespace ustore;

string random_string( size_t length )
{
    auto randchar = []() -> char
    {
        const char charset[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";
        const size_t max_index = (sizeof(charset) - 1);
        return charset[ rand() % max_index ];
    };
    string str(length,0);
    generate_n( str.begin(), length, randchar );
    return str;
}

class RelationTest : public ::testing::Test {
 protected:
  virtual void SetUp() {

  	vector<const Type*> types;
  	types.push_back(IntType::GetInstance());
  	types.push_back(StrType::GetInstance());

  	vector<string> names;
  	names.push_back("IntF");
  	names.push_back("StrF");

  	relation_name = random_string(5);
  	schema = new TupleDscp(relation_name, types, names);


	read_page = new Page(relation_name, schema, 4096);

	write_page = new Page(relation_name, schema, 4096);
	storage = new UstoreHeapStorage(relation_name, *schema, &client, write_page); 

	tuple_store = new unsigned char[4000];

	// storage->SetCommitBuffer(write_page);
  }

  virtual void TearDown() {
  	delete schema;
  	delete storage;
  	delete read_page;
  	delete write_page;
  	delete tuple_store;
  }

  string relation_name;
  ClientService client;
  UstoreHeapStorage* storage;
  Page *read_page;
  Page *write_page;
  TupleDscp *schema;
  unsigned char* tuple_store;
};


TEST_F(RelationTest, Basic) {
	Tuple t1(tuple_store, 0, schema);	
	Field* int_f1 = new IntField(1);
	Field* str_f1 = new StrField("1");
	t1.SetFieldByIndex(0, int_f1, 0);
	t1.SetFieldByIndex(1, str_f1, 0);

	Tuple t2(tuple_store, 500, schema);	
	Field* int_f2 = new IntField(2);
	Field* str_f2 = new StrField("2");
	t2.SetFieldByIndex(0, int_f2, 0);
	t2.SetFieldByIndex(1, str_f2, 0);

	Field* int_f3 = new IntField(3);
	Field* str_f3 = new StrField("3");

	Tuple t32(tuple_store, 1000, schema);	
	t32.SetFieldByIndex(0, int_f3, 0);
	t32.SetFieldByIndex(1, str_f2, 0);

	Tuple t23(tuple_store, 1500, schema);	
	t23.SetFieldByIndex(0, int_f2, 0);
	t23.SetFieldByIndex(1, str_f3, 0);

	CommitID commit_id;
	string msg;

	EXPECT_TRUE(storage->InsertTuple(&t1, &msg)) << msg;
	EXPECT_TRUE(storage->Commit(&commit_id, &msg)) << msg;

	EXPECT_TRUE(storage->InsertTuple(&t2, &msg)) << msg;
	EXPECT_TRUE(storage->Commit(&commit_id, &msg)) << msg;

	Tuple *pt1 = storage->GetTuple("master", int_f1, read_page, &msg);
	EXPECT_NE(pt1, reinterpret_cast<Tuple*>(0)) << msg;
	EXPECT_EQ(t1, *pt1);
	delete pt1;

	Tuple *pt2 = storage->GetTuple("master", int_f2, read_page, &msg);
	EXPECT_NE(pt2, reinterpret_cast<Tuple*>(0)) << msg;
	EXPECT_EQ(t2, *pt2);
	delete pt2;

//Retrieve a non-existent tuple
	Tuple *pt3 = storage->GetTuple("master", int_f3, read_page, &msg);
	EXPECT_EQ(pt3, reinterpret_cast<Tuple*>(0)) << msg;

//Remove a non-existent tuple
	EXPECT_FALSE(storage->RemoveTuple(int_f3, &msg)) << msg;

//Update a non-existent tuple
	EXPECT_FALSE(storage->UpdateTuple(&t32, &msg)) << msg;

//Reinsert a tuple with same pk
	EXPECT_FALSE(storage->InsertTuple(&t23, &msg)) << msg;

//Update a existent tuple and check for existence
	EXPECT_TRUE(storage->UpdateTuple(&t23, &msg)) << msg;

	//Before Commit
	Tuple *pt23 = storage->GetTuple("master", int_f2, read_page, &msg);
	EXPECT_NE(pt23, reinterpret_cast<Tuple*>(0)) << msg;
	EXPECT_EQ(t2, *pt23);
	delete pt23;

	EXPECT_TRUE(storage->Commit(&commit_id, &msg)) << msg;

	//After Commit
	pt23 = storage->GetTuple("master", int_f2, read_page, &msg);
	EXPECT_NE(pt23, reinterpret_cast<Tuple*>(0)) << msg;
	EXPECT_EQ(t23, *pt23);
	delete pt23;

//An empty commit
	EXPECT_FALSE(storage->Commit(&commit_id, &msg)) << msg;

//Remove an existent tuple
	EXPECT_TRUE(storage->RemoveTuple(int_f1, &msg)) << msg;

	//Before commit
	pt1 = storage->GetTuple("master", int_f1, read_page, &msg);
	EXPECT_NE(pt1, reinterpret_cast<Tuple*>(0)) << msg;
	EXPECT_EQ(t1, *pt1);
	delete pt1;

	//After Commit
	EXPECT_TRUE(storage->Commit(&commit_id, &msg)) << msg;

	pt1 = storage->GetTuple("master", int_f1, read_page, &msg);
	EXPECT_EQ(pt1, reinterpret_cast<Tuple*>(0)) << msg;

	delete int_f1; 
	delete int_f2;
	delete int_f3;
	delete str_f1;
	delete str_f2;
	delete str_f3;
}

TEST_F(RelationTest, OperationAfterInsert) {

	Tuple t1(tuple_store, 0, schema);	
	Field* int_f1 = new IntField(1);
	Field* str_f1 = new StrField("1");
	t1.SetFieldByIndex(0, int_f1, 0);
	t1.SetFieldByIndex(1, str_f1, 0);

	Tuple t2(tuple_store, 500, schema);	
	Field* int_f2 = new IntField(2);
	Field* str_f2 = new StrField("2");
	t2.SetFieldByIndex(0, int_f2, 0);
	t2.SetFieldByIndex(1, str_f2, 0);

	Field* int_f3 = new IntField(3);
	Field* str_f3 = new StrField("3");

	Tuple t12(tuple_store, 1000, schema);	
	t12.SetFieldByIndex(0, int_f1, 0);
	t12.SetFieldByIndex(1, str_f2, 0);

	Tuple t23(tuple_store, 1500, schema);	
	t23.SetFieldByIndex(0, int_f2, 0);
	t23.SetFieldByIndex(1, str_f3, 0);

	CommitID commit_id;
	string msg;

	EXPECT_TRUE(storage->InsertTuple(&t2, &msg)) << msg;	

//Reinsertion in the same commit
	EXPECT_FALSE(storage->InsertTuple(&t23, &msg)) << msg;	

//Update after the commit in the same commit
	EXPECT_TRUE(storage->UpdateTuple(&t23, &msg)) << msg;	

	//Before Commit
	Tuple *pt23 = storage->GetTuple("master", int_f2, read_page, &msg);
	EXPECT_EQ(pt23, reinterpret_cast<Tuple*>(0)) << msg;

	EXPECT_TRUE(storage->Commit(&commit_id, &msg)) << msg;

	//After Commit
	pt23 = storage->GetTuple("master", int_f2, read_page, &msg);
	EXPECT_NE(pt23, reinterpret_cast<Tuple*>(0)) << msg;
	EXPECT_EQ(t23, *pt23);
	delete pt23;


//Delete After the insert in the same commit
	EXPECT_TRUE(storage->InsertTuple(&t1, &msg)) << msg;	
	EXPECT_TRUE(storage->RemoveTuple(int_f1, &msg)) << msg;

//Fail to commit since no operaitons
	EXPECT_FALSE(storage->Commit(&commit_id, &msg)) << msg;

	delete int_f1; 
	delete int_f2;
	delete int_f3;
	delete str_f1;
	delete str_f2;
	delete str_f3;
}

TEST_F(RelationTest, OperationAfterUpdate) {

	Tuple t1(tuple_store, 0, schema);	
	Field* int_f1 = new IntField(1);
	Field* str_f1 = new StrField("1");
	t1.SetFieldByIndex(0, int_f1, 0);
	t1.SetFieldByIndex(1, str_f1, 0);

	Tuple t2(tuple_store, 500, schema);	
	Field* int_f2 = new IntField(2);
	Field* str_f2 = new StrField("2");
	t2.SetFieldByIndex(0, int_f2, 0);
	t2.SetFieldByIndex(1, str_f2, 0);

	Field* int_f3 = new IntField(3);
	Field* str_f3 = new StrField("3");
	Field* str_f4 = new StrField("4");

	Tuple t12(tuple_store, 1000, schema);	
	t12.SetFieldByIndex(0, int_f1, 0);
	t12.SetFieldByIndex(1, str_f2, 0);

	Tuple t13(tuple_store, 1500, schema);	
	t13.SetFieldByIndex(0, int_f1, 0);
	t13.SetFieldByIndex(1, str_f3, 0);

	Tuple t14(tuple_store, 1500, schema);	
	t13.SetFieldByIndex(0, int_f1, 0);
	t13.SetFieldByIndex(1, str_f4, 0);

	CommitID commit_id;
	string msg;

	EXPECT_TRUE(storage->InsertTuple(&t1, &msg)) << msg;	

	EXPECT_TRUE(storage->UpdateTuple(&t12, &msg)) << msg;	

//Insert After Update in the same commit
	EXPECT_FALSE(storage->InsertTuple(&t12, &msg)) << msg;	

//Updata after the update in the same commit
	EXPECT_TRUE(storage->UpdateTuple(&t13, &msg)) << msg;	

	EXPECT_TRUE(storage->Commit(&commit_id, &msg)) << msg;

	Tuple* pt13 = storage->GetTuple("master", int_f1, read_page, &msg);
	EXPECT_NE(pt13, reinterpret_cast<Tuple*>(0)) << msg;
	EXPECT_EQ(t13, *pt13);
	delete pt13;


//Delete After the update in the previous commit
	EXPECT_TRUE(storage->RemoveTuple(int_f1, &msg)) << msg;
	EXPECT_TRUE(storage->Commit(&commit_id, &msg)) << msg;

	pt13 = storage->GetTuple("master", int_f1, read_page, &msg);
	EXPECT_EQ(pt13, reinterpret_cast<Tuple*>(0)) << msg;

//Remove after the update int same commit
	EXPECT_TRUE(storage->InsertTuple(&t1, &msg)) << msg;
	EXPECT_TRUE(storage->UpdateTuple(&t14, &msg)) << msg;	
	EXPECT_TRUE(storage->RemoveTuple(int_f1, &msg)) << msg;

//Fail to commit as no operation
	EXPECT_FALSE(storage->Commit(&commit_id, &msg)) << msg;

	delete int_f1; 
	delete int_f2;
	delete int_f3;
	delete str_f1;
	delete str_f2;
	delete str_f3;
	delete str_f4;
}


TEST_F(RelationTest, OperationAfterRemove) {

	Tuple t1(tuple_store, 0, schema);	
	Field* int_f1 = new IntField(1);
	Field* str_f1 = new StrField("1");
	t1.SetFieldByIndex(0, int_f1, 0);
	t1.SetFieldByIndex(1, str_f1, 0);

	Tuple t2(tuple_store, 500, schema);	
	Field* int_f2 = new IntField(2);
	Field* str_f2 = new StrField("2");
	t2.SetFieldByIndex(0, int_f2, 0);
	t2.SetFieldByIndex(1, str_f2, 0);

	Tuple t12(tuple_store, 1000, schema);	
	t12.SetFieldByIndex(0, int_f1, 0);
	t12.SetFieldByIndex(1, str_f2, 0);

	CommitID commit_id;
	string msg;

	EXPECT_TRUE(storage->InsertTuple(&t1, &msg)) << msg;	

	EXPECT_TRUE(storage->Commit(&commit_id, &msg)) << msg;

	EXPECT_TRUE(storage->RemoveTuple(int_f1, &msg)) << msg;
	
	//Can not remove again
	EXPECT_FALSE(storage->RemoveTuple(int_f1, &msg)) << msg;

	// cout << "Msg: " << msg << endl;

	//Can not update after: delete
	EXPECT_FALSE(storage->UpdateTuple(&t12, &msg)) << msg;

	//Reinsert in this commit
	EXPECT_TRUE(storage->InsertTuple(&t1, &msg)) << msg;	

	EXPECT_TRUE(storage->RemoveTuple(int_f1, &msg)) << msg;

	//Can not remove again
	EXPECT_FALSE(storage->RemoveTuple(int_f1, &msg)) << msg;
	//Can not update after delete
	EXPECT_FALSE(storage->UpdateTuple(&t12, &msg)) << msg;

//Fail to commit as no operation
	EXPECT_TRUE(storage->Commit(&commit_id, &msg)) << msg;

	delete int_f1; 
	delete int_f2;
	delete str_f1;
	delete str_f2;
}

TEST_F(RelationTest, Branch) {

	cout << "Relation Name: " << relation_name << endl;
	string msg;
	CommitID commit_id;

	Field* int_f1 = new IntField(1);
	Field* int_f2 = new IntField(2);
	Field* int_f3 = new IntField(3);

	Field* str_f1 = new StrField("1");
	Field* str_f2 = new StrField("2");
	Field* str_f3 = new StrField("3");

	Tuple t1(tuple_store, 0, schema);	
	t1.SetFieldByIndex(0, int_f1, &msg);
	t1.SetFieldByIndex(1, str_f1, &msg);

	Tuple t21(tuple_store, 500, schema);	
	t21.SetFieldByIndex(0, int_f2, &msg);
	t21.SetFieldByIndex(1, str_f1, &msg);

	Tuple t22(tuple_store, 1000, schema);	
	t22.SetFieldByIndex(0, int_f2, &msg);
	t22.SetFieldByIndex(1, str_f2, &msg);

	Tuple t3(tuple_store, 1500, schema);	
	t3.SetFieldByIndex(0, int_f3, &msg);
	t3.SetFieldByIndex(1, str_f3, &msg);

	ASSERT_TRUE(storage->InsertTuple(&t1, &msg)) << msg;
	ASSERT_TRUE(storage->Commit(&commit_id, &msg)) << msg;

	string master_branch = "master";
	string branch1 = "branch1";

	EXPECT_TRUE(storage->Branch(master_branch, branch1, &msg));
	EXPECT_STREQ(storage->GetCurrentBranchInfo().branch_name.c_str(), branch1.c_str());
	EXPECT_STREQ(storage->GetCurrentBranchInfo().base_branch.c_str(), master_branch.c_str());

	ASSERT_TRUE(storage->InsertTuple(&t21, &msg)) << msg;
	ASSERT_TRUE(storage->Commit(&commit_id, &msg)) << msg;

	ASSERT_TRUE(storage->InsertTuple(&t3, &msg)) << msg;
	ASSERT_TRUE(storage->Commit(&commit_id, &msg)) << msg;

	EXPECT_FALSE(storage->Switch("Non Existent Branch", &msg)) << msg;

	ASSERT_TRUE(storage->Switch(master_branch, &msg));
	EXPECT_STREQ(storage->GetCurrentBranchInfo().branch_name.c_str(), master_branch.c_str());
	EXPECT_STREQ(storage->GetCurrentBranchInfo().base_branch.c_str(), master_branch.c_str());

	ASSERT_TRUE(storage->InsertTuple(&t22, &msg)) << msg;
	ASSERT_TRUE(storage->Commit(&commit_id, &msg)) << msg;

/*

t1 -> t21 -> t3 (branch1)
 | -> t22 (master)

*/
	Tuple *pt1,*pt2,*pt3;

//Common tuple pt1 from both branches
	pt1 = storage->GetTuple(master_branch, int_f1, read_page, &msg);
	EXPECT_NE(pt1, reinterpret_cast<Tuple*>(0)) << msg;
	EXPECT_EQ(t1, *pt1);
	delete pt1;

	pt1 = storage->GetTuple(branch1, int_f1, read_page, &msg);
	EXPECT_NE(pt1, reinterpret_cast<Tuple*>(0)) << msg;
	EXPECT_EQ(t1, *pt1);
	delete pt1;

//diffent tuple pt2 with same pk from both branches
	pt2 = storage->GetTuple(master_branch, int_f2, read_page, &msg);
	EXPECT_NE(pt1, reinterpret_cast<Tuple*>(0)) << msg;
	EXPECT_EQ(t22, *pt2);
	delete pt2;

	pt2 = storage->GetTuple(branch1, int_f2, read_page, &msg);
	EXPECT_NE(pt1, reinterpret_cast<Tuple*>(0)) << msg;
	EXPECT_EQ(t21, *pt2);
	delete pt2;

	pt3 = storage->GetTuple(master_branch, int_f3, read_page, &msg);
	EXPECT_EQ(pt3, reinterpret_cast<Tuple*>(0)) << msg;

	pt3 = storage->GetTuple(branch1, int_f3, read_page, &msg);
	EXPECT_NE(pt3, reinterpret_cast<Tuple*>(0)) << msg;
	EXPECT_EQ(t3, *pt3);
	delete pt3;


/*
Merge: 
t1 -> t21 -> t3 (branch1)
 | -> t22 --- | -> master

*/
	ASSERT_TRUE(storage->Merge(&commit_id, master_branch, branch1, &msg)) << msg;

	pt1 = storage->GetTuple(master_branch, int_f1, read_page, &msg);
	EXPECT_NE(pt1, reinterpret_cast<Tuple*>(0)) << msg;
	EXPECT_EQ(t1, *pt1);
	delete pt1;

	pt2 = storage->GetTuple(master_branch, int_f2, read_page, &msg);
	EXPECT_NE(pt1, reinterpret_cast<Tuple*>(0)) << msg;
	EXPECT_EQ(t22, *pt2);
	delete pt2;

	pt3 = storage->GetTuple(master_branch, int_f3, read_page, &msg);
	EXPECT_NE(pt3, reinterpret_cast<Tuple*>(0)) << msg;
	EXPECT_EQ(t3, *pt3);
	delete pt3;

	delete int_f1;
	delete int_f2;
	delete int_f3;

	delete str_f1;
	delete str_f2;
	delete str_f3;
}


TEST_F(RelationTest, ScanDiffJoin) {

	cout << "Relation Name: " << relation_name << endl;
	string msg;
	CommitID commit_id;

	Field* int_f1 = new IntField(1);
	Field* int_f2 = new IntField(2);
	Field* int_f3 = new IntField(3);

	Field* str_f1 = new StrField("1");
	Field* str_f2 = new StrField("2");
	Field* str_f3 = new StrField("3");

	Tuple t1(tuple_store, 0, schema);	
	t1.SetFieldByIndex(0, int_f1, &msg);
	t1.SetFieldByIndex(1, str_f1, &msg);

	Tuple t21(tuple_store, 500, schema);	
	t21.SetFieldByIndex(0, int_f2, &msg);
	t21.SetFieldByIndex(1, str_f1, &msg);

	Tuple t22(tuple_store, 1000, schema);	
	t22.SetFieldByIndex(0, int_f2, &msg);
	t22.SetFieldByIndex(1, str_f2, &msg);

	Tuple t3(tuple_store, 1500, schema);	
	t3.SetFieldByIndex(0, int_f3, &msg);
	t3.SetFieldByIndex(1, str_f3, &msg);

	ASSERT_TRUE(storage->InsertTuple(&t1, &msg)) << msg;
	ASSERT_TRUE(storage->Commit(&commit_id, &msg)) << msg;

	string master_branch = "master";
	string branch1 = "branch1";

	EXPECT_TRUE(storage->Branch(master_branch, branch1, &msg));

	ASSERT_TRUE(storage->InsertTuple(&t21, &msg)) << msg;
	ASSERT_TRUE(storage->Commit(&commit_id, &msg)) << msg;

	ASSERT_TRUE(storage->InsertTuple(&t3, &msg)) << msg;
	ASSERT_TRUE(storage->Commit(&commit_id, &msg)) << msg;

	ASSERT_TRUE(storage->Switch(master_branch, &msg));

	ASSERT_TRUE(storage->InsertTuple(&t22, &msg)) << msg;
	ASSERT_TRUE(storage->Commit(&commit_id, &msg)) << msg;

/*

t1 -> t21 -> t3 (branch1)
 | -> t22 (master)

*/
	storage->SetReadBuffer(read_page);

//Scan branch1
	Tuple::Iterator* it = storage->Scan(branch1, &msg);
	unsigned tuple_count = 0;
	vector<Tuple*> tuples = {&t1, &t21,&t3};
	unsigned expected_count = tuples.size();

	while(!it->End()){
		const Tuple* ct = it->GetTuple();
		auto ft = find_if(tuples.begin(), tuples.end(), [ct](Tuple* t) { return *t == *ct;});
		ASSERT_NE(ft, tuples.end());
		tuples.erase(ft);
		++tuple_count;
		it->Next();
	}

	ASSERT_EQ(tuple_count,expected_count); 
	ASSERT_EQ(tuples.size(), 0);

//Scan master branch 
	it = storage->Scan(master_branch, &msg);
	tuples = {&t1, &t22};
	expected_count = tuples.size();
	tuple_count = 0;

	while(!it->End()){
		const Tuple* ct = it->GetTuple();
		auto ft = find_if(tuples.begin(), tuples.end(), [ct](Tuple* t) { return *t == *ct;});
		ASSERT_NE(ft, tuples.end());
		tuples.erase(ft);
		++tuple_count;
		it->Next();
	}

	ASSERT_EQ(tuple_count,expected_count); 
	ASSERT_EQ(tuples.size(), 0);

//Diff branch1 - master

	it = storage->Diff(branch1, master_branch, &msg);
	tuples = {&t3};
	expected_count = tuples.size();
	tuple_count = 0;

	while(!it->End()){
		const Tuple* ct = it->GetTuple();
		auto ft = find_if(tuples.begin(), tuples.end(), [ct](Tuple* t) { return *t == *ct;});
		ASSERT_NE(ft, tuples.end());
		tuples.erase(ft);
		++tuple_count;
		it->Next();
	}

	ASSERT_EQ(tuple_count,expected_count); 
	ASSERT_EQ(tuples.size(), 0);

//Diff master - branch1
	it = storage->Diff(master_branch,branch1, &msg);
	tuples = {};
	expected_count = tuples.size();
	tuple_count = 0;

	while(!it->End()){
		const Tuple* ct = it->GetTuple();
		auto ft = find_if(tuples.begin(), tuples.end(), [ct](Tuple* t) { return *t == *ct;});
		ASSERT_NE(ft, tuples.end());
		tuples.erase(ft);
		++tuple_count;
		it->Next();
	}

	ASSERT_EQ(tuple_count,expected_count); 
	ASSERT_EQ(tuples.size(), 0);

//Join branch1 and master
	it = storage->Join(branch1,master_branch, 0, &msg);
	tuples = {&t1,&t21};
	expected_count = tuples.size();
	tuple_count = 0;

	while(!it->End()){
		const Tuple* ct = it->GetTuple();
		auto ft = find_if(tuples.begin(), tuples.end(), [ct](Tuple* t) { return *t == *ct;});
		ASSERT_NE(ft, tuples.end());
		tuples.erase(ft);
		++tuple_count;
		it->Next();
	}

	ASSERT_EQ(tuple_count,expected_count); 
	ASSERT_EQ(tuples.size(), 0);


//Join master and branch1 with predicate
	Predicate p("StrF", kEQ, *str_f2);
	it = storage->Join(master_branch,branch1, &p, &msg);
	tuples = {&t22};
	expected_count = tuples.size();
	tuple_count = 0;

	while(!it->End()){
		const Tuple* ct = it->GetTuple();
		auto ft = find_if(tuples.begin(), tuples.end(), [ct](Tuple* t) { return *t == *ct;});
		ASSERT_NE(ft, tuples.end());
		tuples.erase(ft);
		++tuple_count;
		it->Next();
	}

	ASSERT_EQ(tuple_count,expected_count); 
	ASSERT_EQ(tuples.size(), 0);

//Join master and branch1 without predicate
	it = storage->Join(master_branch,branch1, 0, &msg);
	tuples = {&t1, &t22};
	expected_count = tuples.size();
	tuple_count = 0;

	while(!it->End()){
		const Tuple* ct = it->GetTuple();
		auto ft = find_if(tuples.begin(), tuples.end(), [ct](Tuple* t) { return *t == *ct;});
		ASSERT_NE(ft, tuples.end());
		tuples.erase(ft);
		++tuple_count;
		it->Next();
	}

	ASSERT_EQ(tuple_count,expected_count); 
	ASSERT_EQ(tuples.size(), 0);
	delete it;

	delete int_f1;
	delete int_f2;
	delete int_f3;

	delete str_f1;
	delete str_f2;
	delete str_f3;
}