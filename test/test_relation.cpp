/* 

   @Author: RUAN0007
   @Date:   2017-01-11 09:22:13
   @Last_Modified_At:   2017-01-11 14:47:20
   @Last_Modified_By:   RUAN0007

*/


#include <gtest/gtest.h>

#include "relation.h"

#include "client.h"
#include "type.h"
#include "types.h"
#include "field.h"
#include "tuple.h"
#include "page.h"
#include "predicate.h"
#include "version.h"

#include "debug.h"

using namespace std;
using namespace ustore::relation;
using namespace ustore;

class RelationTest : public ::testing::Test {
 protected:
  virtual void SetUp() {

  	vector<const Type*> types;
  	types.push_back(IntType::GetInstance());
  	types.push_back(StrType::GetInstance());

  	vector<string> names;
  	names.push_back("IntF");
  	names.push_back("StrF");

  	schema = new TupleDscp(relation_name, types, names);


	read_page = new Page(relation_name, schema, 1024);

	write_page = new Page(relation_name, schema, 1024);
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

  string relation_name = "Relation";
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