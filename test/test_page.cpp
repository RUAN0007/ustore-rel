// /* 

//    @Author: RUAN0007
//    @Date:   2017-01-07 15:00:36
//    @Last_Modified_At:   2017-01-23 18:34:46
//    @Last_Modified_By:   RUAN0007

// */


#include <gtest/gtest.h>

#include "tuple.h"
#include "field.h"
#include "page.h"

#include <cstring>
#include "debug.h"
#include <vector>

using namespace std;
using namespace ustore::relation;
const TupleDscp* GetSchema() {
    vector<const Type*> types;
    types.push_back(IntType::GetInstance());
    types.push_back(StrType::GetInstance());
    types.push_back(StrType::GetInstance());
    types.push_back(IntType::GetInstance());
    vector<string> names;
    names.push_back("1Int");
    names.push_back("2Str");
    names.push_back("3Str");
    names.push_back("4Int");
    return new TupleDscp("Test_Schema", types, names);
}

Tuple GetTuple(unsigned char* bytes, unsigned position, const TupleDscp* schema) {

    Tuple t(bytes,position, schema);
    string* msg =new string();
    Field *int1 = new IntField(999);
    t.SetFieldByName("1Int", int1, msg);
    Field *str2 = new StrField("Str2");
    t.SetFieldByName("2Str", str2, msg);
    Field *str3 = new StrField("Str3");
    t.SetFieldByIndex(2, str3, msg);
    Field *int4 = new IntField(-99);
    t.SetFieldByName("4Int", int4, msg);
    return t;
}


TEST(Page, MetaData) { 

    const TupleDscp* schema = GetSchema();
    Page p("table1", schema, 4096);
    ASSERT_STREQ(p.GetTableName().c_str(), "table1");
    ASSERT_EQ(p.GetPageSize(), 4096);
    ASSERT_EQ(p.GetTupleNumber(), 0);
    ASSERT_EQ(*p.GetTupleSchema(), *schema);
    delete schema;
}


TEST(Page, AccessTuple) {

    const TupleDscp* schema = GetSchema();
    unsigned char* tuple_buffer1 = new unsigned char[1024]{0};
    Tuple t1 = GetTuple(tuple_buffer1,10,schema);
    unsigned char* tuple_buffer2 = new unsigned char[1024]{0};
    Tuple t2 = GetTuple(tuple_buffer2,15,schema);
    Page p("table1", schema, 4096);
    string* msg =new string();
    EXPECT_EQ(p.InsertTuple(&t1, msg), 0);
    EXPECT_EQ(p.InsertTuple(&t2, msg), 1);
    EXPECT_EQ(p.GetTupleNumber(), 2);
    Tuple* t3 = p.GetTuple(0);
    EXPECT_EQ(reinterpret_cast<IntField*>(t3->GetFieldByName("1Int"))->value(),reinterpret_cast<IntField*>(t1.GetFieldByName("1Int"))->value());
    EXPECT_EQ(t1,*t3);
    Tuple* t4 = p.GetTuple(2);
    EXPECT_EQ(t4,reinterpret_cast<Tuple*>(0));
    delete t3;
    delete t4;
    delete schema;
}

TEST(Page, SetData) {

    const TupleDscp* schema = GetSchema();
    unsigned char* tuple_buffer1 = new unsigned char[1024]{0};
    Tuple t1 = GetTuple(tuple_buffer1,10,schema);
    unsigned char* tuple_buffer2 = new unsigned char[1024]{0};
    Tuple t2 = GetTuple(tuple_buffer2,15,schema);
    Page p("table1", schema, 4096);
    string* msg =new string();
    EXPECT_EQ(p.InsertTuple(&t1, msg), 0);
    EXPECT_EQ(p.InsertTuple(&t2, msg), 1);
    EXPECT_EQ(p.GetTupleNumber(), 2);
    Page p1("table1", schema, 4096);
    unsigned char* p2_data = new unsigned char[4096]{0};
    memcpy(p2_data, p.GetRawData(),p.GetPageSize());
    // cout << "P1_data: " << string((char*)p2_data) << endl;
    p1.SetData(p2_data, p.GetPageSize());
    EXPECT_EQ(p1.GetTupleNumber(), 2);
    delete schema;
}