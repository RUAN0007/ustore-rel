/*

   @Author: RUAN0007
   @Date:   2017-01-06 19:45:52
   @Last_Modified_At:   2017-01-23 18:15:01
   @Last_Modified_By:   RUAN0007

*/

#include <gtest/gtest.h>

#include <vector>
#include <cstring>
#include <string>

#include "./tuple.h"
#include "./field.h"
#include "./operator.h"

#include "./debug.h"

using std::vector;
using std::string;

using ustore::relation::Type;
using ustore::relation::IntType;
using ustore::relation::StrType;

using ustore::relation::TupleDscp;
using ustore::relation::Tuple;
using ustore::relation::Field;
using ustore::relation::IntField;
using ustore::relation::StrField;

using ustore::relation::Predicate;
using ustore::relation::ComparisonOp;

const TupleDscp* GetStandardSchema() {
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

const TupleDscp* GetStandardSchema1() {
    vector<const Type*> types;
    types.push_back(IntType::GetInstance());
    types.push_back(IntType::GetInstance());
    types.push_back(StrType::GetInstance());
    types.push_back(IntType::GetInstance());

    vector<string> names;
    names.push_back("1Int");
    names.push_back("2Str");
    names.push_back("3Str");
    names.push_back("4Int");

    return new TupleDscp("Test_Schema", types, names);
}

TEST(TupleDscp, EQUAL) {
    const TupleDscp* s1 = GetStandardSchema();
    const TupleDscp* s2 = GetStandardSchema();

    EXPECT_TRUE(*s1 == *s2);

    delete s1;
    delete s2;
}

TEST(TupleDscp, NotEQUAL) {
    const TupleDscp* s1 = GetStandardSchema();
    const TupleDscp* s2 = GetStandardSchema1();

    EXPECT_TRUE(*s1 != *s2);

    delete s1;
    delete s2;
}

TEST(TupleDscp, NormalMetaData) {
    const TupleDscp* schema = GetStandardSchema();

    EXPECT_EQ(schema->GetFieldName(1), "2Str");

    EXPECT_TRUE(Type::IsIntType(schema->GetFieldType(0)));

    EXPECT_TRUE(Type::IsStrType(schema->GetFieldType(1)));

    EXPECT_EQ(schema->GetFieldNumber(), 4);
    EXPECT_EQ(schema->FieldNameToIndex("3Str"), 2);

    unsigned expected_position =
        IntType::GetInstance()->GetLen() + StrType::GetInstance()->GetLen();
    EXPECT_EQ(schema->GetFieldOffset(2), expected_position);

    size_t size = 2 * (IntType::GetInstance()->GetLen()
                    + StrType::GetInstance()->GetLen());
    EXPECT_EQ(schema->GetTupleSize(), size);
    delete schema;
}

TEST(TupleDscp, AbnormalMetaData) {
    const TupleDscp* schema = GetStandardSchema();

    EXPECT_STREQ(schema->GetFieldName(6).c_str(), "");
    EXPECT_EQ(schema->GetFieldType(10), static_cast<Type*>(0));

    EXPECT_EQ(schema->FieldNameToIndex("13Str"), -1);
    EXPECT_EQ(schema->GetFieldOffset(12), -1);
    delete schema;
}

TEST(Tuple, EQUAL) {
    const TupleDscp* schema = GetStandardSchema();

    unsigned char* b1 = new unsigned char[1000]{0};
    unsigned char* b2 = new unsigned char[1000]{0};
    Tuple t1(b1, 10,  schema);
    Tuple t2(b2, 15,  schema);

    ASSERT_EQ(memcmp(b1, b2, 1000), 0);

    string* msg = new string();

    Field *int1 = new IntField(999);
    Field *str2 = new StrField("Str2");

    Field *str3 = new StrField("Str3");

    Field *int4 = new IntField(-99);

    t1.SetFieldByName("1Int", int1, msg);
    t1.SetFieldByName("2Str", str2, msg);
    t1.SetFieldByIndex(2, str3, msg);
    t1.SetFieldByName("4Int", int4, msg);

    t2.SetFieldByName("1Int", int1, msg);
    t2.SetFieldByName("2Str", str2, msg);
    t2.SetFieldByIndex(2, str3, msg);
    t2.SetFieldByName("4Int", int4, msg);

    ASSERT_NE(memcmp(b1, b2, 1000), 0);

    ASSERT_EQ(t1, t2);

    unsigned char* b3 = new unsigned char[1000]{0};
    Tuple t3(b3, 20,  schema);

    Field *int5 = new IntField(-9);
    t3.SetFieldByName("1Int", int1, msg);
    t3.SetFieldByName("2Str", str2, msg);
    t3.SetFieldByIndex(2, str3, msg);
    t3.SetFieldByName("4Int", int5, msg);

    EXPECT_NE(t1, t3);

    delete int1;

    delete str2;
    delete str3;
    delete int4;

    delete schema;
    delete[] b1;
    delete[] b2;
    delete[] b3;
}

TEST(Tuple, Predicate) {
    unsigned char* bytes = new unsigned char[1000];
    const TupleDscp* schema = GetStandardSchema();

    Tuple t(bytes, 10,  schema);

    string* msg = new string();

    Field *int1 = new IntField(999);
    t.SetFieldByName("1Int", int1, msg);

    Field *str2 = new StrField("Str2");

    t.SetFieldByName("2Str", str2, msg);

    Field *str3 = new StrField("Str3");

    t.SetFieldByIndex(2, str3, msg);

    Field *int4 = new IntField(-99);
    t.SetFieldByName("4Int", int4, msg);

    Predicate p1("1Int",  ComparisonOp::kLESS, IntField(1000));

    EXPECT_TRUE(t.IsSatisfy(&p1));

    Predicate p2("4Int",  ComparisonOp::kGREATER,  IntField(0));

    EXPECT_FALSE(t.IsSatisfy(&p2));

    Predicate p3("2Str",  ComparisonOp::kEQ,  StrField("Str2"));

    EXPECT_TRUE(t.IsSatisfy(&p3));

    Predicate p4("3Str",  ComparisonOp::kEQ,  StrField("Str22"));

    EXPECT_FALSE(t.IsSatisfy(&p4));

    Predicate p5("5Str",  ComparisonOp::kEQ,  StrField("Str22"));

    EXPECT_FALSE(t.IsSatisfy(&p5));

    delete int1;

    delete str2;
    delete str3;
    delete int4;

    delete schema;
    delete[] bytes;
}

TEST(Tuple, Field) {
    unsigned char* bytes = new unsigned char[1000];
    const TupleDscp* schema = GetStandardSchema();

    Tuple t(bytes, 10,  schema);

    string* msg = new string();

    Field *int1 = new IntField(999);
    ASSERT_TRUE(t.SetFieldByName("1Int", int1, msg));

    Field *str2 = new StrField("Str2");

    ASSERT_TRUE(t.SetFieldByName("2Str", str2, msg));

    Field *str3 = new StrField("Str3");

    ASSERT_TRUE(t.SetFieldByIndex(2, str3, msg));

    Field *int4 = new IntField(-99);
    ASSERT_TRUE(t.SetFieldByName("4Int", int4, msg));

    // Set a non-exist field
    ASSERT_FALSE(t.SetFieldByName("10Int", int4, msg));

    IntField* pk_field = dynamic_cast<IntField*>(t.GetPK());
    EXPECT_EQ(pk_field->value(), 999);

    StrField* str_field1 = dynamic_cast<StrField*>(t.GetFieldByIndex(1));
    EXPECT_STREQ(str_field1->value().c_str(), "Str2");

    StrField* str_field2 = dynamic_cast<StrField*>(t.GetFieldByName("3Str"));
    EXPECT_STREQ(str_field2->value().c_str(), "Str3");

    IntField* int_field2 = dynamic_cast<IntField*>(t.GetFieldByName("4Int"));
    EXPECT_EQ(int_field2->value(), -99);

// Reset the field value
    Field *str4 = new StrField("Str4");

    ASSERT_TRUE(t.SetFieldByIndex(1, str4, msg));

    StrField* str_field3 = dynamic_cast<StrField*>(t.GetFieldByIndex(1));

    EXPECT_STREQ(str_field3->value().c_str(), "Str4");

    delete pk_field;
    delete int1;

    delete str2;
    delete str3;
    delete int4;

    delete schema;
    delete[] bytes;
}
