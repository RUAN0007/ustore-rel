/* 

   @Author: RUAN0007
   @Date:   2017-01-06 19:45:52
   @Last_Modified_At:   2017-01-06 20:52:10
   @Last_Modified_By:   RUAN0007

*/


#include <gtest/gtest.h>

#include "tuple.h"
#include "field.h"

#include "debug.h"
#include <vector>

using namespace std;

using namespace ustore::relation;

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

	return new TupleDscp(types, names);
}

TEST(TupleDscp, NormalMetaData) { 

	const TupleDscp* schema = GetStandardSchema();

	EXPECT_EQ(schema->GetFieldName(1), "2Str");

	EXPECT_TRUE(Type::IsIntType(schema->GetFieldType(0)));	
	EXPECT_TRUE(Type::IsStrType(schema->GetFieldType(1)));	

	EXPECT_EQ(schema->GetFieldNumber(),4);
	EXPECT_EQ(schema->FieldNameToIndex("3Str"),2);

	unsigned expected_position = IntType::GetInstance()->GetLen() + StrType::GetInstance()->GetLen();
	EXPECT_EQ(schema->GetFieldOffset(2), expected_position);


	size_t size = 2 * (IntType::GetInstance()->GetLen() + StrType::GetInstance()->GetLen());
	EXPECT_EQ(schema->GetTupleSize(), size);
	delete schema;
}

TEST(TupleDscp, AbnormalMetaData) { 

	const TupleDscp* schema = GetStandardSchema();

	EXPECT_STREQ(schema->GetFieldName(6).c_str(), "");
	EXPECT_EQ(schema->GetFieldType(10), static_cast<Type*>(0));

	EXPECT_EQ(schema->FieldNameToIndex("13Str"),-1);
	EXPECT_EQ(schema->GetFieldOffset(12), -1);
	delete schema;
}

TEST(Tuple, Field) {
	unsigned char* bytes = new unsigned char[1000];
	const TupleDscp* schema = GetStandardSchema();

	Tuple t(bytes,10,  schema);


	Field *int1 = new IntField(999);
	ASSERT_TRUE(t.SetFieldByName("1Int", int1, 0));

	Field *str2 = new StrField("Str2");	
	ASSERT_TRUE(t.SetFieldByName("2Str", str2, 0));

	Field *str3 = new StrField("Str3");	
	ASSERT_TRUE(t.SetFieldByIndex(2, str3, 0));

	Field *int4 = new IntField(-99);
	ASSERT_TRUE(t.SetFieldByName("4Int", int4, 0));

	//Set a non-exist field
	ASSERT_FALSE(t.SetFieldByName("10Int", int4, 0));

	IntField* pk_field = dynamic_cast<IntField*>(t.GetPK());
	EXPECT_EQ(pk_field->value(),999);

	StrField* str_field1 = dynamic_cast<StrField*>(t.GetFieldByIndex(1));
	EXPECT_STREQ(str_field1->value().c_str(),"Str2");

	StrField* str_field2 = dynamic_cast<StrField*>(t.GetFieldByName("3Str"));
	EXPECT_STREQ(str_field2->value().c_str(),"Str3");

	IntField* int_field2 = dynamic_cast<IntField*>(t.GetFieldByName("4Int"));
	EXPECT_EQ(int_field2->value(),-99);


//Reset the field value
	Field *str4 = new StrField("Str4");	
	ASSERT_TRUE(t.SetFieldByIndex(1, str4, 0));

	StrField* str_field3 = dynamic_cast<StrField*>(t.GetFieldByIndex(1));
	EXPECT_STREQ(str_field3->value().c_str(),"Str4");

	delete pk_field;
	delete int1; 
	delete str2;
	delete str3;
	delete int4; 

	delete schema;
	delete[] bytes;
}