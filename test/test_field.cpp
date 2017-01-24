/*
 * TablePattern-test.cpp
 *
 *  Created on: Oct 7, 2016
 *      Author: ruanpingcheng
 */
#include <gtest/gtest.h>


#include <map>
#include <sstream>

#include "./field.h"

using std::stringstream;
using std::map;
using std::string;

using ustore::relation::Type;
using ustore::relation::IntType;
using ustore::relation::StrType;

using ustore::relation::IntField;
using ustore::relation::StrField;
using ustore::relation::Field;
using ustore::relation::FieldLess;

using ustore::relation::ComparisonOp;

TEST(STREAM, Zero_Int_Field) {
    IntField origin_zero(0);
    stringstream ss;
    ss << origin_zero;
    ss.flush();
    IntField new_zero(1);
    ss >> new_zero;
    EXPECT_EQ(new_zero.value(), 0);
}


TEST(STREAM, One_Int_Field) {
    IntField origin_one(1);
    stringstream ss;
    // cout << "Before: " << ss.str() << endl;
    ss << origin_one;
    // cout << "After: " << ss.str() << endl;
    ss.flush();
    IntField new_one(0);
    ss >> new_one;
    EXPECT_EQ(new_one.value(), 1);
}

TEST(STREAM, Neg_Int_Field) {
    IntField origin_field(-10000);
    stringstream ss;
    // cout << "Before: " << ss.str() << endl;
    ss << origin_field;
    // cout << "After: " << ss.str() << endl;
    ss.flush();
    IntField new_field(0);
    ss >> new_field;
    EXPECT_EQ(new_field.value(), -10000);
}

TEST(STREAM, Big_Int_Field) {
    int big = 99999;
    IntField origin_field(big);
    stringstream ss;
    // cout << "Before: " << ss.str() << endl;
    ss << origin_field;
    // cout << "After: " << ss.str() << endl;
    ss.flush();
    IntField new_field(0);
    ss >> new_field;
    EXPECT_EQ(new_field.value(), big);
}


TEST(STREAM, Empty_Str_Field) {
    StrField origin_field("");
    stringstream ss;
    // cout << "Before: " << ss.str() << endl;
    ss << origin_field;
    // cout << "After: " << ss.str() << endl;
    ss.flush();
    StrField new_field("NOT EMPTY");
    ss >> new_field;
    EXPECT_EQ(new_field.value(), "");    }

TEST(STREAM, Normal_Str_Field) {
    string value = "Normal String";
    StrField origin_field(value);
    stringstream ss;
    // cout << "Before: " << ss.str() << endl;
    ss << origin_field;
    // cout << "After: " << ss.str() << endl;
    ss.flush();
    StrField new_field("NOT EMPTY");
    ss >> new_field;
    EXPECT_EQ(new_field.value(), value);    }


string GetStrByLen(size_t len) {
    stringstream s;
    for (size_t i = 0; i < len; i++) {
        s << "a";
    }
    return s.str();;
}

TEST(STREAM, Nearflow_Str_Field) {
    string value = GetStrByLen(256);
    StrField origin_field(value);
    stringstream ss;
    // cout << "Before: " << ss.str() << endl;
    ss << origin_field;
    // cout << "After: " << ss.str() << endl;
    ss.flush();
    StrField new_field("NOT EMPTY");
    ss >> new_field;
    EXPECT_EQ(new_field.value(), value.substr(0, 256));
}

TEST(STREAM, overflow_Str_Field) {
    string value = GetStrByLen(257);
    StrField origin_field(value);
    stringstream ss;
    // cout << "Before: " << ss.str() << endl;
    ss << origin_field;
    // cout << "After: " << ss.str() << endl;
    ss.flush();
    StrField new_field("NOT EMPTY");
    ss >> new_field;
    EXPECT_EQ(new_field.value(), value.substr(0, 256));
}

TEST(FIELD, EQUAL) {
    Field *f1 = new IntField(1);
    Field *f2 = new IntField(1);
    EXPECT_TRUE(f1->equal(f2));
    Field *f3 = new StrField("s");
    Field *f4 = new StrField("r");
    Field *f5 = new StrField("r");
    EXPECT_FALSE(f1->equal(f3));
    EXPECT_FALSE(f3->equal(f4));
    EXPECT_TRUE(f4->equal(f5));
    delete f1;
    delete f2;
    delete f3;
    delete f4;
    delete f5;
}

TEST(FIELD, STL) {
    const Field *f1 = new IntField(1);
    const Field *f2 = new IntField(1);
    const Field *f3 = new IntField(2);
    map<const Field*, unsigned, FieldLess> fmap;
    fmap[f1] = 1;
    fmap[f3] = 2;
    auto ff1 = fmap.find(f2);
    ASSERT_NE(ff1, fmap.end());
    ASSERT_EQ((*ff1).second, 1);
    // cout << fmap[f2] << endl;
    delete f1;
    delete f2;
    delete f3;
}
