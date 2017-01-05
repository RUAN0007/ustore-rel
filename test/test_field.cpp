/*
 * TablePattern-test.cpp
 *
 *  Created on: Oct 7, 2016
 *      Author: ruanpingcheng
 */
#include <gtest/gtest.h>

#include "field.h"

#include <sstream>

using namespace std;
using namespace ustore::relation;

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


TEST(STREAM, Empty_Str_Field){
	StrField origin_field("");
	stringstream ss;

	// cout << "Before: " << ss.str() << endl;
	ss << origin_field;
	// cout << "After: " << ss.str() << endl;
	ss.flush();
	StrField new_field("NOT EMPTY");
	ss >> new_field;

	EXPECT_EQ(new_field.value(), "");	
}

TEST(STREAM, Normal_Str_Field){
	string value = "Normal String";
	StrField origin_field(value);

	stringstream ss;

	// cout << "Before: " << ss.str() << endl;
	ss << origin_field;
	// cout << "After: " << ss.str() << endl;
	ss.flush();
	StrField new_field("NOT EMPTY");
	ss >> new_field;

	EXPECT_EQ(new_field.value(), value);	
}


string GetStrByLen(size_t len) {
	string s;
	for (size_t i = 0;i < len;i++) {
		s += 'a';
	}
	return s;
}

TEST(STREAM, Overflow_Str_Field){
	string value = GetStrByLen(256);
	StrField origin_field(value);

	stringstream ss;

	// cout << "Before: " << ss.str() << endl;
	ss << origin_field;
	// cout << "After: " << ss.str() << endl;
	ss.flush();
	StrField new_field("NOT EMPTY");
	ss >> new_field;

	EXPECT_EQ(new_field.value(), value.substr(255));	
}