/*
 * type.cc
 *
 *  Created on: Jan 3, 2017
 *      Author: Ruan Pingcheng
 */

#include "field.h"

#include "debug.h"

using namespace std;
namespace ustore{
namespace relation{

size_t GetTypeSize(Type t) {
	if(t == INT_TYPE) {
		return 4;
	}else if(t == STR_TYPE){
		return 256;
	}
	return 0;
}

string GetTypeName(Type t) {
	if(t == INT_TYPE) {
		return "INT";
	}else if(t == STR_TYPE){
		return "STR";
	}
	return 0;
}

bool IntField::IsSatisified(ComparisonOp op, const Field& field) const{
	if (field.GetType() != INT_TYPE) return false;

	const IntField& int_field = dynamic_cast<const IntField&>(field);
	int cmp_value = int_field.value();

	if(op == kEQ) {
		return value() == cmp_value;
	}else if(op == kNOT_EQ) {
		return value() != cmp_value;
	}else if(op == kLESS) {
		return value() < cmp_value;
	}else if(op == kGREATER) {
		return value() > cmp_value;
	}else if(op == kLESS_EQ) {
		return value() <= cmp_value;
	}else if(op == kGREATER_EQ) {
		return value() >= cmp_value;
	}

	LOG(LOG_FATAL,"No Other Comparison Op. Shall not reach here.");
	return false;
}

bool StrField::IsSatisified(ComparisonOp op, const Field& field) const{
	if (field.GetType() != STR_TYPE) return false;

	const StrField& str_field = dynamic_cast<const StrField&>(field);
	string cmp_value = str_field.value();

	if(op == kEQ) {
		return value() == cmp_value;
	}else if(op == kNOT_EQ) {
		return value() != cmp_value;
	}else if(op == kLESS) {
		return value() < cmp_value;
	}else if(op == kGREATER) {
		return value() > cmp_value;
	}else if(op == kLESS_EQ) {
		return value() <= cmp_value;
	}else if(op == kGREATER_EQ) {
		return value() >= cmp_value;
	}

	LOG(LOG_FATAL,"No Other Comparison Op. Shall not reach here.");
	return false;
}

std::istream& operator>>(std::istream &in, StrField& str_field){
	size_t len = GetTypeSize(str_field.GetType());
	char* bytes = new char[len];
	in.read(bytes,len);

	str_field.value_ = std::string(bytes);
	delete[] bytes;
	return in;
};

std::ostream& operator<<(std::ostream &out, StrField& str_field){
	size_t len = GetTypeSize(str_field.GetType());
	char* bytes = new char[len];
	
	const char* char_ptr = str_field.value().c_str();

	char* byte_ptr = bytes;

	size_t count = 0;
	while(*char_ptr != 0) {
		*byte_ptr = *char_ptr;

		char_ptr++;
		byte_ptr++;
		count ++;
	}

	while(count < len) {
		*byte_ptr = 0;
		byte_ptr++;	
		count++;
	}

	out.write(bytes, len);

	delete[] bytes;
	return out;
};

std::istream& operator>>(std::istream &in, IntField& int_field){
	size_t len =  GetTypeSize(int_field.GetType());
	unsigned char* bytes = new unsigned char[len];
	// LOG(LOG_DEBUG, "Before Read: %s", bytes);
	in.read(reinterpret_cast<char*>(bytes),len);
	// LOG(LOG_DEBUG, "After Read: %s", bytes);

	int_field.value_ = (bytes[3] << 24) | (bytes[2] << 16) | (bytes[1] << 8) | (bytes[0]);
	// LOG(LOG_DEBUG, "field value: %d", int_field.value_);
	delete[] bytes;
	return in;
};

std::ostream& operator<<(std::ostream &out, IntField int_field){
	size_t len =  GetTypeSize(int_field.GetType());
	unsigned char bytes[len];

	int value = int_field.value_;

	for (size_t i = 0;i < len;i++) {
		bytes[i] = (value >> (8 * i)) & 0xFF;
	}

	out.write(reinterpret_cast<char*>(bytes), len);
	// delete[] bytes;
	out << int_field.value();
	return out;
};

}
}//namespace ustore