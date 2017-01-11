/*
 * type.cc
 *
 *  Created on: Jan 3, 2017
 *      Author: Ruan Pingcheng
 */

#include "type.h"

#include "field.h"
#include "debug.h"
using namespace std;

namespace ustore{
namespace relation{


bool Type::IsIntType(const Type* type){
	return IntType::GetInstance() == type;
}

bool Type::IsStrType(const Type* type){
	return StrType::GetInstance() == type;
}

Field* IntType::Parse(const unsigned char bytes[]) const{

	int value = (bytes[3] << 24) | (bytes[2] << 16) | (bytes[1] << 8) | (bytes[0]);
	return new IntField(value);

}

const unsigned char* IntType::Serialize(const Field* f) const {

	const IntField* int_field = dynamic_cast<const IntField*>(f);

	if(int_field == 0) {
		LOG(LOG_FATAL, "IntType can not serialize a non IntField");
	}

	int value = int_field->value();
	size_t len = GetLen();
	unsigned char* bytes = new unsigned char[len];

	for (size_t i = 0;i < len;i++) {
		bytes[i] = (value >> (8 * i)) & 0xFF;
	}

	return bytes;
}

Field* StrType::Parse(const unsigned char bytes[]) const{
	size_t max_len = StrType::GetInstance()->GetLen();

	unsigned effective_size = 0;

	//Find the position of first NULL character
	//This should be the number of characters in the string

	const unsigned char* byte_ptr = bytes;
	while(*byte_ptr != 0 && effective_size < max_len) {
		++ effective_size;
		++ byte_ptr;
	}


	string value( reinterpret_cast<const char*>(bytes),effective_size);

	// cout << "value: " << endl;
	// string value( reinterpret_cast<const char*>(bytes));
	return new StrField(value);
}

const unsigned char* StrType::Serialize(const Field* f) const {

	const StrField* str_field = dynamic_cast<const StrField*>(f);
	if(str_field == 0) {
		LOG(LOG_FATAL, "StrType can not serialize a non StrField");
	}
	size_t len = GetLen();
	
	const char* char_ptr = str_field->value().c_str();
	unsigned char* bytes = new unsigned char[len];
	unsigned char* byte_ptr = bytes;

	//Copy c_str of StrField ValUE to bytes.
	//Zero padding bytes to the end
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

	return bytes;
}


}//namespace relation
}//namespace ustore