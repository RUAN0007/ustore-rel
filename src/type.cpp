/*
 * type.cc
 *
 *  Created on: Jan 3, 2017
 *      Author: Ruan Pingcheng
 */

#include "type.h"

using namespace std;

namespace ustore{
namespace relation{


// bool Type::IsIntType(const Type* type){
// 	return IntType::GetInstance() == type;
// }

// bool Type::IsStrType(const Type* type){
// 	return StrType::GetInstance() == type;
// }
// // Field* IntType::Parse(char bytes[]) const{
// 	//Assume Little Endian Encoding
// 	//LSD is at starting index of the byte array
// 	int value = (bytes[3] << 24) | (bytes[2] << 16) | (bytes[1] << 8) | (bytes[0]);

// 	return new IntField(value);
// }

// Field* StrType::Parse(char bytes[]) const{
// 	string value(bytes,StrType::kSTR_LEN);

// 	return new StrField(value);
// }

}//namespace relation
}//namespace ustore