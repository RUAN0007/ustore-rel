/*
 * type.h
 *
 *  Created on: Jan 3, 2017
 *      Author: ruanpingcheng
 */

#ifndef INCLUDE_TYPE_H_
#define INCLUDE_TYPE_H_


#include "string"

namespace ustore{
namespace relation{


class Field;

class Type
{
public:

//Number of bytes of this type
virtual size_t GetLen() const = 0;

// virtual Field* Parse(char bytes[]) const = 0;

virtual const std::string to_str() const = 0;	

virtual Field* Parse(const unsigned char bytes[]) const = 0;

virtual const unsigned char* Serialize(const Field* f) const = 0;

static bool IsIntType(const Type* type);

static bool IsStrType(const Type* type);

virtual ~Type(){};
}; //class Type

class IntType : public Type{
public:

size_t  GetLen() const override{ return 4;}

Field* Parse(const unsigned char bytes[]) const override;

const unsigned char* Serialize(const Field* f) const override;

const std::string to_str() const override{ return "INT_TYPE";}

static const IntType* GetInstance(){ 
	const static IntType instance; 
	return &instance;
}


private:

IntType(const IntType&); // Prevent construction by copying
IntType& operator=(const IntType&); // Prevent assignment
~IntType() {}; // Prevent unwanted destruction
IntType(){}
};//INT_TYPE

class StrType : public Type{
public:
static const int kSTR_LEN = 256;
size_t  GetLen() const override{ return StrType::kSTR_LEN;}

Field* Parse(const unsigned char bytes[]) const override;

const unsigned char* Serialize(const Field* f) const override;

const std::string to_str() const override{ return "STR_TYPE";}

static const StrType* GetInstance(){
	const static StrType instance;
 	return &instance;
}


private:
StrType(const IntType&){}; // Prevent construction by copying
StrType& operator=(const StrType&); // Prevent assignment
StrType(){}
~StrType() {}; // Prevent unwanted destruction
};//STR_TYPE
}//namespace ustore
}//namesapce relation
#endif
