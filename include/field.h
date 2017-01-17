/*
 * type.h
 *
 *  Created on: Jan 3, 2017
 *      Author: ruanpingcheng
 */

#ifndef INCLUDE_FIELD_H_
#define INCLUDE_FIELD_H_

#include "operator.h"

#include "debug.h"
#include "type.h"

#include <iostream>
#include <fstream> 
#include <sstream>  

namespace ustore{
namespace relation{

// enum Type{
// 	INT_TYPE,
// 	STR_TYPE
// };

// size_t GetTypeSize(Type t);

// std::string GetTypeName(Type t);

class Field{
public:

virtual const Type* GetType() const = 0;
virtual bool IsSatisified(ComparisonOp op, const Field* field) const = 0;
virtual std::string to_str() const = 0;
virtual Field* clone() const = 0;
virtual bool equal(const Field* f) const = 0;
virtual std::size_t hash() const = 0;
inline virtual ~Field(){};
}; //class Field

//a customized functor to compare field pointer
//used for STL container
struct FieldLess {
    bool operator()(const Field* lhs, const Field* rhs) const{
        return lhs->hash() < rhs->hash();
    }
};

class IntField:public Field{

public:
IntField(){};
~IntField() {};
explicit IntField(int value):value_(value){};

inline int value() const{ return value_;};

virtual bool IsSatisified(ComparisonOp op, const Field* field) const override;

inline const unsigned char* Serialize() {
	return this->GetType()->Serialize(this);
}

inline Field* clone() const override{
	return new IntField(*this);
}

inline bool equal(const Field* f) const override {
	const IntField *int_f = dynamic_cast<const IntField*>(f);
	if(int_f == 0) return false;

	return this->value() == int_f->value();
}

inline std::string to_str() const override{
	std::ostringstream ss;
	ss << GetType()->to_str();
	ss << ":";
	ss << value_ << " ";
	return ss.str();
}

inline virtual const Type* GetType() const override{
	return IntType::GetInstance();
}

inline virtual std::size_t hash() const override {
	return static_cast<std::size_t>(this->value());
}

friend std::istream& operator>>(std::istream &in, IntField& int_field);

friend std::ostream& operator<<(std::ostream &out, IntField int_field);

private:
int value_;

}; //class IntField

class StrField:public Field{
public:
StrField(){};
~StrField(){};
//Max char number 255
explicit StrField(std::string value){
	size_t max_len = StrType::GetInstance()->GetLen();
	if (value.size() > max_len){
		value_ = value.substr(0,max_len);
	}else{
		value_ = value;
	}
};

inline std::string value() const {return value_;}

inline bool equal(const Field* f) const override {
	const StrField *str_f = dynamic_cast<const StrField*>(f);
	if(str_f == 0)  {
		// std::cout << "I am here" << std::endl;
		return false;
	}
	return this->value() == str_f->value();
}

inline virtual std::size_t hash() const override {
	return std::hash<std::string>()(this->value());
}

inline std::string to_str() const override{
	std::ostringstream ss;
	ss << GetType()->GetLen();
	ss << ":";
	ss << value_ << " ";
	return ss.str();
}

bool IsSatisified(ComparisonOp op, const Field* field) const override;

inline Field* clone() const override{
	return new StrField(*this);
}


inline virtual const Type* GetType() const override{
	return StrType::GetInstance();
}

friend std::istream& operator>>(std::istream &in, StrField& str_field);

friend std::ostream& operator<<(std::ostream &out, StrField str_field);
private:
std::string value_;
};//end of StrField
}//namespace ustore
}//namesapce relation
#endif
