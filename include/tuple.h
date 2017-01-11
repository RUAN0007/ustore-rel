/* 

   @Author: RUAN0007
   @Date:   2017-01-06 12:24:20
   @Last_Modified_At:   2017-01-11 09:44:21
   @Last_Modified_By:   RUAN0007

*/

#ifndef INCLUDE_TUPLE_H
#define INCLUDE_TUPLE_H

#include "type.h"

#include <vector>
#include <string> 
#include <cstring> 

#include "predicate.h"
namespace ustore{

namespace relation{

//TupleDscp is a class to describe a tuple schema.
class TupleDscp
{
public:
	TupleDscp();
	~TupleDscp();

	inline std::string GetSchemaName() const {return this->schema_name_;}
	//return "" if index > field_count
	std::string GetFieldName(unsigned index) const;

	//return 0 if index > field_count
	const Type* GetFieldType(unsigned index) const;

	//return -1 if index > field_count
	int GetFieldOffset(unsigned index) const;

	//Convert field name to field index
	// -1 if name not found.
	int FieldNameToIndex(std::string field_name) const;

	inline size_t GetSize() const{return tuple_size_;};

	inline size_t GetFieldNumber() const{ return field_count_;};

	inline size_t GetPKIndex() const{return pk_index_;}

	inline size_t GetTupleSize() const{ return tuple_size_;}

	TupleDscp(std::string schema_name, std::vector<const Type*> types, std::vector<std::string> field_names);

	TupleDscp(std::string schema_name, unsigned pk_index, std::vector<const Type*> types, std::vector<std::string>  field_names);

	friend bool operator== (const TupleDscp &lhs, const TupleDscp &rhs);
    friend bool operator!= (const TupleDscp &lhs, const TupleDscp &rhs);


private:
	// compute tuple_size_, field_offset once inner_schema_ is set up. 
	void SetUpMetaData();

	//A list of Field Type and Field Name
	std::vector<std::pair<const Type*, std::string>> inner_schema_;

	size_t tuple_size_;

	size_t field_count_;

	std::string schema_name_;
	//A vector of starting byte position for each field
	std::vector<unsigned int> field_offset_;

	//The index of primary key, default to be 1
	unsigned pk_index_;
};

class Tuple
{
public:
	~Tuple(){};

	Tuple(unsigned char* back_store, unsigned start_position, const TupleDscp* schema);
	//Retrieve the primary key of this tuple
	Field* GetPK() const;
		
	//Retrieve the tuple's field by field index
	//	return 0 if index is not valid, e.g, out of bounds
	Field* GetFieldByIndex(unsigned index) const;

	//Retrieve the tuple's field by field name
	//	return 0 if index is not valid, e.g, field name not found
	Field* GetFieldByName(std::string field_name) const;

	//Set A tuple's field by a new field by name
	// msg will contain the message of this operation, e.g, failure reason or succeeded.
	bool SetFieldByName(std::string field_name, Field* new_field, std::string* msg);

	//Set A tuple's field by a new field by index
	// msg will contain the message of this operation, e.g, failure reason or succeeded.
	bool SetFieldByIndex(unsigned index, Field* new_field, std::string* msg);

	inline const TupleDscp* GetSchema() const{return schema_;}

	inline const unsigned char* GetRawBytes(size_t& tuple_size) const {
		tuple_size = this->schema_->GetTupleSize();
		return back_store_ + start_position_;
	}

	inline bool IsSatisfy(const Predicate& predicate) const{
		std::string field_name = predicate.GetFieldName();

		Field* field = GetFieldByName(field_name);

		if (field == 0) return false;

		bool isTrue = field->IsSatisified(predicate.GetOp(), predicate.GetField());
		delete field;

		return isTrue;
	}
	
	inline friend bool operator== (const Tuple &lhs, const Tuple &rhs){

		bool IsSameSchema = (*lhs.schema_ == *rhs.schema_);

		if (!IsSameSchema) return false;
		size_t tuple_size = lhs.GetSchema()->GetTupleSize();
		//compare the contents of char array
		return memcmp(lhs.GetRawBytes(tuple_size), rhs.GetRawBytes(tuple_size), tuple_size) == 0;

	}

    inline friend bool operator!= (const Tuple &lhs, const Tuple &rhs){
    	return !(lhs == rhs);
    }

    class Iterator{
    public:
    	inline static Tuple::Iterator GetEmptyIterator(){
    		return Tuple::Iterator();
    	};
    };

private:
	unsigned char* back_store_;
	unsigned start_position_;
	const TupleDscp* schema_;

};
}	
}

#endif