/* 

   @Author: RUAN0007
   @Date:   2017-01-06 12:24:20
   @Last_Modified_At:   2017-01-13 14:03:20
   @Last_Modified_By:   RUAN0007

*/

#ifndef INCLUDE_TUPLE_H
#define INCLUDE_TUPLE_H

#include "type.h"

#include <vector>
#include <map>
#include <string> 
#include <cstring> 

#include "predicate.h"
#include "page.h"
#include "version.h"
#include "client.h"

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

	inline bool IsSatisfy(const Predicate* predicate) const{
		if(predicate == 0) return true;

		std::string field_name = predicate->GetFieldName();

		Field* field = GetFieldByName(field_name);

		if (field == 0) return false;

		bool isTrue = field->IsSatisified(predicate->GetOp(), predicate->GetField());
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

		//Get tuple pointed by current iterator
		//return 0 if End()
		virtual const Tuple* GetTuple() = 0;

		//Advance the iterator pointer
		//Return false if it is already the end or advance to reach the end
		virtual bool Next() = 0;

		//Return whether the iterator reaches the end
		virtual bool End() const = 0;

		static Iterator* GetEmptyIterator();

	private:
		class EmptyIterator;
};


private:
	unsigned char* back_store_;
	unsigned start_position_;
	const TupleDscp* schema_;

};

class Tuple::Iterator::EmptyIterator : public Tuple::Iterator {

	inline const Tuple* GetTuple()  override {return 0;}

	inline bool Next() override {return false;}

	inline bool End() const override {return true;}
};

class PageIterator: public Tuple::Iterator {
public:
	PageIterator(const std::string& relation_name, ClientService* client, Page* stored_page, 
		const std::vector<RecordID>& tuple_pos, const Predicate* predicate);

	PageIterator(const std::string& relation_name, ClientService* client, Page* stored_page, 
		const std::vector<RecordID>& tuple_pos);

	~PageIterator();

	const Tuple* GetTuple() override;

	bool Next() override;

	bool End() const override;

private:

	//Given a vector of RecordID, group the RecordID with same ustore version together
	//Set the tuple_pos
	void ReorganizeTuplePos(const std::vector<RecordID>& tuple_pos);

	//Load the page whose position is pointed by curr_page_
	void LoadPage();

	std::string relation_name_;
	ClientService* client_;
	Page* stored_page_;
	const Predicate* predicate_;
	std::map<version_t, std::vector<unsigned>> tuple_pages_;


//The iterator pointing to the current page in stored_page
	std::map<version_t, std::vector<unsigned>>::iterator curr_page_;

//the iterator pointing to the current tuple in stored page
	std::vector<unsigned>::iterator curr_tuple_index_;

};

}	
}

#endif