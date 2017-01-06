/* 

   @Author: RUAN0007
   @Date:   2017-01-06 13:12:52
   @Last_Modified_At:   2017-01-06 20:20:29
   @Last_Modified_By:   RUAN0007

*/

#include "tuple.h"

#include <string>
#include <cstring>

#include "debug.h"
#include "field.h"

using namespace std;

namespace ustore{
namespace relation{

TupleDscp::~TupleDscp(){};

string TupleDscp::GetFieldName(unsigned index) const {

	if(index >= field_count_) return "";

	auto fieldPair = this->inner_schema_.at(index);
	return fieldPair.second;
}

const Type* TupleDscp::GetFieldType(unsigned int index) const {
	if (index >= field_count_) return 0;

	auto fieldPair = this->inner_schema_.at(index);
	return fieldPair.first;
}

int TupleDscp::GetFieldOffset(unsigned index) const {

	if (index >= field_count_) return -1;

	return this->field_offset_.at(index);
}

int TupleDscp::FieldNameToIndex(string field_name) const {

	int i = 0;
	for(auto field_item : this->inner_schema_) {

		if(field_name == field_item.second) return i;

		++i;
	}

	return -1;
}


TupleDscp::TupleDscp(vector<const Type*> types, vector<string> field_names):
			TupleDscp::TupleDscp(0, types, field_names) {

}


TupleDscp::TupleDscp(unsigned pk_index, vector<const Type*> types, vector<string> field_names)
		:pk_index_(pk_index){

	auto type_it = types.begin();
	auto name_it = field_names.begin();

	while(type_it != types.end() || name_it != field_names.end()) {
		this->inner_schema_.push_back(make_pair(*type_it, *name_it));

		++ type_it;
		++name_it;

	}//End of while

	this->SetUpMetaData();
}

void TupleDscp::SetUpMetaData() {

	this->field_count_ = this->inner_schema_.size();

	size_t total_size = 0;

	for(auto field_item : this->inner_schema_) {

		this->field_offset_.push_back(total_size);

		const Type* type = field_item.first;
		total_size += type->GetLen();
	}
	this->tuple_size_ = total_size;
}


Tuple::Tuple(unsigned char* back_store, unsigned start_position, const TupleDscp* schema):
		back_store_(back_store),
		start_position_(start_position),
		schema_(schema) {
}


Field* Tuple::GetPK() const {
	unsigned pk_position = this->schema_->GetPKIndex();

	return this->GetFieldByIndex(pk_position);
}

Field* Tuple::GetFieldByIndex(unsigned index) const{

	int field_position = this->schema_->GetFieldOffset(index);

	if(field_position == -1) return 0;

	const Type* field_type = this->schema_->GetFieldType(index);

	return field_type->Parse(this->back_store_ + field_position);
}

Field* Tuple::GetFieldByName(string field_name) const {
	int field_index = this->schema_->FieldNameToIndex(field_name);

	if (field_index == -1) return 0;

	return this->GetFieldByIndex(field_index);
}

bool Tuple::SetFieldByIndex(unsigned index, Field *new_field, string* msg){

	const Type* original_field_type = this->schema_->GetFieldType(index);
	const Type* new_field_type = new_field->GetType();

	if(original_field_type != new_field_type) {
		if(msg != 0) *msg = "Incompatible Field Type";
		return false;
	}

	const Type* type = original_field_type;

	size_t len = type->GetLen();

	const unsigned char* new_field_data = type->Serialize(new_field);

	unsigned field_position = this->schema_->GetFieldOffset(index);

	memcpy(back_store_ + field_position, new_field_data, len);
	return true;

}

bool Tuple::SetFieldByName(string field_name, Field* new_field, std::string* msg){
	int field_position = this->schema_->FieldNameToIndex(field_name);

	if(field_position == -1) {
		if (msg != 0) *msg = "Can not field for name " + field_name;
		return false;
	}

	return SetFieldByIndex(field_position, new_field, msg);
}
}//namespace ustore
}//namepace relation