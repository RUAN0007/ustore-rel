/* 

   @Author: RUAN0007
   @Date:   2017-01-06 13:12:52
   @Last_Modified_At:   2017-01-13 15:30:57
   @Last_Modified_By:   RUAN0007

*/

#include "tuple.h"

#include <string>
#include <cstring>

#include "debug.h"
#include "field.h"
#include "page.h"

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


TupleDscp::TupleDscp(std::string schema_name, vector<const Type*> types, vector<string> field_names):
			TupleDscp::TupleDscp(schema_name, 0, types, field_names) {

}


TupleDscp::TupleDscp(std::string schema_name, unsigned pk_index, vector<const Type*> types, vector<string> field_names):
		schema_name_(schema_name),
		pk_index_(pk_index)
		{

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

bool operator== (const TupleDscp &lhs, const TupleDscp &rhs){
	bool isSameName =  lhs.schema_name_ == rhs.schema_name_;	

	if(!isSameName) return false;

	bool IsSameLen = lhs.GetFieldNumber() == rhs.GetFieldNumber();

	if(!IsSameLen) return false;

	bool isSameStructure = true;

	auto lhs_field_item_it = lhs.inner_schema_.begin();
	auto rhs_field_item_it = rhs.inner_schema_.begin();

	while(lhs_field_item_it != lhs.inner_schema_.end() || rhs_field_item_it != rhs.inner_schema_.end()) {

		
		if(lhs_field_item_it->first != rhs_field_item_it->first ){
			isSameStructure = false;
			break;
		}	

		if(lhs_field_item_it->second != rhs_field_item_it->second ){
			isSameStructure = false;
			break;
		}	
		++lhs_field_item_it;
		++rhs_field_item_it;
	}

	return isSameStructure;
}
bool operator!= (const TupleDscp &lhs, const TupleDscp &rhs){
	return !operator==(lhs, rhs);
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

	unsigned offset = start_position_ + field_position;
	return field_type->Parse(this->back_store_ + offset);
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
		if (msg != 0) *msg = "Incompatible Field Type";
		return false;
	}

	const Type* type = original_field_type;

	size_t len = type->GetLen();

	const unsigned char* new_field_data = type->Serialize(new_field);

	unsigned field_position = this->schema_->GetFieldOffset(index);

	unsigned offset = start_position_ + field_position;
	memcpy(back_store_ + offset, new_field_data, len);
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

Tuple::Iterator* Tuple::Iterator::GetEmptyIterator() {
	return new Tuple::Iterator::EmptyIterator();
}

PageIterator::PageIterator(const string& relation_name, ClientService* client, Page* stored_page, const std::vector<RecordID>& tuple_pos, const Predicate* predicate):
		relation_name_(relation_name),
		client_(client),
		stored_page_(stored_page),
		predicate_(predicate){


	this->ReorganizeTuplePos(tuple_pos);
	Reset();
}

PageIterator::PageIterator(const string& relation_name, ClientService* client, Page* stored_page, const std::vector<RecordID>& tuple_pos):
		PageIterator(relation_name, client, stored_page, tuple_pos, 0){
}

const Tuple* PageIterator::GetTuple() {
	if (End()) return 0;


	const Tuple* tuple = stored_page_->GetTuple(*curr_tuple_index_);

	while(!tuple->IsSatisfy(predicate_) && Next()) {
		tuple = stored_page_->GetTuple(*curr_tuple_index_);
	}

	if(End()){
		return 0;
	}else{
		return tuple;
	}
}

bool PageIterator::Next() {
	if(End()) return false;

	++curr_tuple_index_;

	if(curr_tuple_index_ == curr_page_->second.end()) {
		//pointer reaches the end of this page

		//Advance the page iterator
		++curr_page_;
		if(curr_page_ != tuple_pages_.end()) { 
			//If not the end of page, load the next page
			LoadPage(); 
		}else{
			//already the end
			return false;
		}
	}

	return true;
}

bool PageIterator::End() const {
	return curr_page_ == tuple_pages_.end();
}

void PageIterator::Reset() {
	this->curr_page_ = this->tuple_pages_.begin();
	LoadPage();
}

void PageIterator::ReorganizeTuplePos(const vector<RecordID>& tuple_pos) {
	for(const auto& tpos:tuple_pos) {
		tuple_pages_[tpos.version].push_back(tpos.tuple_index);
	}
}

void PageIterator::LoadPage() {
	version_t version = curr_page_->first;

	value_t* page_value = this->client_->SyncGet(this->relation_name_, version);
	unsigned page_size = page_value->length();
	
	unsigned char* page_data = new unsigned char[page_size];

	page_value->copy(reinterpret_cast<char*>(page_data), page_size);

	stored_page_->SetData(page_data, page_size);	
	// cout << "Page size: " << page->GetTupleNumber() << endl; 
	delete page_value;

	curr_tuple_index_ = curr_page_->second.begin();

}

PageIterator::~PageIterator(){}

}//namespace ustore
}//namepace relation

