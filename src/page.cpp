/* 

   @Author: RUAN0007
   @Date:   2017-01-07 13:21:13
   @Last_Modified_At:   2017-01-10 19:56:03
   @Last_Modified_By:   RUAN0007

*/



#include "page.h"

#include <cstring> /*memcpy()*/

#include "debug.h"

using namespace std;

namespace ustore{
namespace relation{

Page::Page(string table_name, const TupleDscp* tuple_schema, size_t page_size):
		table_name_(table_name),
		tuple_schema_(tuple_schema),
		page_size_(page_size),
		tuple_num_(0),
		empty_slot_offset_(sizeof(int)){

	this->buffer_ = new unsigned char[page_size]{0};
}

Page::Page(string table_name, const TupleDscp* tuple_schema, unsigned char* buffer, size_t page_size):
		table_name_(table_name),
		tuple_schema_(tuple_schema),
		buffer_(buffer),
		page_size_(page_size){


	this->tuple_num_ = *reinterpret_cast<int*>(buffer);
	this->empty_slot_offset_ = sizeof(int) + this->tuple_num_ * this->tuple_schema_->GetTupleSize();
}

void Page::SetData(unsigned char* buffer, size_t page_size){
	this->buffer_ = buffer;
	this->page_size_ = page_size;
	this->tuple_num_ = *reinterpret_cast<int*>(buffer);
	this->empty_slot_offset_ = sizeof(int) + this->tuple_num_ * this->tuple_schema_->GetTupleSize();

} 

Page::~Page() { delete[] this->buffer_;}

void Page::Reset() {
	
	delete[] this->buffer_;
	this->buffer_ = new unsigned char[page_size_]{0};

	empty_slot_offset_ = sizeof(int);	
}

Tuple* Page::GetTuple(unsigned index) const {
	if (index >= this->tuple_num_) return 0;

	size_t tuple_size = this->tuple_schema_->GetTupleSize();

	unsigned start_pos = sizeof(int) + index * tuple_size;

	return new Tuple(this->buffer_, start_pos, this->tuple_schema_);
}


int Page::InsertTuple(const Tuple* tuple, string* msg) {

	size_t tuple_size = this->tuple_schema_->GetTupleSize();

	if (this->empty_slot_offset_ + tuple_size > page_size_){
		if (msg != 0)  *msg = "Not Enough Room in this page. ";
		return -1;
	}

	if(this->tuple_schema_ != tuple->GetSchema()){

		if (msg != 0) *msg = "Incompatible Tuple Type. ";
		return -1;
	}


	const unsigned char* tuple_bytes = tuple->GetRawBytes(tuple_size);

	memcpy(this->buffer_ + empty_slot_offset_, tuple_bytes, tuple_size);

	this->empty_slot_offset_ += tuple_size;
	++this->tuple_num_;

	return this->tuple_num_ - 1;
}
}
}