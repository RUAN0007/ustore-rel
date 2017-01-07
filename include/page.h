/* 

   @Author: RUAN0007
   @Date:   2017-01-07 09:48:57
   @Last_Modified_At:   2017-01-07 15:18:05
   @Last_Modified_By:   RUAN0007

*/


#ifndef INCLUDE_PAGE_H
#define INCLUDE_PAGE_H

#include "tuple.h"

namespace ustore{
namespace relation{

/*
Page contains a chunk of bytes that hold tuples. The first four bytes hold the number of tuples in this page. 
*/
class Page
{

public:
	Page(std::string table_name_, const TupleDscp* tuple_schema, size_t page_size);
	~Page();

	inline std::string GetTableName() const{ return table_name_;}

	inline const TupleDscp* GetTupleSchema() const{return tuple_schema_;}


	inline size_t GetPageSize() const {return page_size_;}

	inline unsigned GetTupleNumber() const{ return tuple_num_;}

	void Reset();

	//return 0 if index > tuple_num_
	Tuple* GetTuple(unsigned index) const;

	//Insert the tuple inside this page
	//Return the tuple position in this page

	//Return -1 if insertion fails. 
	int InsertTuple(Tuple* tuple, std::string& msg);

private:
	std::string table_name_;
	const TupleDscp* tuple_schema_; 

	unsigned empty_slot_offset_;
	unsigned tuple_num_;
	size_t page_size_;

	unsigned char* buffer_;
	
};
}
}
#endif