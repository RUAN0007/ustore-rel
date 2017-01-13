/* 

   @Author: RUAN0007
   @Date:   2017-01-09 19:25:14
   @Last_Modified_At:   2017-01-13 14:34:13
   @Last_Modified_By:   RUAN0007

*/


#ifndef INCLUDE_RELATION_H
#define INCLUDE_RELATION_H


#include "client.h"
#include "type.h"
#include "field.h"
#include "tuple.h"
#include "page.h"
#include "predicate.h"
#include "version.h"

#include <vector>
#include <map>
#include <set>

#include <boost/dynamic_bitset.hpp>

namespace ustore{
namespace relation{

class UstoreHeapStorage{

public:
	UstoreHeapStorage(const std::string& relation_name, const TupleDscp& schema, ClientService *client, Page* commited_page);

	~UstoreHeapStorage();

/*
	Get tuple by primary key in a branch. 
	Shall be deleted after use.

	Args:
		branch_name: the name of branch where the tuple is retrieved.
		pk: the primary key of this tuple
		page: a pointer to a page to hold the tuple
		msg: a string reference that hold any returned message. 
	
	Return:
		the tuple with data as back store. Empty pointer if such tuple not exists. 
*/
	Tuple* GetTuple(const std::string& branch_name, const Field* pk, Page* page, std::string* msg);

/*
	Insert Tuple into current branch for later commit

	Args:
		tuple: the pointer to tuple to be inserted. 
		msg: a string reference that hold any returned message. 
	
	Return:
		whether the operation successful or not
*/
	bool InsertTuple(const Tuple* tuple, std::string* msg);

/*
	Update Tuple into current branch for later commit

	Args:
		tuple: the pointer to tuple to be updated. 
		msg: a string reference that hold any returned message. 
	
	Return:
		whether the operation successful or not
*/
	bool UpdateTuple(const Tuple* tuple, std::string* msg);
/*
	Remove Tuple for primary key into current branch for later commit

	Args:
		tuple: the pointer to tuple to be updated. 
		msg: a string reference that hold any returned message. 
	
	Return:
		whether the operation successful or not
*/
	bool RemoveTuple(const Field* pk, std::string* msg);

/*
	Commit the uncommited operations, e.g Insert, Update & Remove Tuple and create the snapshot

	Args:
		commit_id: the pointer to hold commitID for this commit
		msg: a string reference that hold any returned message. 
	
	Return:
		whether the commit operation successful or not
*/
	bool Commit(CommitID* commit_id, std::string* msg);

/*
	Merge branch1 and branch2 and make a commit. This commit shall fall into branch1. 
	If tuples active in both branch1 and branch2, branch1's version takes the precedence.  

	Args:
		commit_ids: the pointer to hold commitID for this commit
		branch_name1: the name of branch1
		branch_name2: the name of branch2	
		msg: a string reference that hold any returned message. 

	Return:
		whether the commit operation successful or not
*/
	bool Merge(CommitID* commit_id, const std::string& branch_name1, const std::string& branch_name2, std::string* msg);

/*
	Create a new branch rooted on the commit of another branch

	Args:
		commit_id: the ID of the commit from base branch 
		base_branch_bame: the name of base branch
		new_branch_name: the name of created branch
		msg: a pointer of a string to hold any returned message

	Return:
		whether the operation successful or not
*/
	bool Checkout(const CommitID& commit_id, const std::string& base_branch_name, const std::string& new_branch_name, std::string* msg);

/*
	Create a new branch from another branch and switch to new branch

	Args:
		base_branch_bame: the name of base branch
		new_branch_name: the name of created branch
		msg: a pointer of a string to hold any returned message

	Return:
		whether the operation successful or not
*/
	bool Branch(const std::string& base_branch_name, const std::string& new_branch_name, std::string* msg);

/*
	Switch to a existed branch

	Args:
		branch_bame: the name of the branch to be switched to
		msg: a pointer of a string to hold any returned message

	Return:
		whether the operation successful or not
*/
	bool Switch(const std::string& branch_name, std::string* msg);

/*
	Scan all the active tuples in a branch

	Args:
		branch_name: the name of branch for scanning
		msg: a pointer of a string to hold any returned message

	Return:
		the tuple iterator to hold valid tuples. Valid tuple are stored in Read Buffer. 
*/
	Tuple::Iterator* Scan(const std::string& branch_name, std::string* msg);

/*
	Scan all the active tuples in a branch1 BUT not in branch2

	Args:
		branch_name1: the name of branch1
		branch_name2: the name of branch2
		msg: a pointer of a string to hold any returned message

	Return:
		the tuple iterator to hold valid tuples. Valid tuples are stored in Read Buffer. 

*/
	Tuple::Iterator* Diff(const std::string& branch_name1, const std::string branch_name2, std::string* msg);

/*
	Scan all the active tuples in both branch1 and branch2 while in branch1 satisfying the predicate . 

	Args:
		branch_name1: the name of branch1
		branch_name2: the name of branch2
		condition: the predicate to filter for valid tuples in branch1
		msg: a pointer of a string to hold any returned message

	Return:
		the tuple iterator to hold valid tuples. Valid tuples are stored in Read Buffer. 

*/
	Tuple::Iterator* Join(const std::string& branch_name1, const std::string branch_name2, const Predicate* condition, std::string* msg);

/*
	Set the pages for read buffer

	Args:
		a vector of pages for read buffer
*/
	void SetReadBuffer(Page* read_page);

/*
	Set the pages for write buffer

	Args:
		a vector of pages for write buffer
*/
	void SetCommitBuffer(Page* page);

/*
	Get the name of all branches

	Return:
		a vector of branch name in string format
*/
	std::vector<std::string> GetAllBranchName() const;

/*
	Get current branch information

	Return:
		a Branch struct for current branch
*/
	ustore::relation::Branch GetCurrentBranchInfo() const;



private:


/*
Return a vector of Record to identify the tuple position in Ustore based on a tuple presence bitmap and branch name

Args:
	tuple_presence: a bitmap to indicate needed tuples.
	tuple_positions: a map to identify the tuple's position in ustore
		key: tuple's global bit position
		value: RecordID to identify this tuple's position in ustore

Return:
	a vector of RecordID. 
	An empty vector will return if any error occurs, e,g branch name not found. 

*/

std::vector<RecordID> GetTupleRecords(const boost::dynamic_bitset<>& tuple_presence, std::map<unsigned, RecordID> tuple_positions) const;

/*
Construct the tuple iterator based on tuple's position and predicate. The valid tuples are stored in Read Buffer

Args:
	tuple_pos_; a vector of RecordID to identify the candidate tuple's position
	predicate: the predicate to filter for valid tuples. An empty pointer indicates no predicate conditio applies. 

Returns:
	A tuple iterator
*/
	// Tuple::Iterator* ConstructTupleIterator(const std::vector<RecordID>& tuples_pos_,const Predicate* predicate);

	//convert primary key field into bit position
	//-1 if not found
	int pk2index(const Field* f) const;

/*
Check whether a tuple of bit pos is active in previous commit`

Args:
	the bit position for that tuple

Return:
	whether the tuple is active in previous commit
*/
	bool ExistInPreCommit(unsigned bit_pos) const;

/*
Check whether a tuple of bit pos is active in current state as well as comit

Args:
	the bit position for that tuple

Return:
	whether the tuple is active now. 
*/
	bool IsActiveTuple(unsigned bit_pos) const;

/*
Check whether the workspace is empty, aka no uncommited operation

Return:
	true if no uncommited opearation
*/
	bool IsEmptyWorkSpace() const;

/*
Check a branch existence and get branch info based on the branch name.

Args:
	branch_record: a Branch reference to hold the branch record
	branch_name: the name of the branch to look for
	msg: a string reference to return any NotFound Message

Return:
	whethe the branch exists

*/
	// bool CheckGetBranchInfo(ustore::relation::Branch branch_record, const std::string& branch_name, std::string* msg) const;

	UstoreHeapStorage& operator=(const UstoreHeapStorage&) = delete; //Prevent assignment and copy
    UstoreHeapStorage(const UstoreHeapStorage&) = delete;



//About branch info
	//key: branch name
	//value: Branch struct
	std::map<std::string, ustore::relation::Branch> branches_info_; 
	std::string current_branch_name_;

//About Commit Info
	std::map<CommitID, ustore::relation::Commit> commit_info_;

//Read Buffer Pages
	Page* read_page_; 

//Write Buffer Pages
	// std::vector<Page*> write_pages_;
	//Buffer to hold modified tuple in the next commit
	Page *commited_buffer_; 
	// unsigned available_write_page_index_; //the index of write page that has empty slots

//Information for uncommited tuple operation

	//key: global bit position of modified tuples  
	//value: tuple index in the commited page
	std::map<unsigned, unsigned> modified_tuple_info_;	

	//the global bit position of the removed tuples in the next commit
	std::set<unsigned> removed_tuple_pos_;

	//the global bit position of the inserted/updated tuples in the next commit
	// std::set<unsigned> modified_tuple_pos_;

//Mapping info between tuple's primary key to global bit position

//There is one-to-one mapping between primary key field to global bit postion. The tuple's global bit position is determined by its primary key. 

	//primary key to global bit position
	// std::map<Field*, unsigned> pk2pos_;

	//a vector of primary key occurs in any of branch
	std::vector<Field*> pks_;

	std::string relation_name_;

	TupleDscp schema_;

	ClientService* client_;

};

}// namespace relation
}// namespace ustore
#endif