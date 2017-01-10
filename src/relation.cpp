/* 

   @Author: RUAN0007
   @Date:   2017-01-10 09:16:56
   @Last_Modified_At:   2017-01-10 16:54:13
   @Last_Modified_By:   RUAN0007

*/



#include "relation.h"

#include "debug.h"
#include "version.h"
#include "types.h"

#include <algorithm>    // std::find_if
#include <iterator>     // std::distance

using namespace std;
using namespace boost;

namespace ustore{
namespace relation{	

UstoreHeapStorage::UstoreHeapStorage(const string& relation_name, const TupleDscp& schema, ClientService* client):
	relation_name_(relation_name), schema_(schema), client_(client) {
		ustore::relation::Commit base_commit;

		base_commit.id = NULL_VERSION;
		base_commit.start_version = NULL_VERSION;
		base_commit.end_version = NULL_VERSION;

		CommitID base_commit_id = NULL_VERSION;

		this->commit_info_[base_commit_id] = base_commit;

		ustore::relation::Branch master_branch;

		string master_branch_name = "master";
		master_branch.branch_name = master_branch_name;
		master_branch.base_branch= master_branch_name; //the base of master branch is itself
		master_branch.new_commit(base_commit_id);

		this->branches_info_[master_branch_name] = master_branch;
	}

UstoreHeapStorage::~UstoreHeapStorage() {
	//free all hold fields

	for(Field* pk: this->pks_) {
		delete pk;
	}
}

// void UstoreHeapStorage::SetReadBuffer(vector<Page*> read_pages) {
// 	this->read_pages_ = read_pages;
// }

void UstoreHeapStorage::SetCommitBuffer(Page* page) {
	page->Reset();
	this->commited_buffer = page;
}

vector<string> UstoreHeapStorage::GetAllBranchName() const {
	vector<string> branch_names;

	for(const auto& branch_info: branches_info_) {
		branch_names.push_back(branch_info.first);
	}

	return branch_names;
}

ustore::relation::Branch UstoreHeapStorage::GetCurrentBranchInfo() const {
	auto current_branch_info_it = this->branches_info_.find(this->current_branch_name_);
	return current_branch_info_it->second;
}

// bool UstoreHeapStorage::CheckGetBranchInfo(ustore::relation::Branch branch_record, const string& branch_name, string& msg) const{

// 	auto branch_info_it = this->branches_info_.find(branch_name);

// 	if(branch_info_it == this->branches_info_.end()) {
// 		msg = "Base Branch " + branch_name + " does not exist";
// 		return false;
// 	}else{
// 		branch_record.branch_name = branch_info_it->branch_name;
// 		return true;
// 	}

// }

bool UstoreHeapStorage::Branch(const std::string& base_branch_name, const std::string& new_branch_name, std::string& msg) {

	auto base_branch_info_it = this->branches_info_.find(base_branch_name); 
	if(base_branch_info_it == this->branches_info_.end()) {
		msg = "Base Branch " + base_branch_name + " does not exist";
		return false;
	}

	CommitID base_branch_last_commitID = base_branch_info_it->second.commit_ids.back();

	return this->Checkout(base_branch_last_commitID, 
						  base_branch_name, 
						  new_branch_name,
						  msg);

}

Tuple::Iterator UstoreHeapStorage::Scan(const std::string& branch_name, std::string& msg){

//Check for branch existence. 
	auto branch_info_it = this->branches_info_.find(branch_name); 
	if(branch_info_it == this->branches_info_.end()) {
		msg = "Base Branch " + branch_name + " does not exist";
		return Tuple::Iterator::GetEmptyIterator();
	}

	CommitID base_branch_last_commitID = branch_info_it->second.commit_ids.back();
	ustore::relation::Commit commit_record = this->commit_info_[base_branch_last_commitID];

	vector<RecordID> record_ids = this->GetTupleRecords(commit_record.tuple_presence, commit_record.tuple_positions);

	return this->ConstructTupleIterator(record_ids, 0);//0 for empty predicate
}

Tuple::Iterator UstoreHeapStorage::Diff(const std::string& branch_name1, const std::string branch_name2, std::string& msg){

//Check for branch existence. 
	auto branch_info_it = this->branches_info_.find(branch_name1); 
	if(branch_info_it == this->branches_info_.end()) {
		msg = "Base Branch " + branch_name1 + " does not exist";
		return Tuple::Iterator::GetEmptyIterator();
	}

	CommitID branch1_last_commitID = branch_info_it->second.commit_ids.back();
	ustore::relation::Commit branch1_commit = this->commit_info_[branch1_last_commitID];

	branch_info_it = this->branches_info_.find(branch_name2); 
	if(branch_info_it == this->branches_info_.end()) {
		msg = "Base Branch " + branch_name2 + " does not exist";
		return Tuple::Iterator::GetEmptyIterator();
	}

	CommitID branch2_last_commitID = branch_info_it->second.commit_ids.back();
	ustore::relation::Commit branch2_commit = this->commit_info_[branch2_last_commitID];

//Retrieve the bitmap for the last commit of two branches and perform differencing
	dynamic_bitset<> tuple_presence = branch1_commit.tuple_presence - branch2_commit.tuple_presence;

//Retrieve the tuple record for branch1
	map<unsigned, RecordID> branch1_tuple_pos = branch1_commit.tuple_positions;

	vector<RecordID> record_ids = this->GetTupleRecords(tuple_presence, branch1_tuple_pos);

	return this->ConstructTupleIterator(record_ids, 0);//0 for empty predicate
}

Tuple::Iterator UstoreHeapStorage::Join(const std::string& branch_name1, const std::string branch_name2, const Predicate* condition, std::string& msg){

//Check for branch existence. 
	auto branch_info_it = this->branches_info_.find(branch_name1); 
	if(branch_info_it == this->branches_info_.end()) {
		msg = "Base Branch " + branch_name1 + " does not exist";
		return Tuple::Iterator::GetEmptyIterator();
	}

	CommitID branch1_last_commitID = branch_info_it->second.commit_ids.back();
	ustore::relation::Commit branch1_commit = this->commit_info_[branch1_last_commitID];

	branch_info_it = this->branches_info_.find(branch_name2); 
	if(branch_info_it == this->branches_info_.end()) {
		msg = "Base Branch " + branch_name2 + " does not exist";
		return Tuple::Iterator::GetEmptyIterator();
	}

	CommitID branch2_last_commitID = branch_info_it->second.commit_ids.back();
	ustore::relation::Commit branch2_commit = this->commit_info_[branch2_last_commitID];

//Retrieve the bitmap for the last commit of two branches and perform bitwise AND
	dynamic_bitset<> tuple_presence = branch1_commit.tuple_presence & branch2_commit.tuple_presence;

//Retrieve the tuple record for branch1
	map<unsigned, RecordID> branch1_tuple_pos = branch1_commit.tuple_positions;

	vector<RecordID> record_ids = this->GetTupleRecords(tuple_presence, branch1_tuple_pos);

	return this->ConstructTupleIterator(record_ids, condition);
}


vector<RecordID> UstoreHeapStorage::GetTupleRecords(const dynamic_bitset<>& tuple_presence, std::map<unsigned, RecordID> tuple_positions) const{

	dynamic_bitset<>::size_type set_index = tuple_presence.find_first();
	vector<RecordID>  records;

	while(set_index != dynamic_bitset<>::npos) {

		auto recordID_it = tuple_positions.find(set_index);

		if(recordID_it != tuple_positions.end()){
			LOG(LOG_FATAL, " Set bit %d is not found in tuple_positions.", set_index);
		}
		records.push_back(recordID_it->second);

		set_index = tuple_presence.find_next(set_index);
	}

	return records;
}

int UstoreHeapStorage::pk2index(const Field* f) const{
	auto pk_it = find_if(pks_.begin(), pks_.end(), [f](const Field* pk_ptr){  
							return pk_ptr->equal(f);
					});

	if(pk_it == pks_.end()) return -1;

	return distance(pks_.begin(), pk_it);
}

Tuple* UstoreHeapStorage::GetTuple(const string &branch_name, const Field* pk, Page* page, string &msg){

	auto branch_info_it = this->branches_info_.find(branch_name); 
	if(branch_info_it == this->branches_info_.end()) {
		msg = "Branch " + branch_name + " does not exist";
		return 0;
	}

	int bit_pos = this->pk2index(pk);

	if(bit_pos == -1) {
		msg = "Tuple with " + pk->to_str() + " has not been inserted in any of a branch. ";
		return 0;
	}


	CommitID branch_commit_id = branch_info_it->second.commit_ids.back();

	ustore::relation::Commit branch_last_commit = this->commit_info_[branch_commit_id];

	dynamic_bitset<> tuple_presence = branch_last_commit.tuple_presence;

	if( bit_pos >= tuple_presence.size() || !tuple_presence.test(bit_pos)) {
		msg = "Tuple with " + pk->to_str() + " does not exist in branch " + branch_name;
		return 0;
	}

	RecordID tuple_pos = branch_last_commit.tuple_positions[bit_pos];

	//Retrieve page and construct tuple from ustore
	version_t version = tuple_pos.version;
	value_t* page_value = this->client_->SyncGet(this->relation_name_, version);
	
	unsigned page_size = page_value->length();
	unsigned char* page_data = new unsigned char[page_size];

	page_value->copy(reinterpret_cast<char*>(page_data), page_size);

	page->SetData(page_data, page_size);	
	delete page_value;

	return page->GetTuple(tuple_pos.tuple_index);
}

bool UstoreHeapStorage::InsertTuple(const Tuple* tuple, std::string& msg){
	Field *pk = tuple->GetPK();
	int bit_pos = this->pk2index(pk);

	if(bit_pos == -1) {
		//This tuple has not been inserted in any of a branch. 
		bit_pos = this->pks_.size();
		this->pks_.push_back(pk);
	}

	CommitID branch_commit_id = GetCurrentBranchInfo().commit_ids.back();
	ustore::relation::Commit branch_last_commit = this->commit_info_[branch_commit_id];

	dynamic_bitset<> tuple_presence = branch_last_commit.tuple_presence;

	if( bit_pos < tuple_presence.size() && tuple_presence.test(bit_pos)) {
		msg = "Tuple with " + pk->to_str() + " already exists in current branch " + current_branch_name_ + ".";
		return false;
	}

	if(this->modified_tuple_info_.find(bit_pos) != this->modified_tuple_info_.end()) {
		msg = "Tuple with " + pk->to_str() + " has been inserted after the last commit.";
		return false;
	}

	int page_index = this->commited_buffer_->InsertTuple(tuple, msg);

	if (page_index == -1) {
		//Insertion fails. 
		return false;
	}

	this->modified_tuple_info_[bit_pos] = page_index;
	return true;
}

bool UstoreHeapStorage::UpdateTuple(const Tuple* tuple, std::string& msg){
	Field *pk = tuple->GetPK();
	int bit_pos = this->pk2index(pk);

	if(bit_pos == -1) {
		//This tuple has not been inserted in any of a branch. 
		msg = "Tuple with " + pk->to_str() + " has not been inserted in any of a branch. ";
		return false;

	}

	CommitID branch_commit_id = GetCurrentBranchInfo().commit_ids.back();
	ustore::relation::Commit branch_last_commit = this->commit_info_[branch_commit_id];

	dynamic_bitset<> tuple_presence = branch_last_commit.tuple_presence;


	bool existInPreCommit = (bit_pos < tuple_presence.size() && tuple_presence.test(bit_pos));

	bool existInCommit = (this->modified_tuple_info_.find(bit_pos) != this->modified_tuple_info_.end());

	if(!existInPreCommit && !existInCommit) {
		msg = "Tuple with " + pk->to_str() + " has not been inserted in branch " + current_branch_name_ ;
		return false;
	}

	int page_index = this->commited_buffer_->InsertTuple(tuple, msg);

	if (page_index == -1) {
		//Insertion to page fails. 
		return false;
	}

	this->modified_tuple_info_[bit_pos] = page_index;
	return true;
}

bool UstoreHeapStorage::RemoveTuple(const Field* pk, std::string& msg){
	int bit_pos = this->pk2index(pk);

	if(bit_pos == -1) {
		//This tuple has not been inserted in any of a branch. 
		msg = "Tuple with " + pk->to_str() + " has not been inserted in any of a branch. ";
		return false;
	}

	CommitID branch_commit_id = GetCurrentBranchInfo().commit_ids.back();
	ustore::relation::Commit branch_last_commit = this->commit_info_[branch_commit_id];

	dynamic_bitset<> tuple_presence = branch_last_commit.tuple_presence;


	bool existInPreCommit = (bit_pos < tuple_presence.size() && tuple_presence.test(bit_pos));

	bool existInCommit = (this->modified_tuple_info_.find(bit_pos) != this->modified_tuple_info_.end());

	if(!existInPreCommit && !existInCommit) {
		msg = "Tuple with " + pk->to_str() + " has not been inserted in branch " + current_branch_name_ ;
		return false;
	}

	this->modified_tuple_info_.erase(bit_pos);
	return true;
}
}
}