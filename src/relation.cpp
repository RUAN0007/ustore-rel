/* 

   @Author: RUAN0007
   @Date:   2017-01-10 09:16:56
   @Last_Modified_At:   2017-01-13 17:01:11
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

UstoreHeapStorage::UstoreHeapStorage(const string& relation_name, const TupleDscp& schema, ClientService* client, Page* commited_page):
		commited_buffer_(commited_page),relation_name_(relation_name),schema_(schema),client_(client){
			
		CommitRecord base_commit;

		base_commit.id = NULL_VERSION;
		base_commit.ustore_version = NULL_VERSION;
		base_commit.tuple_presence = dynamic_bitset<>(0);
		base_commit.tuple_positions = map<unsigned, RecordID>();

		CommitID base_commit_id = NULL_VERSION;

		this->commit_info_[base_commit_id] = base_commit;

		BranchRecord master_branch;

		string master_branch_name = "master";
		master_branch.branch_name = master_branch_name;
		master_branch.base_branch= master_branch_name; //the base of master branch is itself
		master_branch.new_commit(base_commit_id);

		this->branches_info_[master_branch_name] = master_branch;
		this->current_branch_name_ = master_branch_name;
	}

UstoreHeapStorage::~UstoreHeapStorage() {
	//free all hold fields

	for(Field* pk: this->pks_) {
		delete pk;
	}
}

void UstoreHeapStorage::SetReadBuffer(Page* read_page) {
	read_page->Reset(this->relation_name_, &(this->schema_));
	this->read_page_ = read_page;
}

void UstoreHeapStorage::SetCommitBuffer(Page* page) {
	page->Reset();
	this->commited_buffer_ = page;
}

vector<string> UstoreHeapStorage::GetAllBranchName() const {
	vector<string> branch_names;

	for(const auto& branch_info: branches_info_) {
		branch_names.push_back(branch_info.first);
	}

	return branch_names;
}

BranchRecord UstoreHeapStorage::GetCurrentBranchInfo() const {
	auto current_branch_info_it = this->branches_info_.find(this->current_branch_name_);
	return current_branch_info_it->second;
}

// bool UstoreHeapStorage::CheckGetBranchInfo(ustore::relation::Branch branch_record, const string& branch_name, string& msg) const{

// 	auto branch_info_it = this->branches_info_.find(branch_name);

// 	if(branch_info_it == this->branches_info_.end()) {
// 		if (msg != 0) *msg = "Base Branch " + branch_name + " does not exist";
// 		return false;
// 	}else{
// 		branch_record.branch_name = branch_info_it->branch_name;
// 		return true;
// 	}

// }

bool UstoreHeapStorage::Branch(const std::string& base_branch_name, const std::string& new_branch_name, std::string* msg) {

	auto base_branch_info_it = this->branches_info_.find(base_branch_name); 
	if(base_branch_info_it == this->branches_info_.end()) {
		if (msg != 0) *msg = "Base Branch " + base_branch_name + " does not exist";
		return false;
	}

	CommitID base_branch_last_commitID = base_branch_info_it->second.commit_ids.back();

	return this->Checkout(base_branch_last_commitID, 
						  base_branch_name, 
						  new_branch_name,
						  msg);

}

Tuple::Iterator* UstoreHeapStorage::Scan(const std::string& branch_name, std::string* msg){

//Check for branch existence. 
	auto branch_info_it = this->branches_info_.find(branch_name); 
	if(branch_info_it == this->branches_info_.end()) {
		if (msg != 0) *msg = "Base Branch " + branch_name + " does not exist";
		return Tuple::Iterator::GetEmptyIterator();
	}

	CommitID base_branch_last_commitID = branch_info_it->second.commit_ids.back();
	CommitRecord commit_record = this->commit_info_[base_branch_last_commitID];

	vector<RecordID> record_ids = this->GetTupleRecords(commit_record.tuple_presence, commit_record.tuple_positions);

	return new PageIterator(this->relation_name_, this->client_, this->read_page_, record_ids); 
}

Tuple::Iterator* UstoreHeapStorage::Diff(const std::string& branch_name1, const std::string branch_name2, std::string* msg){

//Check for branch existence. 
	auto branch_info_it = this->branches_info_.find(branch_name1); 
	if(branch_info_it == this->branches_info_.end()) {
		if (msg != 0) *msg = "Base Branch " + branch_name1 + " does not exist";
		return Tuple::Iterator::GetEmptyIterator();
	}

	CommitID Branch_last_commitID = branch_info_it->second.commit_ids.back();
	CommitRecord Branch_commit = this->commit_info_[Branch_last_commitID];

	branch_info_it = this->branches_info_.find(branch_name2); 
	if(branch_info_it == this->branches_info_.end()) {
		if (msg != 0) *msg = "Base Branch " + branch_name2 + " does not exist";
		return Tuple::Iterator::GetEmptyIterator();
	}

	CommitID branch2_last_commitID = branch_info_it->second.commit_ids.back();
	CommitRecord branch2_commit = this->commit_info_[branch2_last_commitID];

//Retrieve the bitmap for the last commit of two branches and perform differencing

	//Resize to be the same length
	size_t b1_size = Branch_commit.tuple_presence.size();
	size_t b2_size = branch2_commit.tuple_presence.size();

	if(b1_size > b2_size) {
		branch2_commit.tuple_presence.resize(b1_size, 0);
	}else{
		Branch_commit.tuple_presence.resize(b2_size, 0);
	}

	dynamic_bitset<> tuple_presence = Branch_commit.tuple_presence - branch2_commit.tuple_presence;

//Retrieve the tuple record for Branch
	map<unsigned, RecordID> Branch_tuple_pos = Branch_commit.tuple_positions;

	vector<RecordID> record_ids = this->GetTupleRecords(tuple_presence, Branch_tuple_pos);

	return new PageIterator(this->relation_name_, this->client_, this->read_page_, record_ids); 
}

Tuple::Iterator* UstoreHeapStorage::Join(const std::string& branch_name1, const std::string branch_name2, const Predicate* predicate , std::string* msg){

//Check for branch existence. 
	auto branch_info_it = this->branches_info_.find(branch_name1); 
	if(branch_info_it == this->branches_info_.end()) {
		if (msg != 0) *msg = "Base Branch " + branch_name1 + " does not exist";
		return Tuple::Iterator::GetEmptyIterator();
	}

	CommitID Branch_last_commitID = branch_info_it->second.commit_ids.back();
	CommitRecord Branch_commit = this->commit_info_[Branch_last_commitID];

	branch_info_it = this->branches_info_.find(branch_name2); 
	if(branch_info_it == this->branches_info_.end()) {
		if (msg != 0) *msg = "Base Branch " + branch_name2 + " does not exist";
		return Tuple::Iterator::GetEmptyIterator();
	}

	CommitID branch2_last_commitID = branch_info_it->second.commit_ids.back();
	CommitRecord branch2_commit = this->commit_info_[branch2_last_commitID];

	//Resize to be the same length
	size_t b1_size = Branch_commit.tuple_presence.size();
	size_t b2_size = branch2_commit.tuple_presence.size();

	if(b1_size > b2_size) {
		branch2_commit.tuple_presence.resize(b1_size, 0);
	}else{
		Branch_commit.tuple_presence.resize(b2_size, 0);
	}
//Retrieve the bitmap for the last commit of two branches and perform bitwise AND
	dynamic_bitset<> tuple_presence = Branch_commit.tuple_presence & branch2_commit.tuple_presence;

//Retrieve the tuple record for Branch
	map<unsigned, RecordID> Branch_tuple_pos = Branch_commit.tuple_positions;

	vector<RecordID> record_ids = this->GetTupleRecords(tuple_presence, Branch_tuple_pos);

	return new PageIterator(this->relation_name_, this->client_, this->read_page_, record_ids,predicate); 
}


vector<RecordID> UstoreHeapStorage::GetTupleRecords(const dynamic_bitset<>& tuple_presence, std::map<unsigned, RecordID> tuple_positions) const{

	dynamic_bitset<>::size_type set_index = tuple_presence.find_first();
	vector<RecordID>  records;

	while(set_index != dynamic_bitset<>::npos) {

		auto recordID_it = tuple_positions.find(set_index);

		if(recordID_it == tuple_positions.end()){
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

Tuple* UstoreHeapStorage::GetTuple(const string &branch_name, const Field* pk, Page* page, string *msg){

	auto branch_info_it = this->branches_info_.find(branch_name); 
	if(branch_info_it == this->branches_info_.end()) {
		if (msg != 0) *msg = "Branch " + branch_name + " does not exist";
		return 0;
	}

	int bit_pos = this->pk2index(pk);

	if(bit_pos == -1) {
		if (msg != 0) *msg = "Tuple with " + pk->to_str() + " has not been inserted in any of a branch. ";
		return 0;
	}

	CommitID branch_commit_id = branch_info_it->second.commit_ids.back();

	CommitRecord branch_last_commit = this->commit_info_[branch_commit_id];

	dynamic_bitset<> tuple_presence = branch_last_commit.tuple_presence;

	if( bit_pos >= tuple_presence.size() || !tuple_presence.test(bit_pos)) {
		if (msg != 0) *msg = "Tuple with " + pk->to_str() + " does not exist in branch " + branch_name;
		return 0;
	}
	RecordID tuple_pos = branch_last_commit.tuple_positions[bit_pos];

	//Retrieve page and construct tuple from ustore
	version_t version = tuple_pos.version;	

	// cout << "Version: " << tuple_pos.version << endl;
	// cout << "Page Index: " << tuple_pos.tuple_index << endl;
	// cout << endl;

	value_t* page_value = this->client_->SyncGet(this->relation_name_, version);
	
	unsigned page_size = page_value->length();
	unsigned char* page_data = new unsigned char[page_size];

	page_value->copy(reinterpret_cast<char*>(page_data), page_size);

	page->SetData(page_data, page_size);	
	delete page_value;

	return page->GetTuple(tuple_pos.tuple_index);
}

bool UstoreHeapStorage::InsertTuple(const Tuple* tuple, std::string* msg){
	Field *pk = tuple->GetPK();
	int bit_pos = this->pk2index(pk);

	if(bit_pos == -1) {
		//This tuple has not been inserted in any of a branch. 
		bit_pos = this->pks_.size();
		this->pks_.push_back(pk);
	}

	CommitID branch_commit_id = GetCurrentBranchInfo().commit_ids.back();
	CommitRecord branch_last_commit = this->commit_info_[branch_commit_id];

	dynamic_bitset<> tuple_presence = branch_last_commit.tuple_presence;
	// cout << "Presence: " << tuple_presence << endl;

	if(IsActiveTuple(bit_pos)) {
		if (msg != 0) *msg = "Tuple with " + pk->to_str() + " has already exists in branch " + current_branch_name_;
		return false;
	}
	int page_index = this->commited_buffer_->InsertTuple(tuple, msg);

	if (page_index == -1) {
		//Insertion fails. 
		return false;
	}

	this->modified_tuple_info_[bit_pos] = page_index;
	this->removed_tuple_pos_.erase(bit_pos);

	*msg = "Update succeeded";
	return true;
}

bool UstoreHeapStorage::UpdateTuple(const Tuple* tuple, std::string* msg){
	Field *pk = tuple->GetPK();
	int bit_pos = this->pk2index(pk);

	if(bit_pos == -1) {
		//This tuple has not been inserted in any of a branch. 
		if (msg != 0) *msg = "Tuple with " + pk->to_str() + " has not been inserted in any of a branch. ";
		return false;

	}

	if(!IsActiveTuple(bit_pos)) {
		if (msg != 0) *msg = "Tuple with " + pk->to_str() + " has not been inserted in branch " + current_branch_name_ ;
		return false;
	}

	int page_index = this->commited_buffer_->InsertTuple(tuple, msg);

	if (page_index == -1) {
		//Insertion to page fails. 
		return false;
	}

	this->modified_tuple_info_[bit_pos] = page_index;
	this->removed_tuple_pos_.erase(bit_pos);

	*msg = "Update succeeded";
	return true;
}

bool UstoreHeapStorage::IsActiveTuple(unsigned bit_pos) const{


	bool exist_pre_commit = ExistInPreCommit(bit_pos);

	bool removed_in_commit = (this->removed_tuple_pos_.find(bit_pos) != this->removed_tuple_pos_.end());

	bool modified_in_commit = (this->modified_tuple_info_.find(bit_pos) != this->modified_tuple_info_.end());


	return !removed_in_commit && (modified_in_commit || exist_pre_commit);
}

bool UstoreHeapStorage::ExistInPreCommit(unsigned bit_pos) const {

	CommitID branch_commit_id = GetCurrentBranchInfo().commit_ids.back();
	CommitRecord branch_last_commit = this->commit_info_.find(branch_commit_id)->second;

	dynamic_bitset<> tuple_presence = branch_last_commit.tuple_presence;

	return  bit_pos < tuple_presence.size() && tuple_presence.test(bit_pos);
}

bool UstoreHeapStorage::RemoveTuple(const Field* pk, std::string* msg){
	int bit_pos = this->pk2index(pk);

	if(bit_pos == -1) {
		//This tuple has not been inserted in any of a branch. 
		if (msg != 0) *msg = "Tuple with " + pk->to_str() + " has not been inserted in any of a branch. ";
		return false;
	}

	if(!IsActiveTuple(bit_pos)) {
		if (msg != 0) *msg = "Tuple with " + pk->to_str() + " has not been inserted in branch " + current_branch_name_ ;
		return false;
	}

	this->modified_tuple_info_.erase(bit_pos);

	if(ExistInPreCommit(bit_pos)) this->removed_tuple_pos_.insert(bit_pos);
	*msg = "Remove succeeded";
	return true;
}

bool UstoreHeapStorage::Merge(CommitID* commit_id, const std::string& branch_name1, const std::string& branch_name2, std::string* msg) {

	if(commit_id == 0) {
		if (msg != 0) *msg = "Can not pass in an empty commit_id pointer";
		return false;
	}
//Check for branch existence. 
	auto branch_info_it = this->branches_info_.find(branch_name1); 
	if(branch_info_it == this->branches_info_.end()) {
		if (msg != 0) *msg = "Branch " + branch_name1 + " does not exist";
		return false;
	}

	CommitID Branch_last_commitID = branch_info_it->second.commit_ids.back();
	CommitRecord Branch_commit = this->commit_info_[Branch_last_commitID];
	map<unsigned, RecordID> Branch_tuple_pos = Branch_commit.tuple_positions;	

	branch_info_it = this->branches_info_.find(branch_name2); 
	if(branch_info_it == this->branches_info_.end()) {
		if (msg != 0) *msg = "Branch " + branch_name2 + " does not exist";
		return false;
	}

	CommitID branch2_last_commitID = branch_info_it->second.commit_ids.back();
	CommitRecord branch2_commit = this->commit_info_[branch2_last_commitID];
	map<unsigned, RecordID> branch2_tuple_pos = branch2_commit.tuple_positions;	

	//Resize to be the same length
	size_t b1_size = Branch_commit.tuple_presence.size();
	size_t b2_size = branch2_commit.tuple_presence.size();

	if(b1_size > b2_size) {
		branch2_commit.tuple_presence.resize(b1_size, 0);
	}else{
		Branch_commit.tuple_presence.resize(b2_size, 0);
	}
	//Perform the OR operation on bitmap
	dynamic_bitset<> merged_tuple_presence = Branch_commit.tuple_presence | branch2_commit.tuple_presence;

	map<unsigned, RecordID> merged_tuple_pos;

	dynamic_bitset<>::size_type set_index = merged_tuple_presence.find_first();

	//Combine RecordID from commits of both branches.
	//Commit from Branch takes the precedence. 

	while(set_index != dynamic_bitset<>::npos) {

		auto recordID_it = Branch_tuple_pos.find(set_index);

		if(recordID_it != Branch_tuple_pos.end()){
			merged_tuple_pos[set_index] = recordID_it->second;
		}else{
			recordID_it = branch2_tuple_pos.find(set_index);

			if(recordID_it != branch2_tuple_pos.end()){
				merged_tuple_pos[set_index] = recordID_it->second;
			}else{
				LOG(LOG_FATAL, "Tuple with global bit %d is not found in either branch %s or %s. ", set_index, branch_name1.c_str(), branch_name2.c_str());
			}
		}


		set_index = merged_tuple_presence.find_next(set_index);
	}

	version_t Branch_version = Branch_commit.ustore_version;
	version_t branch2_version = branch2_commit.ustore_version;
	
	version_t merge_version = this->client_->SyncMerge(this->relation_name_, Branch_version, branch2_version, NULL_VALUE);

	CommitRecord merge_commit;

	merge_commit.id = merge_version;
	merge_commit.ustore_version = merge_version;
	merge_commit.tuple_presence = merged_tuple_presence;
	merge_commit.tuple_positions = merged_tuple_pos;

	*commit_id = merge_commit.id;

	this->commit_info_[*commit_id] = merge_commit;
	this->branches_info_[branch_name1].new_commit(*commit_id);

	return true;
}

bool UstoreHeapStorage::IsEmptyWorkSpace() const{
	return this->modified_tuple_info_.size() == 0 && this->removed_tuple_pos_.size() == 0;
}

bool UstoreHeapStorage::Commit(CommitID* commit_id, std::string* msg) {

	if(commit_id == 0) {
		if (msg != 0) *msg = "Can not pass in an empty commit_id pointer";
		return false;
	}

	if(IsEmptyWorkSpace()){
		if(msg != 0) *msg = "There exists no tuple operations. Can not commit!.";
		return false;	
	}

	CommitID last_commit_id = GetCurrentBranchInfo().commit_ids.back();

//SyncPut the page to ustore 
	value_t ustore_value = value_t(reinterpret_cast<const char*>(commited_buffer_->GetRawData()), commited_buffer_->GetPageSize());

	CommitRecord last_commit = this->commit_info_[last_commit_id];

	version_t ustore_version = this->client_->SyncPut(this->relation_name_, last_commit.ustore_version, ustore_value);

	dynamic_bitset<> new_tuple_presence = last_commit.tuple_presence;
	map<unsigned, RecordID> new_tuple_pos = last_commit.tuple_positions;

	for(const auto& tuple_info: modified_tuple_info_) {
		unsigned tuple_bit_pos = tuple_info.first;
		unsigned tuple_page_index = tuple_info.second;

		//Set the tuple's position
		RecordID tuple_record;
		tuple_record.version = ustore_version;
		tuple_record.tuple_index = tuple_page_index;

		new_tuple_pos[tuple_bit_pos] = tuple_record;

		//Set the bitmap
		if(new_tuple_presence.size() <= tuple_bit_pos) {
			new_tuple_presence.resize(tuple_bit_pos + 1, 0);
		}

		new_tuple_presence[tuple_bit_pos] = true;

	}//end of for

//unset the bit for deleted tuples
	for(const auto& tuple_bit_pos: removed_tuple_pos_){
		new_tuple_presence[tuple_bit_pos] = false;
	}

//Reset relevant vars
	commited_buffer_->Reset();
	modified_tuple_info_.clear();
	removed_tuple_pos_.clear();	

//Create new commit
	CommitRecord new_commit;
	new_commit.id = ustore_version;
	new_commit.ustore_version = ustore_version;
	new_commit.tuple_positions = new_tuple_pos;
	new_commit.tuple_presence = new_tuple_presence;

//Update global branch and commit info
	this->commit_info_[new_commit.id] = new_commit;
	this->branches_info_[this->current_branch_name_].new_commit(new_commit.id);

	*msg = "Commit Succeeded";
	return true;
}

bool UstoreHeapStorage::Checkout(const CommitID& commit_id, const std::string& base_branch_name, const std::string& new_branch_name, std::string* msg){
	

//check for any uncommited tuples
	if(!IsEmptyWorkSpace()){

		if(msg != 0) *msg = "There exists uncommited tuple operation. Can not check out!.";
		return false;	
	}	

//Check base branch exists
	auto branch_info_it = this->branches_info_.find(base_branch_name); 
	if(branch_info_it == this->branches_info_.end()) {
		if (msg != 0) *msg = "Base Branch " + base_branch_name + " does not exist";
		return false;
	}

//Check new branch does not exist
	branch_info_it = this->branches_info_.find(new_branch_name); 
	if(branch_info_it != this->branches_info_.end()) {
		if (msg != 0) *msg = "New Branch " + base_branch_name + " already exists";
		return false;
	}

//check commit exists in base branch.
	vector<CommitID> branch_commit_ids = GetCurrentBranchInfo().commit_ids;
	auto commit_it = find(branch_commit_ids.begin(), branch_commit_ids.end(), commit_id);

	if(commit_it == branch_commit_ids.end()) {
		if(msg != 0) *msg = "Commit with id " + to_string(commit_id) + " is not found in Branch " + base_branch_name;
		return false;	
	}

//Create the new branch
	BranchRecord new_branch;
	new_branch.branch_name = new_branch_name;
	new_branch.base_branch = base_branch_name;
	new_branch.new_commit(commit_id);

	this->branches_info_[new_branch_name] = new_branch;

	Switch(new_branch.branch_name,msg);

	*msg = "Create a new branch " + new_branch_name + " and switch to that";
	return true;
}

bool UstoreHeapStorage::Switch(const string& branch_name, std::string *msg) {

	if(!IsEmptyWorkSpace()){

		if(msg != 0) *msg = "There exists uncommited tuple operation. Can not switch.";
		return false;	
	}	

//Check base branch exists
	auto branch_info_it = this->branches_info_.find(branch_name); 
	if(branch_info_it == this->branches_info_.end()) {
		if (msg != 0) *msg = "Switched Branch " + branch_name + " does not exist";
		return false;
	}

	this->current_branch_name_ = branch_name;

	*msg = "Switch to Branch " + branch_name;
	return true;
}

// Tuple::Iterator* UstoreHeapStorage::ConstructTupleIterator(const std::vector<RecordID>& tuples_pos_,const Predicate* predicate) {


// 	return Tuple::Iterator::GetEmptyIterator();	
// }
}
}