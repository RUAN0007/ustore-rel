/* 

   @Author: RUAN0007
   @Date:   2017-01-09 18:49:59
   @Last_Modified_At:   2017-01-10 09:36:01
   @Last_Modified_By:   RUAN0007

*/


#ifndef INCLUDE_VERSION_H
#define INCLUDE_VERSION_H

#include "types.h"

#include <map>
#include <vector>
#include <boost/dynamic_bitset.hpp>

namespace ustore{
namespace relation{

typedef version_t CommitID;

//RecordID uniquely identifies a tuple's location in Ustore.
typedef struct {
	version_t version; //the version_t of the page that contains this tuple
	unsigned tuple_index; // the tuple's index in this page, starting with 0. 
} RecordID;

typedef struct {

	// Commit(CommitID init_id, version_t init_start, version_t init_end){
	// 	id = init_id;
	// 	start_version = init_start;
	// 	end_version = init_end;
	// }
	
	CommitID id; //id of this commit. Shall be the version_t of start_version
	version_t start_version; //first version_t of the commited page
	version_t end_version; //last version_t of the commited page

	//mapping pairs of tuple bit position to tuple's location in Ustore
	std::map<unsigned, RecordID> tuple_positions;
    boost::dynamic_bitset<> tuple_presence; //Bitmap to indicate the tuple's presence in this commit
} Commit;

typedef struct Branch{
	std::string branch_name;// this branch name
	std::string base_branch; // the base branch from which this branch originates

	//the vector of commitID of this branch
	//the first CommitID shall be in base_branch
	//the last commitID is where the current branch points to
	std::vector<CommitID> commit_ids; 

	void new_commit(CommitID commit_id){
		commit_ids.push_back(commit_id);
	}
} Branch;

}
}


#endif