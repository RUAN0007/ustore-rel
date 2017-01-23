/* 

   @Author: RUAN0007
   @Date:   2017-01-09 09:49:33
   @Last_Modified_At:   2017-01-23 17:09:37
   @Last_Modified_By:   RUAN0007

*/


#ifndef INCLUDE_CLIENT_H_
#define INCLUDE_CLIENT_H_

#include <map>
#include <vector>
#include <utility>

#include "./types.h"

namespace ustore {

struct UObject{
  std::map< version_t, std::vector<version_t> > children_;

  std::map< version_t, std::pair<version_t, version_t> > parents_;

  std::map<version_t, value_t> values_;
};

class ClientService{
 public:
// Synchronous Put, return new version.
  version_t SyncPut(const key_t& key, const version_t& pre_version,
    const value_t& value);
// Synchronous Get, return a pointer to value, the user of this function
// should delete free the memory sapce pointed to by this pointer after use.
  value_t* SyncGet(const key_t& key, const version_t& version);
// Synchronous Merge, return new version.
  version_t SyncMerge(const key_t& key, const version_t& pre_version_1,
    const version_t& pre_version_2, const value_t& value);

// Synchronous GetPreviousVersion, return a pair of version_t. NULL_VERSION
// stands for no previous version on that slot (there could be one existing
// previous version and one NULL_VERSION).
  std::pair<version_t, version_t> SyncGetPreviousVersion(
    const key_t& key, const version_t& version);

// Synchronous Get, return a pair of pointers to values of previous versions,
// the user of this function should free the memory space pointed to by these
// pointers after use. A NULL pointer stands for no value for this slot.
  std::pair<value_t*, value_t*> SyncGetPreviousValue(const key_t& key,
                                                     const version_t& version);

 private:
  std::map<key_t, UObject> objects_;
};

}  // namespace ustore
#endif  // INCLUDE_CLIENT_H_
