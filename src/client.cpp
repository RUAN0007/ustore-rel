/*
   @Author: RUAN0007
   @Date:   2017-01-09 09:58:03
   @Last_Modified_At:   2017-01-24 12:38:59
   @Last_Modified_By:   RUAN0007
*/
#include <utility>

#include "./client.h"
#include "./debug.h"
#include "./types.h"

using std::pair;
using std::make_pair;

namespace ustore {
version_t ClientService::SyncPut(const key_t& key, const version_t& pre_version,
                const value_t& value) {
    version_t new_version = objects_[key].values_.size() + 1;
    objects_[key].values_[new_version] = value;
    objects_[key].children_[pre_version].push_back(new_version);
    objects_[key].parents_[new_version] = make_pair(pre_version, NULL_VERSION);
    return new_version;
}
value_t* ClientService::SyncGet(const key_t& key, const version_t& version) {
    if (objects_.count(key) == 0) return new value_t(NULL_VALUE);
    if (objects_[key].values_.count(version) == 0) {
        return new value_t(NULL_VALUE);
    }
    return new value_t(objects_[key].values_[version]);
}
version_t ClientService::SyncMerge(
            const key_t& key, const version_t& pre_version_1,
            const version_t& pre_version_2, const value_t& value) {
    version_t new_version = objects_[key].values_.size() + 1;
    objects_[key].values_[new_version] = value;
    objects_[key].children_[pre_version_1].push_back(new_version);
    objects_[key].children_[pre_version_2].push_back(new_version);
    objects_[key].parents_[new_version]
                   = make_pair(pre_version_1, pre_version_2);
    return new_version;
}
std::pair<version_t, version_t> ClientService::SyncGetPreviousVersion(
  const key_t& key, const version_t& version) {
    if (objects_.count(key) == 0) return make_pair(NULL_VERSION, NULL_VERSION);
    if (objects_[key].values_.count(version) == 0) {
        return make_pair(NULL_VERSION, NULL_VERSION);
    }

    return objects_[key].parents_[version];
}
std::pair<value_t*, value_t*> ClientService::SyncGetPreviousValue(
  const key_t& key, const version_t& version) {
    pair<version_t, version_t> pre_versions =
                             SyncGetPreviousVersion(key, version);
    value_t* v1 = 0;
    value_t* v2 = 0;
    if (pre_versions.first != NULL_VERSION) {
        v1 = new value_t(objects_[key].values_[pre_versions.first]);
    }
    if (pre_versions.second != NULL_VERSION) {
        v2 = new value_t(objects_[key].values_[pre_versions.second]);
    }
    return make_pair(v1, v2);
}
}  // namespace ustore
