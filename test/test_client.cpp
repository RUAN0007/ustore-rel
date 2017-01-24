/* 

   @Author: RUAN0007
   @Date:   2017-01-09 10:53:35
   @Last_Modified_At:   2017-01-24 09:35:03
   @Last_Modified_By:   RUAN0007

*/

#include <gtest/gtest.h>

#include <utility>

#include "./client.h"

using ustore::ClientService;
using ustore::NULL_VALUE;
using ustore::NULL_VERSION;
using ustore::version_t;
using ustore::value_t;

using  std::pair;

class ClientTest : public ::testing::Test {
 protected:
  virtual void SetUp() {
      v1 = client.SyncPut("key", NULL_VERSION, "1");
      v2 = client.SyncPut("key", v1, "2");
      v3 = client.SyncPut("key", NULL_VERSION, "3");

      v4 = client.SyncMerge("key", v2, v3, "4");
  }

  virtual void TearDown() {}

  ClientService client;

  version_t v1, v2, v3, v4;
};


TEST_F(ClientTest, SyncGet) {
    value_t* v = client.SyncGet("key", v2);

    ASSERT_STREQ(v->c_str(), "2");
    delete v;
}

TEST_F(ClientTest, SyncGetSinglePreVersion) {
    pair<version_t, version_t> pre_versions =
             client.SyncGetPreviousVersion("key", v2);

    EXPECT_EQ(pre_versions.first, v1);
    EXPECT_EQ(pre_versions.second, NULL_VERSION);
}

TEST_F(ClientTest, SyncGetBothPreVersion) {
    pair<version_t, version_t> pre_versions =
                 client.SyncGetPreviousVersion("key", v4);

    EXPECT_EQ(pre_versions.first, v2);
    EXPECT_EQ(pre_versions.second, v3);
}

// TEST_f(ClientTest, SyncGetNonePreVersion) {

//     pair<version_t,version_t> pre_versions
//           = client.SyncGetPreviousVersion("key", v4);

//     EXPECT_EQ(pre_versions.first, v2);
//     EXPECT_EQ(pre_versions.second, v3);
// }

TEST_F(ClientTest, SyncGetSinglePreValue) {
    pair<value_t*, value_t*> pre_values =
                   client.SyncGetPreviousValue("key", v2);

    EXPECT_STREQ(pre_values.first->c_str(), "1");
    EXPECT_EQ(pre_values.second, reinterpret_cast<value_t*>(0));

    delete pre_values.first;
    delete pre_values.second;
}

TEST_F(ClientTest, SyncGetBothPreValue) {
    pair<value_t*, value_t*> pre_values =
                         client.SyncGetPreviousValue("key", v4);

    EXPECT_STREQ(pre_values.first->c_str(), "2");
    EXPECT_STREQ(pre_values.second->c_str(), "3");

    delete pre_values.first;
    delete pre_values.second;
}

TEST_F(ClientTest, SyncGetNonePreValue) {
    version_t v5 = 5;
    pair<value_t*, value_t*> pre_values =
             client.SyncGetPreviousValue("key", v5);

    EXPECT_EQ(pre_values.first, reinterpret_cast<value_t*>(0));
    EXPECT_EQ(pre_values.second, reinterpret_cast<value_t*>(0));

    delete pre_values.first;
    delete pre_values.second;

    pre_values = client.SyncGetPreviousValue("nonkey", v4);

    EXPECT_EQ(pre_values.first, reinterpret_cast<value_t*>(0));
    EXPECT_EQ(pre_values.second, reinterpret_cast<value_t*>(0));

    delete pre_values.first;
    delete pre_values.second;
}
