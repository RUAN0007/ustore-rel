/* 

   @Author: RUAN0007
   @Date:   2017-01-09 09:48:07
   @Last_Modified_At:   2017-01-23 17:47:52
   @Last_Modified_By:   RUAN0007

*/


#ifndef INCLUDE_TYPES_H_
#define INCLUDE_TYPES_H_

#include <string>

namespace ustore {


typedef std::string key_t;
typedef uint64_t version_t;
typedef std::string value_t;

const version_t NULL_VERSION = 0;
const value_t NULL_VALUE = "";
}
#endif  // INCLUDE_TYPES_H_
