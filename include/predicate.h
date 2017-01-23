/*
 * type.h
 *
 *  Created on: Jan 3, 2017
 *      Author: ruanpingcheng
 */

#ifndef INCLUDE_PREDICATE_H_
#define INCLUDE_PREDICATE_H_

#include <string>

#include "./field.h"
#include "./operator.h"

namespace ustore {
namespace relation {


class Predicate{
 public:
Predicate(const std::string& field_name, ComparisonOp op, const Field& field):
    field_name_(field_name),
    op_(op),
    field_(field.clone()) {
}

std::string GetFieldName() const {return field_name_;}

ComparisonOp GetOp() const { return op_;}

const Field* GetField() const { return field_;}

// Return whether the tuple satsifies this predicate
// Note: this function return false if this tuple does not contain field_name_
// bool Filter(Tuple t) const;

~Predicate() {delete field_;}

 private:
std::string field_name_;
ComparisonOp op_;
Field* field_;
};  // class Predicate

}  // namespace relation
}  // namespace ustore
#endif  // INCLUDE_PREDICATE_H_
