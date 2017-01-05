/*
 * type.h
 *
 *  Created on: Jan 5, 2017
 *      Author: ruanpingcheng
 */

#include "operator.h"
#include <string>

namespace ustore{
namespace relation{

std::string to_str(ComparisonOp op) {
	if(op == kEQ) {
		return "=";
	}else if(op == kNOT_EQ) {
		return "!=";
	}else if(op == kLESS) {
		return "<";
	}else if(op == kGREATER) {
		return ">";
	}else if(op == kLESS_EQ) {
		return "<=";
	}else if(op == kGREATER_EQ) {
		return ">=";
	}

	return 0;
}
}//namespace ustore
}//namespace ustore
