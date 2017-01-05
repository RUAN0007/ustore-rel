/*
 * type.h
 *
 *  Created on: Jan 5, 2017
 *      Author: ruanpingcheng
 */

#ifndef INCLUDE_OPERATOR_H_
#define INCLUDE_OPERATOR_H_ 

#include <string>

namespace ustore{
namespace relation{

enum ComparisonOp{
	kEQ,kNOT_EQ,
	kLESS, kGREATER,
	kLESS_EQ, kGREATER_EQ
};

std::string to_str(ComparisonOp op);
}//namespace ustore
}//namespace ustore

#endif