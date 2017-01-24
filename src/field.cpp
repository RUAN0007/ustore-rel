/*
 * type.cc
 *
 *  Created on: Jan 3, 2017
 *      Author: Ruan Pingcheng
 */

#include "field.h"

#include "debug.h"
#include "type.h"

using namespace std;
namespace ustore{
namespace relation{


bool IntField::IsSatisified(ComparisonOp op, const Field* field) const {
    if (!Type::IsIntType(field->GetType())) return false;

    const IntField* int_field = dynamic_cast<const IntField*>(field);
    int cmp_value = int_field->value();

    if (op == kEQ) {
        return value() == cmp_value;
    } else  if (op == kNOT_EQ) {
        return value() != cmp_value;
    } else  if (op == kLESS) {
        return value() < cmp_value;
    } else  if (op == kGREATER) {
        return value() > cmp_value;
    } else  if (op == kLESS_EQ) {
        return value() <= cmp_value;
    } else  if (op == kGREATER_EQ) {
        return value() >= cmp_value;
    }

    LOG(LOG_FATAL, "No Other Comparison Op. Shall not reach here.");
    return false;
}

bool StrField::IsSatisified(ComparisonOp op, const Field* field) const {
    if (!Type::IsStrType(field->GetType())) return false;

    const StrField* str_field = dynamic_cast<const StrField*>(field);
    string cmp_value = str_field->value();

    if (op == kEQ) {
        return value() == cmp_value;
    } else  if (op == kNOT_EQ) {
        return value() != cmp_value;
    } else  if (op == kLESS) {
        return value() < cmp_value;
    } else  if (op == kGREATER) {
        return value() > cmp_value;
    } else  if (op == kLESS_EQ) {
        return value() <= cmp_value;
    } else  if (op == kGREATER_EQ) {
        return value() >= cmp_value;
    }

    LOG(LOG_FATAL, "No Other Comparison Op. Shall not reach here.");
    return false;
}

std::istream& operator>>(std::istream &in, StrField& str_field) {
    size_t len = StrType::GetInstance()->GetLen();
    char* bytes = new char[len];
    in.read(bytes, len);

    Field* parsed_field = StrType::GetInstance()->Parse(
        reinterpret_cast<unsigned char*>(bytes));
    StrField* parsed_str_field = dynamic_cast<StrField*>(parsed_field);
    str_field.value_ = parsed_str_field->value();

    delete parsed_str_field;
    delete[] bytes;
    return in;
}

std::ostream& operator<<(std::ostream &out, StrField str_field) {
    size_t len = StrType::GetInstance()->GetLen();

    const unsigned char* bytes = StrType::GetInstance()->Serialize(&str_field);

    out.write(reinterpret_cast<const char*>(bytes), len);

    delete[] bytes;
    return out;
}

std::istream& operator>>(std::istream &in, IntField& int_field){
    size_t len = IntType::GetInstance()->GetLen();
    char* bytes = new char[len];
    in.read(bytes,len);

    Field* parsed_field = IntType::GetInstance()->Parse(reinterpret_cast<unsigned char*>(bytes));
    IntField* parsed_int_field = dynamic_cast<IntField*>(parsed_field);
    int_field.value_ = parsed_int_field->value();

    delete parsed_int_field;
    delete[] bytes;
    return in;
}

std::ostream& operator<<(std::ostream &out, IntField int_field) {
    size_t len = IntType::GetInstance()->GetLen();
    const unsigned char* bytes = IntType::GetInstance()->Serialize(&int_field);

    out.write(reinterpret_cast<const char*>(bytes), len);

    delete[] bytes;
    return out;
};

}
}//namespace ustore