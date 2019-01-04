// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// The Designs of Filter and related codes are inspired by hbase which is licensed under
// Apache 2.0 License (found in the LICENSE.Apache file in the root directory). Please refer to
// https://hbase.apache.org/2.0/apidocs/org/apache/hadoop/hbase/filter/Filter.html
// to see more detailed design of hbase filter.

#include <string>
#include <memory>
#include "tera.h"
#include "proto/filter.pb.h"
#include "filter_utils.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "io/coding.h"

namespace tera {
namespace filter {

bool IntegerComparator::EncodeInteger(IntegerValueType value_type, uint64_t value,
                                      std::string* encoded_value) {
  char* buf;
  switch (value_type) {
    case IntegerValueType::kInt64:
    case IntegerValueType::kUint64:
      encoded_value->resize(sizeof(int64_t) + 1);
      buf = const_cast<char*>(encoded_value->c_str());
      memcpy(buf, &value, sizeof(value));
      return true;
    case IntegerValueType::kInt32:
    case IntegerValueType::kUint32:
      encoded_value->resize(sizeof(int32_t) + 1);
      buf = const_cast<char*>(encoded_value->c_str());
      memcpy(buf, &value, sizeof(value));
      return true;
    case IntegerValueType::kInt16:
    case IntegerValueType::kUint16:
      encoded_value->resize(sizeof(int16_t) + 1);
      buf = const_cast<char*>(encoded_value->c_str());
      memcpy(buf, &value, sizeof(value));
      return true;
    case IntegerValueType::kInt8:
    case IntegerValueType::kUint8:
      encoded_value->resize(sizeof(int8_t) + 1);
      buf = const_cast<char*>(encoded_value->c_str());
      memcpy(buf, &value, sizeof(value));
      return true;
    default:
      LOG(ERROR) << "not support IntegerValueType";
      return false;
  }
}

bool IntegerComparator::DecodeInteger(IntegerValueType value_type, const std::string& value,
                                      uint64_t* decoded_value) {
  switch (value_type) {
    case IntegerValueType::kInt64:
    case IntegerValueType::kUint64:
      memcpy(decoded_value, const_cast<char*>(value.c_str()), sizeof(uint64_t));
      return true;
    case IntegerValueType::kInt32:
    case IntegerValueType::kUint32:
      memcpy(decoded_value, const_cast<char*>(value.c_str()), sizeof(uint32_t));
      return true;
    case IntegerValueType::kInt16:
    case IntegerValueType::kUint16:
      memcpy(decoded_value, const_cast<char*>(value.c_str()), sizeof(uint16_t));
      return true;
    case IntegerValueType::kInt8:
    case IntegerValueType::kUint8:
      memcpy(decoded_value, const_cast<char*>(value.c_str()), sizeof(uint8_t));
      return true;
    default:
      LOG(ERROR) << "not support IntegerValueType";
      return false;
  }
}

IntegerComparator::IntegerComparator()
    : value_type_(IntegerValueType::kUnknown), integer_value_(0) {}

IntegerComparator::IntegerComparator(IntegerValueType value_type, uint64_t value)
    : value_type_(value_type), integer_value_(value) {}

IntegerComparator::~IntegerComparator() {}

ComparatorType IntegerComparator::Type() { return ComparatorType::kIntegerComparator; }

int IntegerComparator::CompareWith(const std::string& value) {
  uint64_t decoded_value = 0;
  if (!DecodeInteger(value_type_, value, &decoded_value)) {
    return 0;
  }
  switch (value_type_) {
    case IntegerValueType::kInt64:
      return Compare(static_cast<int64_t>(decoded_value), static_cast<int64_t>(integer_value_));
    case IntegerValueType::kUint64:
      return Compare(static_cast<uint64_t>(decoded_value), static_cast<uint64_t>(integer_value_));
    case IntegerValueType::kInt32:
      return Compare(static_cast<int32_t>(decoded_value), static_cast<int32_t>(integer_value_));
    case IntegerValueType::kUint32:
      return Compare(static_cast<uint32_t>(decoded_value), static_cast<uint32_t>(integer_value_));
    case IntegerValueType::kInt16:
      return Compare(static_cast<int16_t>(decoded_value), static_cast<int16_t>(integer_value_));
    case IntegerValueType::kUint16:
      return Compare(static_cast<uint16_t>(decoded_value), static_cast<uint16_t>(integer_value_));
    case IntegerValueType::kInt8:
      return Compare(static_cast<int8_t>(decoded_value), static_cast<int8_t>(integer_value_));
    case IntegerValueType::kUint8:
      return Compare(static_cast<uint8_t>(decoded_value), static_cast<uint8_t>(integer_value_));
    default:
      LOG(ERROR) << "not support IntegerValueType";
      return 0;
  }
}

bool IntegerComparator::SerializeTo(std::string* serialized_comparator) {
  IntegerComparatorDesc comparator_desc;
  FilterValueType filter_value_type = TransIntegerValueType(value_type_);
  if (filter_value_type == FilterValueType::kUnknownValueType) {
    return false;
  }
  comparator_desc.set_value_type(filter_value_type);
  comparator_desc.set_integer_value(integer_value_);
  return comparator_desc.SerializeToString(serialized_comparator);
}

bool IntegerComparator::ParseFrom(const std::string& serialized_comparator) {
  IntegerComparatorDesc comparator_desc;
  bool ret = comparator_desc.ParseFromString(serialized_comparator);
  if (!ret) {
    LOG(ERROR) << "parse pb string failed";
    return false;
  }
  integer_value_ = comparator_desc.integer_value();
  value_type_ = TransFilterValueType(comparator_desc.value_type());
  if (value_type_ == IntegerValueType::kUnknown) {
    return false;
  }
  return true;
}

std::string DecimalComparator::EncodeDecimal(double value) {
  std::string encoded_value;
  encoded_value.resize(sizeof(double) + 1);
  char* buf = const_cast<char*>(encoded_value.c_str());
  memcpy(buf, &value, sizeof(value));
  return encoded_value;
}

double DecimalComparator::DecodeDecimal(const std::string& value) {
  double decoded_value;
  memcpy(&decoded_value, const_cast<char*>(value.c_str()), sizeof(decoded_value));
  return decoded_value;
}

DecimalComparator::DecimalComparator() : decimal_value_(0.0) {}

DecimalComparator::DecimalComparator(double value) : decimal_value_(value) {}

DecimalComparator::~DecimalComparator() {}

ComparatorType DecimalComparator::Type() { return ComparatorType::kDecimalComparator; }

int DecimalComparator::CompareWith(const std::string& value) {
  double db_value = DecodeDecimal(value);
  double diff = db_value - decimal_value_;
  const double EPSINON = 1e-10;
  if (diff > -1 * EPSINON && diff < EPSINON) {
    return 0;
  } else if (diff > EPSINON) {
    return 1;
  } else {
    return -1;
  }
}

bool DecimalComparator::SerializeTo(std::string* serialized_comparator) {
  DecimalComparatorDesc comparator_desc;
  comparator_desc.set_decimal_value(decimal_value_);
  return comparator_desc.SerializeToString(serialized_comparator);
}

bool DecimalComparator::ParseFrom(const std::string& serialized_comparator) {
  DecimalComparatorDesc comparator_desc;
  bool ret = comparator_desc.ParseFromString(serialized_comparator);
  if (!ret) {
    LOG(ERROR) << "parse pb string failed";
    return false;
  }
  decimal_value_ = comparator_desc.decimal_value();
  return true;
}

BinaryComparator::BinaryComparator() : value_("") {}

BinaryComparator::BinaryComparator(const std::string& value) : value_(value) {}

BinaryComparator::~BinaryComparator() {}

ComparatorType BinaryComparator::Type() { return ComparatorType::kBinaryComparator; }

int BinaryComparator::CompareWith(const std::string& value) {
  if (value > value_) {
    return 1;
  } else if (value < value_) {
    return -1;
  } else {
    return 0;
  }
}

bool BinaryComparator::SerializeTo(std::string* serialized_comparator) {
  BinaryComparatorDesc comparator_desc;
  comparator_desc.set_value(value_);
  return comparator_desc.SerializeToString(serialized_comparator);
}

bool BinaryComparator::ParseFrom(const std::string& serialized_comparator) {
  BinaryComparatorDesc comparator_desc;
  bool ret = comparator_desc.ParseFromString(serialized_comparator);
  if (!ret) {
    LOG(ERROR) << "parse pb string failed";
    return false;
  }
  value_ = comparator_desc.value();
  return true;
}

}  // namesapce filter
}  // namesapce tera
