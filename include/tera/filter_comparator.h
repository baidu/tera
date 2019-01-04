// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// The Designs of Filter and related codes are inspired by hbase which is licensed under
// Apache 2.0 License (found in the LICENSE.Apache file in the root directory). Please refer to
// https://hbase.apache.org/2.0/apidocs/org/apache/hadoop/hbase/filter/Filter.html
// to see more detailed design of hbase filter.

#pragma once

#include <string>
#include <memory>
#include "tera/error_code.h"

namespace tera {
namespace filter {

/*
 * If you want to output the row where there is a value which is less than the ref_value in the
 * filter (or say in the comparator) in the scanning or reading, please use LESS.
 * Other values are similar.
 */
enum class CompareOperator {
  kLess,            // <
  kLessOrEqual,     // <=
  kEqual,           // ==
  kNotEqual,        // !=
  kGreaterOrEqual,  // >=
  kGreater,         // >
  kNoOp             // invalid
};

enum class ComparatorType {
  kIntegerComparator,  // IntegerComparator
  kDecimalComparator,  // DecimalComparator
  kBinaryComparator    // BinaryComparator
};

enum class IntegerValueType {
  kInt64,   // value is regarded as int64_t
  kUint64,  // value is regarded as uint64_t
  kInt32,   // value is regarded as int32_t
  kUint32,  // value is regarded as uint32_t
  kInt16,   // value is regarded as int16_t
  kUint16,  // value is regarded as uint16_t
  kInt8,    // value is regarded as int8_t
  kUint8,   // value is regarded as uint8_t
  kUnknown  // value type is illigal
};

class FilterComparator;
class IntegerComparator;
class DecimalComparator;
class BinaryComparator;

using FilterComparatorPtr = std::shared_ptr<FilterComparator>;
using IntegerComparatorPtr = std::shared_ptr<IntegerComparator>;
using DecimalComparatorPtr = std::shared_ptr<DecimalComparator>;
using BinaryComparatorPtr = std::shared_ptr<BinaryComparator>;

/*
 * User do NOT need using this class，this is a base class for comparator.
 */
class FilterComparator {
 public:
  virtual ComparatorType Type() = 0;
  virtual int CompareWith(const std::string& value) = 0;
  virtual bool SerializeTo(std::string* serialized_comparator) = 0;
  virtual bool ParseFrom(const std::string& serialized_comparator) = 0;
};

/*
 * User can use this class for making a comparator for filtering the value which is integer.
 * Just Using the Constructor which have two parameters is OK.
 */
class IntegerComparator : public FilterComparator {
  // User Interface
 public:
  /*
   * Use this method to New a object of this class. Just New, and then transmit the object to
   * the filter. Do not need do any other things.
   * Note that the value will be transfered inside the method by the value_type.
   * User must use std::make_shared to New the object and assign it to IntegerComparatorPtr.
   */
  IntegerComparator(IntegerValueType value_type, uint64_t value);

  /*
   * When writing and reading integer values, users can use these two methods
   * to transfer the integer.
   */
  static bool EncodeInteger(IntegerValueType value_type, uint64_t value,
                            std::string* encoded_value);
  static bool DecodeInteger(IntegerValueType value_type, const std::string& value,
                            uint64_t* decoded_value);

  // Internal use
 public:
  IntegerComparator();
  virtual ~IntegerComparator();
  virtual ComparatorType Type();
  virtual int CompareWith(const std::string& value);
  virtual bool SerializeTo(std::string* serialized_comparator);
  virtual bool ParseFrom(const std::string& serialized_comparator);

 private:
  template <typename T>
  int Compare(T v1, T v2) {
    if (v1 < v2) {
      return -1;
    } else if (v1 > v2) {
      return 1;
    } else {
      return 0;
    }
  }

 private:
  IntegerValueType value_type_;
  uint64_t integer_value_;
};

/*
 * User can use this class for making a comparator for filtering the value which is decimal.
 * Just Using the explicit Constructor is OK.
 */
class DecimalComparator : public FilterComparator {
  // User Interface
 public:
  /*
   * Use this method to New a object of this class. Just New, and then transmit the object to
   * the filter. Do not need do any other things.
   * User must use std::make_shared to New the object and assign it to DecimalComparatorPtr.
   */
  explicit DecimalComparator(double value);

  /*
   * When writing and reading decimal values, users can use these two methods
   * to transfer the decimal value.
   */
  static std::string EncodeDecimal(double value);
  static double DecodeDecimal(const std::string& value);

  // Internal use
 public:
  DecimalComparator();
  virtual ~DecimalComparator();
  virtual ComparatorType Type();
  virtual int CompareWith(const std::string& value);
  virtual bool SerializeTo(std::string* serialized_comparator);
  virtual bool ParseFrom(const std::string& serialized_comparator);

 private:
  double decimal_value_;
};

/*
 * User can use this class for making a comparator for filtering the value which is binary (string
 * is also binary).
 * Just Using the explicit Constructor is OK.
 */
class BinaryComparator : public FilterComparator {
  // User Interface
 public:
  /*
   * Use this method to New a object of this class. Just New, and then transmit the object to
   * the filter. Do not need do any other things.
   * User must use std::make_shared to New the object and assign it to BinaryComparatorPtr.
   */
  explicit BinaryComparator(const std::string& value);

  // Internal use
 public:
  BinaryComparator();
  virtual ~BinaryComparator();
  virtual ComparatorType Type();
  virtual int CompareWith(const std::string& value);
  virtual bool SerializeTo(std::string* serialized_comparator);
  virtual bool ParseFrom(const std::string& serialized_comparator);

 private:
  std::string value_;
};

}  // namesapce filter
}  // namesapce tera
