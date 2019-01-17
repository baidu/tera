// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "gtest/gtest.h"
#include "tera.h"
#include "sdk/scan_impl.h"
#include "proto/filter.pb.h"
#include "io/coding.h"
#include "proto/tabletnode_rpc.pb.h"

namespace tera {
namespace filter {

struct RowData {
  std::string column_family;
  std::string qualifier;
  std::string value;
};

class ValueFilterTest : public ::testing::Test {};

TEST_F(ValueFilterTest, SetValueFilterINT64) {
  ScanDescriptor scan_desc("");
  int64_t ref_value = INT64_MIN;
  IntegerComparatorPtr comparator =
      std::make_shared<IntegerComparator>(IntegerValueType::kInt64, ref_value);
  ValueFilterPtr value_filter_in =
      std::make_shared<ValueFilter>(CompareOperator::kLess, comparator);
  value_filter_in->SetColumnFamily("cf1");
  value_filter_in->SetColumnQualifier("qu1");
  value_filter_in->SetFilterIfMissing(true);
  ASSERT_TRUE(scan_desc.SetFilter(value_filter_in));

  ScanDescImpl* scan_desc_impl = scan_desc.GetImpl();
  ASSERT_TRUE(scan_desc_impl);
  FilterDesc* filter_desc = scan_desc_impl->GetFilterDesc();
  ASSERT_TRUE(filter_desc);
  EXPECT_EQ(filter_desc->type(), FilterDesc::kValueFilter);
  ValueFilterPtr value_filter_out = std::make_shared<ValueFilter>();
  ASSERT_TRUE(value_filter_out->ParseFrom(filter_desc->serialized_filter()));
  EXPECT_EQ(value_filter_out->column_family_, "cf1");
  EXPECT_EQ(value_filter_out->column_qualifier_, "qu1");
  EXPECT_EQ(value_filter_out->filter_if_missing_, true);
  IntegerComparator* cp = dynamic_cast<IntegerComparator*>(value_filter_out->comparator_.get());
  EXPECT_EQ((int64_t)(cp->integer_value_), ref_value);
}

TEST_F(ValueFilterTest, SetValueFilterUINT64) {
  ScanDescriptor scan_desc("");
  uint64_t ref_value = UINT64_MAX;
  IntegerComparatorPtr comparator =
      std::make_shared<IntegerComparator>(IntegerValueType::kUint64, ref_value);
  ValueFilterPtr value_filter_in =
      std::make_shared<ValueFilter>(CompareOperator::kLessOrEqual, comparator);
  value_filter_in->SetColumnFamily("cf1");
  value_filter_in->SetFilterIfMissing(true);
  ASSERT_TRUE(scan_desc.SetFilter(value_filter_in));

  ScanDescImpl* scan_desc_impl = scan_desc.GetImpl();
  ASSERT_TRUE(scan_desc_impl);
  FilterDesc* filter_desc = scan_desc_impl->GetFilterDesc();
  ASSERT_TRUE(filter_desc);
  EXPECT_EQ(filter_desc->type(), FilterDesc::kValueFilter);
  ValueFilterPtr value_filter_out = std::make_shared<ValueFilter>();
  ASSERT_TRUE(value_filter_out->ParseFrom(filter_desc->serialized_filter()));
  EXPECT_EQ(value_filter_out->column_family_, "cf1");
  EXPECT_EQ(value_filter_out->column_qualifier_, "");
  EXPECT_EQ(value_filter_out->filter_if_missing_, true);
  IntegerComparator* cp = dynamic_cast<IntegerComparator*>(value_filter_out->comparator_.get());
  EXPECT_EQ((uint64_t)(cp->integer_value_), ref_value);
}

TEST_F(ValueFilterTest, SetValueFilterINT32) {
  ScanDescriptor scan_desc("");
  int32_t ref_value = INT32_MIN;
  IntegerComparatorPtr comparator =
      std::make_shared<IntegerComparator>(IntegerValueType::kInt32, ref_value);
  ValueFilterPtr value_filter_in =
      std::make_shared<ValueFilter>(CompareOperator::kEqual, comparator);
  value_filter_in->SetFilterIfMissing(true);
  ASSERT_TRUE(scan_desc.SetFilter(value_filter_in));

  ScanDescImpl* scan_desc_impl = scan_desc.GetImpl();
  ASSERT_TRUE(scan_desc_impl);
  FilterDesc* filter_desc = scan_desc_impl->GetFilterDesc();
  ASSERT_TRUE(filter_desc);
  EXPECT_EQ(filter_desc->type(), FilterDesc::kValueFilter);
  ValueFilterPtr value_filter_out = std::make_shared<ValueFilter>();
  ASSERT_TRUE(value_filter_out->ParseFrom(filter_desc->serialized_filter()));
  EXPECT_EQ(value_filter_out->column_family_, "");
  EXPECT_EQ(value_filter_out->column_qualifier_, "");
  EXPECT_EQ(value_filter_out->filter_if_missing_, true);
  IntegerComparator* cp = dynamic_cast<IntegerComparator*>(value_filter_out->comparator_.get());
  EXPECT_EQ((int32_t)(cp->integer_value_), ref_value);
}

TEST_F(ValueFilterTest, SetValueFilterUINT32) {
  ScanDescriptor scan_desc("");
  uint32_t ref_value = UINT32_MAX;
  IntegerComparatorPtr comparator =
      std::make_shared<IntegerComparator>(IntegerValueType::kUint32, ref_value);
  ValueFilterPtr value_filter_in =
      std::make_shared<ValueFilter>(CompareOperator::kNotEqual, comparator);
  value_filter_in->SetColumnFamily("");
  value_filter_in->SetColumnQualifier("qu1");
  ASSERT_TRUE(scan_desc.SetFilter(value_filter_in));

  ScanDescImpl* scan_desc_impl = scan_desc.GetImpl();
  ASSERT_TRUE(scan_desc_impl);
  FilterDesc* filter_desc = scan_desc_impl->GetFilterDesc();
  ASSERT_TRUE(filter_desc);
  EXPECT_EQ(filter_desc->type(), FilterDesc::kValueFilter);
  ValueFilterPtr value_filter_out = std::make_shared<ValueFilter>();
  ASSERT_TRUE(value_filter_out->ParseFrom(filter_desc->serialized_filter()));
  EXPECT_EQ(value_filter_out->column_family_, "");
  EXPECT_EQ(value_filter_out->column_qualifier_, "qu1");
  EXPECT_EQ(value_filter_out->filter_if_missing_, false);
  IntegerComparator* cp = dynamic_cast<IntegerComparator*>(value_filter_out->comparator_.get());
  EXPECT_EQ((uint32_t)(cp->integer_value_), ref_value);
}

TEST_F(ValueFilterTest, SetValueFilterDecimal) {
  ScanDescriptor scan_desc("");
  float ref_value = 123.456;
  DecimalComparatorPtr comparator = std::make_shared<DecimalComparator>(ref_value);
  ValueFilterPtr value_filter_in =
      std::make_shared<ValueFilter>(CompareOperator::kGreaterOrEqual, comparator);
  value_filter_in->SetColumnFamily("cf1");
  value_filter_in->SetColumnQualifier("qu1");
  ASSERT_TRUE(scan_desc.SetFilter(value_filter_in));

  ScanDescImpl* scan_desc_impl = scan_desc.GetImpl();
  ASSERT_TRUE(scan_desc_impl);
  FilterDesc* filter_desc = scan_desc_impl->GetFilterDesc();
  ASSERT_TRUE(filter_desc);
  EXPECT_EQ(filter_desc->type(), FilterDesc::kValueFilter);
  ValueFilterPtr value_filter_out = std::make_shared<ValueFilter>();
  ASSERT_TRUE(value_filter_out->ParseFrom(filter_desc->serialized_filter()));
  EXPECT_EQ(value_filter_out->column_family_, "cf1");
  EXPECT_EQ(value_filter_out->column_qualifier_, "qu1");
  EXPECT_EQ(value_filter_out->filter_if_missing_, false);
  DecimalComparator* cp = dynamic_cast<DecimalComparator*>(value_filter_out->comparator_.get());
  EXPECT_TRUE(cp->decimal_value_ < ref_value + 1e-6 && cp->decimal_value_ > ref_value - 1e-6);
}

TEST_F(ValueFilterTest, SetValueFilterBinary) {
  ScanDescriptor scan_desc("");
  std::string ref_value = "abcdefg";
  BinaryComparatorPtr comparator = std::make_shared<BinaryComparator>(ref_value);
  ValueFilterPtr value_filter_in =
      std::make_shared<ValueFilter>(CompareOperator::kGreater, comparator);
  value_filter_in->SetColumnFamily("cf1");
  value_filter_in->SetColumnQualifier("qu1");
  ASSERT_TRUE(scan_desc.SetFilter(value_filter_in));

  ScanDescImpl* scan_desc_impl = scan_desc.GetImpl();
  ASSERT_TRUE(scan_desc_impl);
  FilterDesc* filter_desc = scan_desc_impl->GetFilterDesc();
  ASSERT_TRUE(filter_desc);
  EXPECT_EQ(filter_desc->type(), FilterDesc::kValueFilter);
  ValueFilterPtr value_filter_out = std::make_shared<ValueFilter>();
  ASSERT_TRUE(value_filter_out->ParseFrom(filter_desc->serialized_filter()));
  EXPECT_EQ(value_filter_out->column_family_, "cf1");
  EXPECT_EQ(value_filter_out->column_qualifier_, "qu1");
  EXPECT_EQ(value_filter_out->filter_if_missing_, false);
  BinaryComparator* cp = dynamic_cast<BinaryComparator*>(value_filter_out->comparator_.get());
  EXPECT_EQ(cp->value_, ref_value);
}

TEST_F(ValueFilterTest, FilterCase) {
  int64_t ref_value_1 = 10;
  IntegerComparatorPtr comparator1 =
      std::make_shared<IntegerComparator>(IntegerValueType::kInt64, ref_value_1);
  ValueFilterPtr value_filter_1 =
      std::make_shared<ValueFilter>(CompareOperator::kLess, comparator1);
  value_filter_1->SetColumnFamily("cf1");
  value_filter_1->SetColumnQualifier("qu1");
  value_filter_1->SetFilterIfMissing(true);

  float ref_value_2 = 123.456;
  DecimalComparatorPtr comparator2 = std::make_shared<DecimalComparator>(ref_value_2);
  ValueFilterPtr value_filter_2 =
      std::make_shared<ValueFilter>(CompareOperator::kGreaterOrEqual, comparator2);
  value_filter_2->SetColumnFamily("cf2");
  value_filter_2->SetColumnQualifier("qu2");

  std::string ref_value_3 = "abc";
  BinaryComparatorPtr comparator3 = std::make_shared<BinaryComparator>(ref_value_3);
  ValueFilterPtr value_filter_3 =
      std::make_shared<ValueFilter>(CompareOperator::kGreater, comparator3);
  value_filter_3->SetColumnFamily("cf3");
  value_filter_3->SetColumnQualifier("qu3");

  int64_t ref_value_4 = 10;
  IntegerComparatorPtr comparator4 =
      std::make_shared<IntegerComparator>(IntegerValueType::kInt64, ref_value_4);
  ValueFilterPtr value_filter_4 =
      std::make_shared<ValueFilter>(CompareOperator::kLess, comparator1);
  value_filter_4->SetColumnFamily("cf4");
  value_filter_4->SetColumnQualifier("qu4");
  value_filter_4->SetFilterIfMissing(true);

  float ref_value_5 = 123.456;
  DecimalComparatorPtr comparator5 = std::make_shared<DecimalComparator>(ref_value_5);
  ValueFilterPtr value_filter_5 =
      std::make_shared<ValueFilter>(CompareOperator::kGreaterOrEqual, comparator5);
  value_filter_5->SetColumnFamily("cf5");
  value_filter_5->SetColumnQualifier("qu5");

  int64_t ref_value_6 = 11;
  IntegerComparatorPtr comparator6 =
      std::make_shared<IntegerComparator>(IntegerValueType::kInt64, ref_value_6);
  ValueFilterPtr value_filter_6 =
      std::make_shared<ValueFilter>(CompareOperator::kGreater, comparator6);
  value_filter_6->SetColumnFamily("cf1");

  int64_t ref_value_7 = 13;
  IntegerComparatorPtr comparator7 =
      std::make_shared<IntegerComparator>(IntegerValueType::kInt64, ref_value_7);
  ValueFilterPtr value_filter_7 =
      std::make_shared<ValueFilter>(CompareOperator::kLess, comparator7);
  value_filter_7->SetColumnFamily("cf1");

  RowData row_buf[5];

  row_buf[0].column_family = "cf1";
  row_buf[0].qualifier = "";
  std::string value;
  comparator1->EncodeInteger(IntegerValueType::kInt64, 12, &value);
  row_buf[0].value = value;

  row_buf[1].column_family = "cf1";
  row_buf[1].qualifier = "qu0";
  comparator1->EncodeInteger(IntegerValueType::kInt64, 11, &value);
  row_buf[1].value = value;

  row_buf[2].column_family = "cf1";
  row_buf[2].qualifier = "qu1";
  comparator1->EncodeInteger(IntegerValueType::kInt64, 9, &value);
  row_buf[2].value = value;

  row_buf[3].column_family = "cf2";
  row_buf[3].qualifier = "qu2";
  value = comparator2->EncodeDecimal(123.455);
  row_buf[3].value = value;

  row_buf[4].column_family = "cf3";
  row_buf[4].qualifier = "qu3";
  row_buf[4].value = "abd";

  value_filter_1->Reset();
  for (size_t i = 0; i < 5; ++i) {
    Filter::ReturnCode rc = value_filter_1->FilterCell(row_buf[i].column_family,
                                                       row_buf[i].qualifier, row_buf[i].value);
    if (rc == Filter::kNotIncludeCurAndLeftCellOfRow) {
      break;
    }
  }
  EXPECT_EQ(value_filter_1->FilterRow(), false);

  value_filter_2->Reset();
  for (size_t i = 0; i < 5; ++i) {
    Filter::ReturnCode rc = value_filter_2->FilterCell(row_buf[i].column_family,
                                                       row_buf[i].qualifier, row_buf[i].value);
    if (rc == Filter::kNotIncludeCurAndLeftCellOfRow) {
      break;
    }
  }
  EXPECT_EQ(value_filter_2->FilterRow(), true);

  value_filter_3->Reset();
  for (size_t i = 0; i < 5; ++i) {
    Filter::ReturnCode rc = value_filter_3->FilterCell(row_buf[i].column_family,
                                                       row_buf[i].qualifier, row_buf[i].value);
    if (rc == Filter::kNotIncludeCurAndLeftCellOfRow) {
      break;
    }
  }
  EXPECT_EQ(value_filter_3->FilterRow(), false);

  value_filter_4->Reset();
  for (size_t i = 0; i < 5; ++i) {
    Filter::ReturnCode rc = value_filter_4->FilterCell(row_buf[i].column_family,
                                                       row_buf[i].qualifier, row_buf[i].value);
    if (rc == Filter::kNotIncludeCurAndLeftCellOfRow) {
      break;
    }
  }
  EXPECT_EQ(value_filter_4->FilterRow(), true);

  value_filter_5->Reset();
  for (size_t i = 0; i < 5; ++i) {
    Filter::ReturnCode rc = value_filter_5->FilterCell(row_buf[i].column_family,
                                                       row_buf[i].qualifier, row_buf[i].value);
    if (rc == Filter::kNotIncludeCurAndLeftCellOfRow) {
      break;
    }
  }
  EXPECT_EQ(value_filter_5->FilterRow(), false);

  value_filter_6->Reset();
  for (size_t i = 0; i < 5; ++i) {
    Filter::ReturnCode rc = value_filter_6->FilterCell(row_buf[i].column_family,
                                                       row_buf[i].qualifier, row_buf[i].value);
    if (rc == Filter::kNotIncludeCurAndLeftCellOfRow) {
      break;
    }
  }
  EXPECT_EQ(value_filter_6->FilterRow(), true);

  value_filter_7->Reset();
  for (size_t i = 0; i < 5; ++i) {
    Filter::ReturnCode rc = value_filter_7->FilterCell(row_buf[i].column_family,
                                                       row_buf[i].qualifier, row_buf[i].value);
    if (rc == Filter::kNotIncludeCurAndLeftCellOfRow) {
      break;
    }
  }
  EXPECT_EQ(value_filter_7->FilterRow(), false);
}

}  // namespace filter
}  // namespace tera
