// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "gtest/gtest.h"
#include "tera.h"
#include "sdk/scan_impl.h"
#include "proto/filter.pb.h"
#include "io/coding.h"
#include "io/tablet_io.h"
#include "proto/tabletnode_rpc.pb.h"
#include "sdk/filter_list_base.h"
#include "leveldb/raw_key_operator.h"
#include "leveldb/db/dbformat.h"
#include "leveldb/comparator.h"
#include "leveldb/db/memtable.h"

namespace tera {
namespace filter {

class FilterListTest : public ::testing::Test {
 private:
  void SetFilterListForCheck(ScanDescriptor* desc);
  void CheckFilterList(const ScanDescriptor& desc);
  void SetFilterListForFilterOneRow(ScanDescriptor* desc);
  void FilterOneRowCase1(tera::io::TabletIO& tablet_io, const tera::io::ScanOptions& scan_options);
  void FilterOneRowCase2(tera::io::TabletIO& tablet_io, const tera::io::ScanOptions& scan_options);
  void FilterOneRowCase3(tera::io::TabletIO& tablet_io, const tera::io::ScanOptions& scan_options);
  void FilterOneRowCase4(tera::io::TabletIO& tablet_io, const tera::io::ScanOptions& scan_options);
  void FilterOneRowCase5(tera::io::TabletIO& tablet_io, const tera::io::ScanOptions& scan_options);
  void FilterOneRowForEmptyRow(tera::io::TabletIO& tablet_io,
                               const tera::io::ScanOptions& scan_options);
  void FilterOneRowForEmptyFilter(tera::io::TabletIO& tablet_io,
                                  const tera::io::ScanOptions& scan_options);
  void FilterHalfRowCase1(tera::io::TabletIO& tablet_io, const tera::io::ScanOptions& scan_options,
                          const tera::io::SingleRowBuffer& row_buf);
  void MakeOldFilterList(tera::FilterList* old_filter_list_desc);
};

void FilterListTest::SetFilterListForCheck(ScanDescriptor* desc) {
  int64_t ref_value_1 = 10;
  IntegerComparatorPtr comparator_1 =
      std::make_shared<IntegerComparator>(IntegerValueType::kInt64, ref_value_1);
  ValueFilterPtr value_filter_1 =
      std::make_shared<ValueFilter>(CompareOperator::kGreaterOrEqual, comparator_1);
  int64_t ref_value_2 = 5;
  IntegerComparatorPtr comparator_2 =
      std::make_shared<IntegerComparator>(IntegerValueType::kInt64, ref_value_2);
  ValueFilterPtr value_filter_2 =
      std::make_shared<ValueFilter>(CompareOperator::kLess, comparator_2);
  int64_t ref_value_3 = 0;
  IntegerComparatorPtr comparator_3 =
      std::make_shared<IntegerComparator>(IntegerValueType::kInt64, ref_value_3);
  ValueFilterPtr value_filter_3 =
      std::make_shared<ValueFilter>(CompareOperator::kGreater, comparator_3);

  FilterListPtr sub_filter_list = std::make_shared<FilterList>(FilterList::kOr);
  sub_filter_list->AddFilter(value_filter_1);
  sub_filter_list->AddFilter(value_filter_2);
  FilterListPtr filter_list = std::make_shared<FilterList>(FilterList::kAnd);
  filter_list->AddFilter(value_filter_3);
  filter_list->AddFilter(sub_filter_list);

  ASSERT_TRUE(desc->SetFilter(filter_list));
}

void FilterListTest::CheckFilterList(const ScanDescriptor& desc) {
  ScanDescImpl* scan_desc_impl = desc.GetImpl();
  ASSERT_TRUE(scan_desc_impl);
  FilterDesc* filter_desc = scan_desc_impl->GetFilterDesc();
  ASSERT_TRUE(filter_desc);
  EXPECT_EQ(filter_desc->type(), FilterDesc::kFilterList);
  FilterListPtr filter_list = std::make_shared<FilterList>();
  ASSERT_TRUE(filter_list->ParseFrom(filter_desc->serialized_filter()));
  EXPECT_EQ(filter_list->op_, FilterList::kAnd);

  const std::vector<FilterPtr>& filters = filter_list->filter_list_base_->GetFilters();
  EXPECT_EQ(filters[0]->Type(), kValueFilter);
  ValueFilter* value_filter = dynamic_cast<ValueFilter*>(filters[0].get());
  IntegerComparator* cp = dynamic_cast<IntegerComparator*>(value_filter->comparator_.get());
  int64_t ref_value_3 = 0;
  EXPECT_EQ((int64_t)(cp->integer_value_), ref_value_3);
  EXPECT_EQ(filters[1]->Type(), kFilterList);
  FilterList* sub_filter_list = dynamic_cast<FilterList*>(filters[1].get());
  const std::vector<FilterPtr>& sub_filters = sub_filter_list->filter_list_base_->GetFilters();
  EXPECT_EQ(sub_filters[0]->Type(), kValueFilter);
  value_filter = dynamic_cast<ValueFilter*>(sub_filters[0].get());
  cp = dynamic_cast<IntegerComparator*>(value_filter->comparator_.get());
  int64_t ref_value_1 = 10;
  EXPECT_EQ((int64_t)(cp->integer_value_), ref_value_1);
  EXPECT_EQ(sub_filters[1]->Type(), kValueFilter);
  value_filter = dynamic_cast<ValueFilter*>(sub_filters[1].get());
  cp = dynamic_cast<IntegerComparator*>(value_filter->comparator_.get());
  int64_t ref_value_2 = 5;
  EXPECT_EQ((int64_t)(cp->integer_value_), ref_value_2);
}

TEST_F(FilterListTest, SetFilterList) {
  ScanDescriptor scan_desc("");
  SetFilterListForCheck(&scan_desc);
  CheckFilterList(scan_desc);
}

void FilterListTest::SetFilterListForFilterOneRow(ScanDescriptor* desc) {
  BinaryComparatorPtr comparator_1 = std::make_shared<BinaryComparator>("d");
  ValueFilterPtr value_filter_1 =
      std::make_shared<ValueFilter>(CompareOperator::kGreaterOrEqual, comparator_1);
  value_filter_1->SetColumnFamily("cf5");
  value_filter_1->SetColumnQualifier("qu5");
  BinaryComparatorPtr comparator_2 = std::make_shared<BinaryComparator>("m");
  ValueFilterPtr value_filter_2 =
      std::make_shared<ValueFilter>(CompareOperator::kLess, comparator_2);
  value_filter_2->SetColumnFamily("cf5");
  value_filter_2->SetColumnQualifier("qu5");
  BinaryComparatorPtr comparator_3 = std::make_shared<BinaryComparator>("v");
  ValueFilterPtr value_filter_3 =
      std::make_shared<ValueFilter>(CompareOperator::kGreater, comparator_3);
  value_filter_3->SetColumnFamily("cf5");
  value_filter_3->SetColumnQualifier("qu5");

  FilterListPtr sub_filter_list = std::make_shared<FilterList>(FilterList::kAnd);
  sub_filter_list->AddFilter(value_filter_1);
  sub_filter_list->AddFilter(value_filter_2);
  FilterListPtr filter_list = std::make_shared<FilterList>(FilterList::kOr);
  filter_list->AddFilter(sub_filter_list);
  filter_list->AddFilter(value_filter_3);

  ASSERT_TRUE(desc->SetFilter(filter_list));
}

void FilterListTest::FilterOneRowCase1(tera::io::TabletIO& tablet_io,
                                       const tera::io::ScanOptions& scan_options) {
  tera::io::SingleRowBuffer row_buf;
  row_buf.Add("key", "cf1", "qu1", "a", 0);
  row_buf.Add("key", "cf2", "qu2", "a", 0);
  row_buf.Add("key", "cf5", "qu5", "a", 0);
  row_buf.Add("key", "cf7", "qu7", "a", 0);

  EXPECT_EQ(tablet_io.ShouldFilterRowBuffer(row_buf, scan_options), true);
}

void FilterListTest::FilterOneRowCase2(tera::io::TabletIO& tablet_io,
                                       const tera::io::ScanOptions& scan_options) {
  tera::io::SingleRowBuffer row_buf;
  row_buf.Add("key", "cf1", "qu1", "a", 0);
  row_buf.Add("key", "cf2", "qu2", "a", 0);
  row_buf.Add("key", "cf5", "qu5", "d", 0);
  row_buf.Add("key", "cf7", "qu7", "a", 0);

  EXPECT_EQ(tablet_io.ShouldFilterRowBuffer(row_buf, scan_options), false);
}

void FilterListTest::FilterOneRowCase3(tera::io::TabletIO& tablet_io,
                                       const tera::io::ScanOptions& scan_options) {
  tera::io::SingleRowBuffer row_buf;
  row_buf.Add("key", "cf1", "qu1", "a", 0);
  row_buf.Add("key", "cf2", "qu2", "a", 0);
  row_buf.Add("key", "cf5", "qu5", "m", 0);
  row_buf.Add("key", "cf7", "qu7", "a", 0);

  EXPECT_EQ(tablet_io.ShouldFilterRowBuffer(row_buf, scan_options), true);
}

void FilterListTest::FilterOneRowCase4(tera::io::TabletIO& tablet_io,
                                       const tera::io::ScanOptions& scan_options) {
  tera::io::SingleRowBuffer row_buf;
  row_buf.Add("key", "cf1", "qu1", "a", 0);
  row_buf.Add("key", "cf2", "qu2", "a", 0);
  row_buf.Add("key", "cf5", "qu5", "v", 0);
  row_buf.Add("key", "cf7", "qu7", "a", 0);

  EXPECT_EQ(tablet_io.ShouldFilterRowBuffer(row_buf, scan_options), true);
}

void FilterListTest::FilterOneRowCase5(tera::io::TabletIO& tablet_io,
                                       const tera::io::ScanOptions& scan_options) {
  tera::io::SingleRowBuffer row_buf;
  row_buf.Add("key", "cf1", "qu1", "a", 0);
  row_buf.Add("key", "cf2", "qu2", "a", 0);
  row_buf.Add("key", "cf5", "qu5", "x", 0);
  row_buf.Add("key", "cf7", "qu7", "a", 0);

  EXPECT_EQ(tablet_io.ShouldFilterRowBuffer(row_buf, scan_options), false);
}

void FilterListTest::FilterOneRowForEmptyRow(tera::io::TabletIO& tablet_io,
                                             const tera::io::ScanOptions& scan_options) {
  tera::io::SingleRowBuffer row_buf;

  EXPECT_EQ(tablet_io.ShouldFilterRowBuffer(row_buf, scan_options), true);
}

void FilterListTest::FilterOneRowForEmptyFilter(tera::io::TabletIO& tablet_io,
                                                const tera::io::ScanOptions& scan_options) {
  tera::io::SingleRowBuffer row_buf;
  row_buf.Add("key", "cf1", "qu1", "a", 0);
  row_buf.Add("key", "cf2", "qu2", "a", 0);
  row_buf.Add("key", "cf5", "qu5", "x", 0);
  row_buf.Add("key", "cf7", "qu7", "a", 0);

  EXPECT_EQ(tablet_io.ShouldFilterRowBuffer(row_buf, scan_options), false);
}

TEST_F(FilterListTest, FilterOneRow) {
  ScanDescriptor scan_desc("");
  SetFilterListForFilterOneRow(&scan_desc);
  ScanDescImpl* scan_desc_impl = scan_desc.GetImpl();
  ASSERT_TRUE(scan_desc_impl);
  FilterDesc* filter_desc = scan_desc_impl->GetFilterDesc();
  ASSERT_TRUE(filter_desc);

  tera::io::ScanOptions scan_options;
  tera::io::TabletIO tablet_io("", "", "");

  FilterOneRowForEmptyFilter(tablet_io, scan_options);

  ASSERT_TRUE(tablet_io.SetupFilter(*filter_desc, &scan_options));

  FilterOneRowCase1(tablet_io, scan_options);
  FilterOneRowCase2(tablet_io, scan_options);
  FilterOneRowCase3(tablet_io, scan_options);
  FilterOneRowCase4(tablet_io, scan_options);
  FilterOneRowCase5(tablet_io, scan_options);
  FilterOneRowForEmptyRow(tablet_io, scan_options);
}

void FilterListTest::MakeOldFilterList(tera::FilterList* old_filter_list_desc) {
  tera::Filter* old_filter_desc = old_filter_list_desc->add_filter();
  old_filter_desc->set_type(tera::BinComp);
  old_filter_desc->set_bin_comp_op(tera::GE);
  old_filter_desc->set_field(tera::ValueFilter);
  old_filter_desc->set_content("cf3");
  std::string ref_value_1;
  int64_t value_int64 = 11;
  ref_value_1.assign((char*)&value_int64, sizeof(int64_t));
  old_filter_desc->set_ref_value(ref_value_1);
  old_filter_desc->set_value_type(tera::kINT64);

  old_filter_desc = old_filter_list_desc->add_filter();
  old_filter_desc->set_type(tera::BinComp);
  old_filter_desc->set_bin_comp_op(tera::LT);
  old_filter_desc->set_field(tera::ValueFilter);
  old_filter_desc->set_content("cf5");
  std::string ref_value_2;
  value_int64 = 20;
  ref_value_2.assign((char*)&value_int64, sizeof(int64_t));
  old_filter_desc->set_ref_value(ref_value_2);
  old_filter_desc->set_value_type(tera::kINT64);
}

TEST_F(FilterListTest, TransFilter) {
  tera::FilterList old_filter_list_desc;
  MakeOldFilterList(&old_filter_list_desc);
  tera::io::ScanOptions scan_options;
  tera::io::TabletIO tablet_io("", "", "");
  ASSERT_TRUE(tablet_io.TransFilter(old_filter_list_desc, &scan_options));

  tera::io::SingleRowBuffer row_buf;
  IntegerComparatorPtr comparator = std::make_shared<IntegerComparator>();
  std::string value;
  comparator->EncodeInteger(IntegerValueType::kInt64, 11, &value);
  row_buf.Add("key", "cf1", "", value, 0);
  row_buf.Add("key", "cf3", "", value, 0);
  row_buf.Add("key", "cf5", "", value, 0);
  row_buf.Add("key", "cf7", "", value, 0);

  EXPECT_EQ(tablet_io.ShouldFilterRowBuffer(row_buf, scan_options), false);
}

TEST_F(FilterListTest, TransFilterAbnormal) {
  tera::io::TabletIO tablet_io("", "", "");
  tera::FilterList old_filter_list_desc;
  tera::Filter* old_filter_desc = old_filter_list_desc.add_filter();
  {
    tera::io::ScanOptions scan_options;
    EXPECT_EQ(tablet_io.TransFilter(old_filter_list_desc, &scan_options), false);
  }
  old_filter_desc->set_type(tera::Regex);
  {
    tera::io::ScanOptions scan_options;
    EXPECT_EQ(tablet_io.TransFilter(old_filter_list_desc, &scan_options), false);
  }
  old_filter_desc->set_type(tera::BinComp);
  {
    tera::io::ScanOptions scan_options;
    EXPECT_EQ(tablet_io.TransFilter(old_filter_list_desc, &scan_options), false);
  }
  old_filter_desc->set_field(tera::RowFilter);
  {
    tera::io::ScanOptions scan_options;
    EXPECT_EQ(tablet_io.TransFilter(old_filter_list_desc, &scan_options), false);
  }
  old_filter_desc->set_field(tera::ValueFilter);
  {
    tera::io::ScanOptions scan_options;
    EXPECT_EQ(tablet_io.TransFilter(old_filter_list_desc, &scan_options), false);
  }
  old_filter_desc->set_value_type(tera::kINT64);
  {
    tera::io::ScanOptions scan_options;
    EXPECT_EQ(tablet_io.TransFilter(old_filter_list_desc, &scan_options), false);
  }
  old_filter_desc->set_content("cf1");
  {
    tera::io::ScanOptions scan_options;
    EXPECT_EQ(tablet_io.TransFilter(old_filter_list_desc, &scan_options), false);
  }
  std::string ref_value_1;
  int64_t value_int64 = 11;
  ref_value_1.assign((char*)&value_int64, sizeof(int64_t));
  old_filter_desc->set_ref_value(ref_value_1);
  {
    tera::io::ScanOptions scan_options;
    EXPECT_EQ(tablet_io.TransFilter(old_filter_list_desc, &scan_options), false);
  }
  old_filter_desc->set_bin_comp_op(tera::GT);
  {
    tera::io::ScanOptions scan_options;
    EXPECT_EQ(tablet_io.TransFilter(old_filter_list_desc, &scan_options), true);
  }
}

}  // namespace filter
}  // namespace tera
