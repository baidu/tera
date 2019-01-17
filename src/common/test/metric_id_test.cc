// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <iostream>
#include <string>
#include <vector>

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

#include "common/metric/metric_id.h"

namespace tera {

static const std::string kTestMetricName = "test_name";

class MetricIdTest : public ::testing::Test {
 public:
  virtual void SetUp() {
    empty_id_ = new MetricId();
    id_with_name_ = new MetricId(kTestMetricName);

    MetricLabels label_map;
    label_map.insert(std::make_pair("test_label1", "test_value1"));
    label_map.insert(std::make_pair("test_label2", "test_value2"));
    label_str_ = "test_label1:test_value1,test_label2:test_value2";

    id_with_label_ = new MetricId("", label_map);
    id_with_name_and_label_ = new MetricId(kTestMetricName, label_map);
  }

  virtual void TearDown() {
    delete empty_id_;
    delete id_with_name_;
    delete id_with_label_;
    delete id_with_name_and_label_;
  }

 private:
  MetricId *empty_id_;
  MetricId *id_with_name_;
  MetricId *id_with_label_;
  MetricId *id_with_name_and_label_;
  std::string label_str_;
};

TEST_F(MetricIdTest, BasicTest) {
  // empty id
  ASSERT_FALSE(empty_id_->IsValid());
  ASSERT_TRUE(empty_id_->GetName().empty());
  ASSERT_TRUE(empty_id_->GetLabelMap().empty());
  ASSERT_TRUE(empty_id_->ToString().empty());
  ASSERT_TRUE(empty_id_->GetLabel("whatever_label").empty());
  ASSERT_FALSE(empty_id_->ExistLabel("whatever_label"));
  ASSERT_FALSE(empty_id_->CheckLabel("whatever_label", "whatever_value"));

  // id with name, empty label
  ASSERT_TRUE(id_with_name_->IsValid());
  ASSERT_STREQ(id_with_name_->GetName().c_str(), kTestMetricName.c_str());
  ASSERT_TRUE(id_with_name_->GetLabelMap().empty());
  ASSERT_STREQ(id_with_name_->ToString().c_str(), kTestMetricName.c_str());
  ASSERT_TRUE(id_with_name_->GetLabel("whatever_label").empty());
  ASSERT_FALSE(id_with_name_->ExistLabel("whatever_label"));
  ASSERT_FALSE(id_with_name_->CheckLabel("whatever_label", "whatever_value"));

  // id with name and label
  ASSERT_TRUE(id_with_name_and_label_->IsValid());
  ASSERT_STREQ(id_with_name_and_label_->GetName().c_str(), kTestMetricName.c_str());
  ASSERT_EQ(id_with_name_and_label_->GetLabelMap().size(), 2);

  std::string expected_id_str = kTestMetricName + kNameLabelsDelimiter + label_str_;
  ASSERT_STREQ(id_with_name_and_label_->ToString().c_str(), expected_id_str.c_str());
  ASSERT_STREQ(id_with_name_and_label_->GetLabel("test_label1").c_str(), "test_value1");
  ASSERT_TRUE(id_with_name_and_label_->ExistLabel("test_label1"));
  ASSERT_TRUE(id_with_name_and_label_->CheckLabel("test_label1", "test_value1"));

  ASSERT_TRUE(id_with_name_and_label_->GetLabel("not_exist_label").empty());
  ASSERT_FALSE(id_with_name_and_label_->ExistLabel("not_exist_label"));
  ASSERT_FALSE(id_with_name_and_label_->CheckLabel("not_exist_label", "test_value1"));
  ASSERT_FALSE(id_with_name_and_label_->CheckLabel("test_label1", "test_value2"));

  // id with label, empty name
  ASSERT_FALSE(id_with_label_->IsValid());
}

TEST_F(MetricIdTest, CopyTest) {
  // copy id
  MetricId copy_id(*id_with_name_and_label_);
  ASSERT_TRUE(copy_id.IsValid());
  ASSERT_STREQ(copy_id.GetName().c_str(), id_with_name_and_label_->GetName().c_str());
  ASSERT_EQ(copy_id.GetLabelMap().size(), id_with_name_and_label_->GetLabelMap().size());
  ASSERT_STREQ(copy_id.ToString().c_str(), id_with_name_and_label_->ToString().c_str());
  ASSERT_STREQ(copy_id.GetLabel("test_label1").c_str(), "test_value1");
  ASSERT_TRUE(copy_id.ExistLabel("test_label1"));
  ASSERT_TRUE(copy_id.CheckLabel("test_label1", "test_value1"));

  ASSERT_TRUE(copy_id.GetLabel("not_exist_label").empty());
  ASSERT_FALSE(copy_id.ExistLabel("not_exist_label"));
  ASSERT_FALSE(copy_id.CheckLabel("not_exist_label", "test_value1"));
  ASSERT_FALSE(copy_id.CheckLabel("test_label1", "test_value2"));
  ASSERT_TRUE(copy_id == *id_with_name_and_label_);

  // assign id
  MetricId assign_id;
  assign_id = *id_with_name_and_label_;
  ASSERT_TRUE(assign_id.IsValid());
  ASSERT_STREQ(assign_id.GetName().c_str(), id_with_name_and_label_->GetName().c_str());
  ASSERT_EQ(assign_id.GetLabelMap().size(), id_with_name_and_label_->GetLabelMap().size());
  ASSERT_STREQ(assign_id.ToString().c_str(), id_with_name_and_label_->ToString().c_str());
  ASSERT_STREQ(assign_id.GetLabel("test_label1").c_str(), "test_value1");
  ASSERT_TRUE(assign_id.ExistLabel("test_label1"));
  ASSERT_TRUE(assign_id.CheckLabel("test_label1", "test_value1"));

  ASSERT_TRUE(assign_id.GetLabel("not_exist_label").empty());
  ASSERT_FALSE(assign_id.ExistLabel("not_exist_label"));
  ASSERT_FALSE(assign_id.CheckLabel("not_exist_label", "test_value1"));
  ASSERT_FALSE(assign_id.CheckLabel("test_label1", "test_value2"));
  ASSERT_TRUE(assign_id == *id_with_name_and_label_);
}

TEST_F(MetricIdTest, BuildTest) {
  MetricId test_id;
  bool ret = false;

  std::string legal_label_str = LabelStringBuilder()
                                    .Append("test_label1", "test_value1")
                                    .Append("test_label2", "test_value2")
                                    .ToString();
  ASSERT_STREQ(legal_label_str.c_str(), label_str_.c_str());

  ret = MetricId::ParseFromString(kTestMetricName, legal_label_str, &test_id);
  ASSERT_TRUE(ret) << "Parse label string: " << legal_label_str << ", failed" << std::endl;
  ASSERT_TRUE(test_id.IsValid());
  ASSERT_STREQ(test_id.GetName().c_str(), kTestMetricName.c_str());
  ASSERT_EQ(test_id.GetLabelMap().size(), id_with_name_and_label_->GetLabelMap().size());
  std::string expected_id_str = kTestMetricName + kNameLabelsDelimiter + legal_label_str;
  ASSERT_STREQ(test_id.ToString().c_str(), expected_id_str.c_str());

  std::string single_label_str =
      LabelStringBuilder().Append("test_label1", "test_value1").ToString();
  ASSERT_STREQ(single_label_str.c_str(), "test_label1:test_value1");
  ret = MetricId::ParseFromString(kTestMetricName, single_label_str, &test_id);
  ASSERT_TRUE(ret) << "Parse label string: " << single_label_str << ", failed" << std::endl;
  ASSERT_TRUE(test_id.IsValid());
  ASSERT_STREQ(test_id.GetName().c_str(), kTestMetricName.c_str());
  ASSERT_EQ(test_id.GetLabelMap().size(), 1);
  expected_id_str = kTestMetricName + kNameLabelsDelimiter + single_label_str;
  ASSERT_STREQ(test_id.ToString().c_str(), expected_id_str.c_str());

  std::string empty_label_str = LabelStringBuilder().ToString();
  ASSERT_STREQ(empty_label_str.c_str(), "");
  ret = MetricId::ParseFromString(kTestMetricName, empty_label_str, &test_id);
  ASSERT_TRUE(ret);
  ASSERT_TRUE(test_id.IsValid());
  ASSERT_STREQ(test_id.GetName().c_str(), kTestMetricName.c_str());
  ASSERT_TRUE(test_id.GetLabelMap().empty());
  ASSERT_STREQ(test_id.ToString().c_str(), kTestMetricName.c_str());

  std::vector<std::string> illegal_label_str_vec;
  illegal_label_str_vec.push_back("haha:hehe,,,,");
  illegal_label_str_vec.push_back("haha:hehe,hoho");
  illegal_label_str_vec.push_back("haha:hehe,hoho:heihei,");
  illegal_label_str_vec.push_back("haha");
  illegal_label_str_vec.push_back(",lalala");

  for (const std::string &illegal_label : illegal_label_str_vec) {
    ret = MetricId::ParseFromString(kTestMetricName, illegal_label, &test_id);
    ASSERT_FALSE(ret);
  }
}

}  // end namespace tera

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
