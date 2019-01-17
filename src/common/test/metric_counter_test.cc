// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

#include "common/metric/metric_counter.h"

namespace tera {

class MetricCounterTest : public ::testing::Test {
 public:
  virtual void SetUp() {
    label_str_ = LabelStringBuilder()
                     .Append("test_label1", "test_value1")
                     .Append("test_label2", "test_value2")
                     .ToString();
  }

  virtual void TearDown() {}

 private:
  std::string label_str_;
};

TEST_F(MetricCounterTest, RegisterTest) {
  MetricId test_id;
  {
    // with name and labels
    MetricCounter counter1("counter1", label_str_);
    test_id = counter1.metric_id_;

    EXPECT_TRUE(CollectorReportPublisher::GetInstance().HasCollector(counter1.metric_id_))
        << "metric_id " << counter1.metric_id_.ToString() << std::endl;
    EXPECT_TRUE(CollectorReportPublisher::GetInstance().HasCollector(test_id))
        << "metric_id " << test_id.ToString() << std::endl;
    EXPECT_TRUE(counter1.IsRegistered());
  }
  EXPECT_FALSE(CollectorReportPublisher::GetInstance().HasCollector(test_id))
      << "metric_id " << test_id.ToString() << std::endl;

  {
    // with name only
    MetricCounter counter2("counter2", {}, true);
    test_id = counter2.metric_id_;

    EXPECT_TRUE(CollectorReportPublisher::GetInstance().HasCollector(counter2.metric_id_))
        << "metric_id " << counter2.metric_id_.ToString() << std::endl;
    EXPECT_TRUE(CollectorReportPublisher::GetInstance().HasCollector(test_id))
        << "metric_id " << test_id.ToString() << std::endl;
    EXPECT_TRUE(counter2.IsRegistered());
  }
  EXPECT_FALSE(CollectorReportPublisher::GetInstance().HasCollector(test_id))
      << "metric_id " << test_id.ToString() << std::endl;

  // with illegal label string
  ASSERT_THROW(MetricCounter("counter3", "illegal_label_string", {}, true), std::invalid_argument);

  // with empty name
  ASSERT_THROW(MetricCounter("", label_str_, {}, true), std::invalid_argument);
  ASSERT_THROW(MetricCounter("", {}, true), std::invalid_argument);
}

TEST_F(MetricCounterTest, CollectTest) {
  MetricCounter periodic_counter("periodic", label_str_, {}, true);
  MetricCounter nonperiodic_counter("nonperiodic", label_str_, {}, false);

  for (size_t i = 0; i < 3; ++i) {
    periodic_counter.Inc();
    nonperiodic_counter.Inc();
  }
  EXPECT_EQ(periodic_counter.Get(), 3);
  EXPECT_EQ(nonperiodic_counter.Get(), 3);

  // do collect
  CollectorReportPublisher::GetInstance().Refresh();

  EXPECT_EQ(periodic_counter.Get(), 0);
  EXPECT_EQ(nonperiodic_counter.Get(), 3);

  periodic_counter.Inc();
  nonperiodic_counter.Inc();
  EXPECT_EQ(periodic_counter.Get(), 1);
  EXPECT_EQ(nonperiodic_counter.Get(), 4);
}

}  // end namespace tera

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
