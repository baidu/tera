// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <iostream>
#include <stdexcept>
#include <string>

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

#include "common/metric/percentile_counter.h"

namespace tera {

class PercentileCounterTest : public ::testing::Test {
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

TEST_F(PercentileCounterTest, RegisterTest) {
  MetricId test_id;
  {
    // with name and labels
    PercentileCounter counter1("counter1", label_str_, 10);
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
    PercentileCounter counter2("counter2", 10);
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
  ASSERT_THROW(PercentileCounter("counter3", "illegal_label_string", 10, {}),
               std::invalid_argument);

  // with empty name
  ASSERT_THROW(PercentileCounter("", label_str_, 10, {}), std::invalid_argument);
  ASSERT_THROW(PercentileCounter("", 10, {}), std::invalid_argument);
}

TEST_F(PercentileCounterTest, CounterTest) {
  PercentileCounter Percent_80("80Percent", label_str_, 80, {SubscriberType::LATEST});
  PercentileCounter Percent_90("90Percent", label_str_, 90, {SubscriberType::LATEST});
  PercentileCounter Percent_99("99Percent", label_str_, 99, {SubscriberType::LATEST});

  for (int i = 1; i <= 100; ++i) {
    Percent_80.Append(i);
    Percent_90.Append(i);
    Percent_99.Append(i);
  }

  EXPECT_EQ(Percent_80.Get(), 81);
  EXPECT_EQ(Percent_90.Get(), 91);
  EXPECT_EQ(Percent_99.Get(), 100);

  // do collect
  CollectorReportPublisher::GetInstance().Refresh();
  auto report = CollectorReportPublisher::GetInstance().GetSubscriberReport();

  EXPECT_EQ(Percent_80.Get(), -1);
  EXPECT_EQ(Percent_90.Get(), -1);
  EXPECT_EQ(Percent_99.Get(), -1);

  EXPECT_TRUE(report->find(MetricId("80Percent", label_str_)) != report->end());
  EXPECT_TRUE(report->find(MetricId("90Percent", label_str_)) != report->end());
  EXPECT_TRUE(report->find(MetricId("99Percent", label_str_)) != report->end());

  EXPECT_EQ(report->find(MetricId("80Percent", label_str_))->second.Value(), 81);
  EXPECT_EQ(report->find(MetricId("90Percent", label_str_))->second.Value(), 91);
  EXPECT_EQ(report->find(MetricId("99Percent", label_str_))->second.Value(), 100);

  for (int i = 1; i <= 10; ++i) {
    Percent_80.Append(i);
    Percent_90.Append(i);
    Percent_99.Append(i);
  }

  EXPECT_EQ(Percent_80.Get(), 9);
  EXPECT_EQ(Percent_90.Get(), 10);
  EXPECT_EQ(Percent_99.Get(), 10);
}

}  // end namespace tera

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
