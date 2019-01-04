// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <iostream>
#include <string>
#include <vector>

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

#include "common/metric/metric_counter.h"
#include "common/metric/hardware_collectors.h"
#include "common/metric/collector_report_publisher.h"
#include "common/this_thread.h"

DECLARE_int64(tera_hardware_collect_period_second);

namespace tera {

class MetricsTest : public ::testing::Test {
 public:
  virtual void SetUp() {
    // shorter period for test
    FLAGS_tera_hardware_collect_period_second = 1;
    CollectorReportPublisher::GetInstance().AddHardwareCollectors();

    label_map_["test_label1"] = "test_value1";
    label_map_["test_label2"] = "test_value2";
  }

  virtual void TearDown() {
    CollectorReportPublisher::GetInstance().collectors_.clear();
    label_map_.clear();
  }

 private:
  MetricLabels label_map_;
};

static void PrintCollectorReportPublisher() {
  std::cout << "Print Metric Registry: " << std::endl;
  auto& metric_map = CollectorReportPublisher::GetInstance().collectors_;
  auto metric_iter = metric_map.begin();
  for (; metric_iter != metric_map.end(); ++metric_iter) {
    std::cout << metric_iter->first.ToString() << std::endl;
  }
}

TEST_F(MetricsTest, RegisterTest) {
  // hardware metrics
  ASSERT_TRUE(CollectorReportPublisher::GetInstance().HasCollector(MetricId(kInstCpuMetricName)));
  ASSERT_TRUE(CollectorReportPublisher::GetInstance().HasCollector(MetricId(kInstMemMetricName)));
  ASSERT_TRUE(CollectorReportPublisher::GetInstance().HasCollector(MetricId(kInstNetRXMetricName)));
  ASSERT_TRUE(CollectorReportPublisher::GetInstance().HasCollector(MetricId(kInstNetTXMetricName)));

  bool ret = false;
  Counter* test_counters = new Counter[5];
  // register a counter
  MetricId test_id_1("test_counter", label_map_);
  ret = CollectorReportPublisher::GetInstance().AddCollector(
      test_id_1, std::unique_ptr<Collector>(new CounterCollector(&test_counters[0])));
  EXPECT_TRUE(ret);
  EXPECT_TRUE(CollectorReportPublisher::GetInstance().HasCollector(test_id_1));
  PrintCollectorReportPublisher();

  // register a counter with different name
  MetricId test_id_2("test_counter_2", label_map_);
  ret = CollectorReportPublisher::GetInstance().AddCollector(
      test_id_2, std::unique_ptr<Collector>(new CounterCollector(&test_counters[0])));
  EXPECT_TRUE(ret);
  EXPECT_TRUE(CollectorReportPublisher::GetInstance().HasCollector(test_id_2));
  PrintCollectorReportPublisher();

  // register a counter with name only
  ret = CollectorReportPublisher::GetInstance().AddCollector(
      MetricId("test_counter3"),
      std::unique_ptr<Collector>(new CounterCollector(&test_counters[2])));
  EXPECT_TRUE(ret);
  EXPECT_TRUE(CollectorReportPublisher::GetInstance().HasCollector(MetricId("test_counter3")));
  PrintCollectorReportPublisher();

  // register a counter with same name and different labels
  label_map_["test_label2"] = "other_label_value";
  MetricId test_id_4("test_counter", label_map_);
  ret = CollectorReportPublisher::GetInstance().AddCollector(
      test_id_4, std::unique_ptr<Collector>(new CounterCollector(&test_counters[3])));
  EXPECT_TRUE(ret);
  EXPECT_TRUE(CollectorReportPublisher::GetInstance().HasCollector(test_id_4));
  PrintCollectorReportPublisher();

  // register a counter with same id
  ret = CollectorReportPublisher::GetInstance().AddCollector(
      test_id_1, std::unique_ptr<Collector>(new CounterCollector(&test_counters[4])));
  EXPECT_FALSE(ret);
  EXPECT_TRUE(CollectorReportPublisher::GetInstance().HasCollector(test_id_1));
  PrintCollectorReportPublisher();

  ret = CollectorReportPublisher::GetInstance().AddCollector(
      MetricId("test_counter3"),
      std::unique_ptr<Collector>(new CounterCollector(&test_counters[4])));
  EXPECT_FALSE(ret);
  EXPECT_TRUE(CollectorReportPublisher::GetInstance().HasCollector(MetricId("test_counter3")));
  PrintCollectorReportPublisher();

  // unregister
  ret = CollectorReportPublisher::GetInstance().DeleteCollector(test_id_1);
  EXPECT_TRUE(ret);
  EXPECT_FALSE(CollectorReportPublisher::GetInstance().HasCollector(test_id_1));
  EXPECT_TRUE(CollectorReportPublisher::GetInstance().HasCollector(test_id_2));

  ret = CollectorReportPublisher::GetInstance().DeleteCollector(MetricId("test_counter3"));
  EXPECT_TRUE(ret);
  EXPECT_FALSE(CollectorReportPublisher::GetInstance().HasCollector(MetricId("test_counter3")));
  EXPECT_TRUE(CollectorReportPublisher::GetInstance().HasCollector(test_id_2));

  MetricId not_registered_id("not_registered_name", label_map_);
  ret = CollectorReportPublisher::GetInstance().DeleteCollector(not_registered_id);
  EXPECT_FALSE(ret);

  label_map_["test_label2"] = "not_registered_value";
  MetricId not_registered_id_2("test_counter", label_map_);
  ret = CollectorReportPublisher::GetInstance().DeleteCollector(not_registered_id_2);
  EXPECT_FALSE(ret);

  ret = CollectorReportPublisher::GetInstance().DeleteCollector(MetricId("not_registered_name"));
  EXPECT_FALSE(ret);

  delete[] test_counters;
}

TEST_F(MetricsTest, ReportTest) {
  // check report cache
  int64_t value = 0;

  // register 2 counter
  std::string label_str = LabelStringBuilder()
                              .Append("test_label1", "test_value1")
                              .Append("test_label2", "test_value2")
                              .ToString();
  MetricCounter periodic_counter("periodic", label_str, {}, true);
  MetricCounter nonperiodic_counter("nonperiodic", label_str, {}, false);

  for (size_t i = 0; i < 3; ++i) {
    periodic_counter.Inc();
    nonperiodic_counter.Inc();
  }
  EXPECT_EQ(periodic_counter.Get(), 3);
  EXPECT_EQ(nonperiodic_counter.Get(), 3);

  // do collect
  ThisThread::Sleep(10);

  CollectorReportPublisher::GetInstance().Refresh();
  std::shared_ptr<CollectorReport> report =
      CollectorReportPublisher::GetInstance().GetCollectorReport();

  EXPECT_EQ(periodic_counter.Get(), 0);
  EXPECT_EQ(nonperiodic_counter.Get(), 3);

  // check report
  EXPECT_EQ(report->report.size(), CollectorReportPublisher::GetInstance().collectors_.size());
  value = report->FindMetricValue("periodic", label_str);
  EXPECT_EQ(value, 3);
  value = report->FindMetricValue("nonperiodic", label_str);
  EXPECT_EQ(value, 3);

  // change counter value
  periodic_counter.Inc();
  nonperiodic_counter.Dec();
  EXPECT_EQ(periodic_counter.Get(), 1);
  EXPECT_EQ(nonperiodic_counter.Get(), 2);

  // report again
  CollectorReportPublisher::GetInstance().Refresh();
  report = CollectorReportPublisher::GetInstance().GetCollectorReport();
  EXPECT_EQ(periodic_counter.Get(), 0);
  EXPECT_EQ(nonperiodic_counter.Get(), 2);

  value = report->FindMetricValue("periodic", label_str);
  EXPECT_EQ(value, 1);
  value = report->FindMetricValue("nonperiodic", label_str);
  EXPECT_EQ(value, 2);
}

}  // end namespace tera

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
