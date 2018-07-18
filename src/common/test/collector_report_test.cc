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
#include "common/metric/collector_report.h"
#include "common/this_thread.h" 
 
namespace tera { 
 
class CollectorReportTest : public ::testing::Test {
public:
    CollectorReportTest() 
        : nonperiod_counter1_label(LabelStringBuilder().Append("key1", "value1").ToString()),
          nonperiod_counter1("counter1", nonperiod_counter1_label, {}, false),
          nonperiod_counter2("counter2", {}, false), 
          period_counter1_label(LabelStringBuilder().Append("key2", "value2").ToString()),
          period_counter1("counter1", period_counter1_label, {}, true),
          period_counter3("counter3", {}, true) {
        other_whatever_ids.push_back(MetricId());
        other_whatever_ids.push_back(MetricId("whatevername"));
        
        MetricLabels whatever_labels;
        whatever_labels["haha"] = "hehe";
        whatever_labels["heihei"] = "hoho";
        other_whatever_ids.push_back(MetricId("", whatever_labels));
        other_whatever_ids.push_back(MetricId("whatevername", whatever_labels));
    }

    virtual void SetUp() {
        nonperiod_counter1.Set(1);
        nonperiod_counter2.Set(2);
        period_counter1.Set(3);
        period_counter3.Set(4);
    }
    
    virtual void TearDown() {
        // reset cache to initial status
        CollectorReportPublisher::GetInstance().last_collector_report_.reset(new CollectorReport());
    }
private:
    std::string nonperiod_counter1_label;
    MetricCounter nonperiod_counter1;
    MetricCounter nonperiod_counter2;
    std::string period_counter1_label;
    MetricCounter period_counter1;
    MetricCounter period_counter3;
    
    std::vector<MetricId> other_whatever_ids;
};  

TEST_F(CollectorReportTest, FindTest) {
    int64_t value = 0;
    CollectorReportPublisher::GetInstance().Refresh();
    std::shared_ptr<CollectorReport> report = CollectorReportPublisher::GetInstance().GetCollectorReport();
    
    // check report
    EXPECT_EQ(report->report.size(), CollectorReportPublisher::GetInstance().collectors_.size());
    
    // nonperiod_counter1
    value = report->FindMetricValue("counter1", nonperiod_counter1_label);
    EXPECT_EQ(value, 1);
    value = report->FindMetricValue(nonperiod_counter1.metric_id_);
    EXPECT_EQ(value, 1);
    value = report->FindMetricValue("counter1");
    EXPECT_EQ(value, 0);
    value = report->FindMetricValue("counter1", "other not exist label");
    EXPECT_EQ(value, 0);
    value = report->FindMetricValue("not exist name", nonperiod_counter1_label);
    EXPECT_EQ(value, 0);
    value = report->FindMetricValue(MetricId("counter1"));
    EXPECT_EQ(value, 0);
    
    // nonperiod_counter2
    value = report->FindMetricValue("counter2");
    EXPECT_EQ(value, 2);
    value = report->FindMetricValue("counter2", "");
    EXPECT_EQ(value, 2);
    value = report->FindMetricValue(MetricId("counter2"));
    EXPECT_EQ(value, 2);
    value = report->FindMetricValue("counter2", "whatever_label");
    EXPECT_EQ(value, 0);
    
    // period_counter1
    value = report->FindMetricValue("counter1", period_counter1_label);
    EXPECT_EQ(value, 3);
    value = report->FindMetricValue(period_counter1.metric_id_);
    EXPECT_EQ(value, 3);
    
    // period_counter3
    value = report->FindMetricValue("counter3");
    EXPECT_EQ(value, 4);
    value = report->FindMetricValue(period_counter3.metric_id_);
    EXPECT_EQ(value, 4);
    
    // invalid 
    for (const MetricId& not_exist_id : other_whatever_ids) {
        value = report->FindMetricValue(not_exist_id.GetName());
        EXPECT_EQ(value, 0);
        value = report->FindMetricValue(not_exist_id.ToString());
        EXPECT_EQ(value, 0);
        value = report->FindMetricValue(not_exist_id);
        EXPECT_EQ(value, 0);
    }
    
    // report again
    nonperiod_counter1.Inc();
    nonperiod_counter2.Inc();
    period_counter1.Inc();
    period_counter3.Inc();
    MetricCounter another_counter1("another1");
    MetricCounter another_counter2("another2");
    another_counter1.Inc();
    CollectorReportPublisher::GetInstance().Refresh();
    report = CollectorReportPublisher::GetInstance().GetCollectorReport();
    EXPECT_EQ(report->report.size(), CollectorReportPublisher::GetInstance().collectors_.size());
    
    value = report->FindMetricValue(nonperiod_counter1.metric_id_);
    EXPECT_EQ(value, 2);
    value = report->FindMetricValue(nonperiod_counter2.metric_id_);
    EXPECT_EQ(value, 3);
    value = report->FindMetricValue(period_counter1.metric_id_);
    EXPECT_EQ(value, 1);
    value = report->FindMetricValue(period_counter3.metric_id_);
    EXPECT_EQ(value, 1);
    value = report->FindMetricValue(another_counter1.metric_id_);
    EXPECT_EQ(value, 1);
    value = report->FindMetricValue(another_counter2.metric_id_);
    EXPECT_EQ(value, 0);
}

TEST_F(CollectorReportTest, CacheTest) {
    // do not update yet
    std::shared_ptr<CollectorReport> initial_report = CollectorReportPublisher::GetInstance().GetCollectorReport();
    EXPECT_TRUE(initial_report.get() != NULL);
    EXPECT_TRUE(initial_report->report.empty());
    
    // update
    CollectorReportPublisher::GetInstance().Refresh();
    std::shared_ptr<CollectorReport> report1 = CollectorReportPublisher::GetInstance().GetCollectorReport();
    EXPECT_EQ(report1->report.size(), CollectorReportPublisher::GetInstance().collectors_.size());
    EXPECT_TRUE(report1.get() == CollectorReportPublisher::GetInstance().last_collector_report_.get());
    
    // modify counters and report again
    nonperiod_counter1.Inc();
    nonperiod_counter2.Inc();
    period_counter1.Inc();
    period_counter3.Inc();
    MetricCounter another_counter1("another1");
    MetricCounter another_counter2("another2");
    another_counter1.Inc();
    
    // get report before update, return same ptr
    std::shared_ptr<CollectorReport> report2 = CollectorReportPublisher::GetInstance().GetCollectorReport();
    EXPECT_TRUE(report2.get() == CollectorReportPublisher::GetInstance().last_collector_report_.get());
    EXPECT_TRUE(report2.get() == report1.get());
    EXPECT_EQ(report2->FindMetricValue(period_counter3.metric_id_), 4);
    
    // update and get
    CollectorReportPublisher::GetInstance().Refresh();
    std::shared_ptr<CollectorReport> report3 = CollectorReportPublisher::GetInstance().GetCollectorReport();
    EXPECT_TRUE(report3.get() == CollectorReportPublisher::GetInstance().last_collector_report_.get());
    EXPECT_FALSE(report3.get() == report1.get());
    EXPECT_EQ(report3->report.size(), report2->report.size() + 2);
    EXPECT_EQ(report3->FindMetricValue(period_counter3.metric_id_), 1);
}
 
} // end namespace tera
 
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */

