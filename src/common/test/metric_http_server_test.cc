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
#include "common/metric/metric_http_server.h" 
#include "common/metric/collector_report.h" 
#include "common/base/string_ext.h" 

namespace tera { 
 
class MetricHttpServerTest : public ::testing::Test { 
public:
    virtual void SetUp() {
        // register metrics
        test_counter = new MetricCounter("counter", {SubscriberType::LATEST});
        server = new MetricHttpServer;
        test_counter->Set(1);
    }
    virtual void TearDown() {
        delete test_counter;
        delete server;
    }

private:
    MetricCounter* test_counter;
    MetricHttpServer* server;
}; 
 
TEST_F(MetricHttpServerTest, BuildType) { 
    std::string body;
    ResponseBodyBuilder::BuildType(&body, "good", "gauge");
    EXPECT_STREQ(body.c_str(), "# TYPE good gauge\n");
    ResponseBodyBuilder::BuildType(&body, "bad", "summary");
    EXPECT_STREQ(body.c_str(), "# TYPE good gauge\n"
                               "# TYPE bad summary\n");
}

TEST_F(MetricHttpServerTest, BuildHelp) { 
    std::string body;
    ResponseBodyBuilder::BuildHelp(&body, "good", "good");
    EXPECT_STREQ(body.c_str(), "# HELP good good\n");
    ResponseBodyBuilder::BuildHelp(&body, "bad", "bad");
    EXPECT_STREQ(body.c_str(), "# HELP good good\n"
                               "# HELP bad bad\n");
}

TEST_F(MetricHttpServerTest, BuildMetricItem) { 
    CollectorReportPublisher::GetInstance().Refresh();
    auto report = CollectorReportPublisher::GetInstance().GetSubscriberReport();

    std::string body;
    int64_t time_stamp;

    for (const auto& item : *report) {
        if (item.first.GetName() == "counter") {
            ResponseBodyBuilder::BuildMetricItem(&body, item.first, item.second);
            time_stamp = item.second.Time();
        }
    }
    std::string expect_body = "counter{value_type=\"Latest\"} 1 " + 
                              std::to_string(time_stamp) + "\n";

    EXPECT_EQ(body, expect_body);
    EXPECT_EQ(test_counter->Get(), 0);
    test_counter->Set(2);

    CollectorReportPublisher::GetInstance().Refresh();
    report = CollectorReportPublisher::GetInstance().GetSubscriberReport();

    for (const auto& item : *report) {
        if (item.first.GetName() == "counter") {
            ResponseBodyBuilder::BuildMetricItem(&body, item.first, item.second);
            time_stamp = item.second.Time();
        }
    }

    expect_body += "counter{value_type=\"Latest\"} 2 " + 
                   std::to_string(time_stamp) + "\n";

    EXPECT_EQ(body, expect_body);
}

TEST_F(MetricHttpServerTest, GetResponseBody) { 
    CollectorReportPublisher::GetInstance().Refresh();
    int64_t timestamp = CollectorReportPublisher::GetInstance().GetCollectorReport()->timestamp_ms;
    std::string body = server->GetResponseBody();
    std::vector<std::string> splited_string;
    SplitString(body, "\n", &splited_string);
    bool find_counter = false;
    for (int idx = 0; idx != splited_string.size(); ++ idx) {
        if (splited_string[idx].substr(0, 8) == "counter{") {
            find_counter = true;
            EXPECT_STREQ(splited_string[idx - 2].c_str(), 
                           "# HELP counter counter");
            EXPECT_STREQ(splited_string[idx - 1].c_str(), 
                           "# TYPE counter gauge");
            std::string expected_line = "counter{value_type=\"Latest\"} 1 " + std::to_string(timestamp);
            EXPECT_EQ(expected_line, splited_string[idx]);
        }
    }
    EXPECT_TRUE(find_counter);
    EXPECT_EQ(test_counter->Get(), 0);
    test_counter->Set(19);
    find_counter = false;

    CollectorReportPublisher::GetInstance().Refresh();
    timestamp = CollectorReportPublisher::GetInstance().GetCollectorReport()->timestamp_ms;
    body = server->GetResponseBody();
    splited_string.clear();
    SplitString(body, "\n", &splited_string);
    for (int idx = 0; idx != splited_string.size(); ++ idx) {
        if (splited_string[idx].substr(0, 8) == "counter{") {
            find_counter = true;
            EXPECT_STREQ(splited_string[idx - 2].c_str(), 
                           "# HELP counter counter");
            EXPECT_STREQ(splited_string[idx - 1].c_str(), 
                           "# TYPE counter gauge");
            std::string expected_line = "counter{value_type=\"Latest\"} 19 " + std::to_string(timestamp);
            EXPECT_EQ(expected_line, splited_string[idx]);
        }
    }

    EXPECT_TRUE(find_counter);
    EXPECT_EQ(test_counter->Get(), 0);
}
} // end namespace tera
 
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */

