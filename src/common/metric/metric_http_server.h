// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_COMMON_METRIC_METRIC_HTTP_SERVER_H_
#define TERA_COMMON_METRIC_METRIC_HTTP_SERVER_H_
 
#include <atomic>
#include <string> 
#include <vector>
 
#include "mongoose.h" 

#include "common/metric/collector_report_publisher.h"
#include "common/mutex.h" 
#include "common/thread.h"
 
namespace tera {

struct ResponseBodyBuilder {
    static void BuildType(std::string* body, 
                                 const std::string& metric_name, 
                                 const std::string& type);

    static void BuildHelp(std::string* body, 
                                 const std::string& metric_name, 
                                 const std::string& help_info);

    static void BuildMetricItem(std::string* body, 
                                       const MetricId& metric_id, 
                                       const ReportItem& report_item);
};

// a simple http server based on mongoose
class MetricHttpServer {
public:
    MetricHttpServer();
    ~MetricHttpServer();
    
private:
    // disallow copy
    MetricHttpServer(const MetricHttpServer&) = delete;
    MetricHttpServer& operator = (const MetricHttpServer&) = delete;

private:    
    static void EventHandler(struct mg_connection *conn, int event, void *p_data);
    
public:
    bool Start(int32_t listen_port);
    void Stop();
    
    bool IsRunning() const {
        return is_running_.load();
    }
    
private:
    void BackgroundWorkWrapper();
    
    // http request handlers
    void HandleHttpRequest(struct mg_connection *conn, struct http_message *request);
    void HandleMetrics(struct mg_connection *conn, struct http_message *request);
    void HandleUnknowUri(struct mg_connection *conn, struct http_message *request);
    
    // prometheus handle functions
    std::string GetResponseBody();

private:
    mutable Mutex mutex_;
    std::atomic<bool> is_running_;
    std::atomic<bool> stop_;
    int32_t listen_port_;
    
    // background thread
    common::Thread bg_thread_;
    
    // mongoose info
    struct mg_mgr mongoose_mgr_;
}; 
 
} // end namespace tera 
 
#endif // TERA_COMMON_METRIC_METRIC_HTTP_SERVER_H_
 
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
