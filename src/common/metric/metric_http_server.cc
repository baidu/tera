// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "common/metric/metric_http_server.h"

#include <unordered_map>
#include <algorithm>
#include <string>

#include "glog/logging.h"

#include "common/timer.h"
#include "common/metric/collector_report.h"

using std::string;

namespace tera {

void ResponseBodyBuilder::BuildType(string* body, const string& metric_name, const string& type) {
  body->append("# TYPE " + metric_name + " " + type + "\n");
}

void ResponseBodyBuilder::BuildHelp(string* body, const string& metric_name,
                                    const string& help_info) {
  body->append("# HELP " + metric_name + " " + help_info + "\n");
}

void ResponseBodyBuilder::BuildMetricItem(string* body, const MetricId& metric_id,
                                          const ReportItem& report_item) {
  VLOG(12) << "[Building Metric] name: " << metric_id.GetName()
           << "\tValue: " << static_cast<double>(report_item.Value())
           << "\tTimeStamp: " << report_item.Time() << "\tType: " << report_item.Type();

  if (report_item.Time() == -1) {
    return;
  }

  body->append(metric_id.GetName() + "{");
  const auto& label_map = metric_id.GetLabelMap();
  auto iter = label_map.begin();
  bool has_label = false;
  if (iter != label_map.end()) {
    body->append(iter->first + "=" + "\"" + iter->second + "\"");
    has_label = true;
    ++iter;
  }
  while (iter != label_map.end()) {
    body->append("," + iter->first + "=" + "\"" + iter->second + "\"");
    ++iter;
  }

  if (has_label) {
    body->append(",value_type=\"" + report_item.Type() + "\"");
  } else {
    body->append("value_type=\"" + report_item.Type() + "\"");
  }

  body->append("} " + std::to_string(report_item.Value()) + " " +
               std::to_string(report_item.Time()));
  body->append("\n");
}

static const int kMongoosePollTimeoutMs = 1000;

static void LogRequest(struct http_message* request) {
  VLOG(16) << "[MetricHttpServer] Recv http request."
           << " method [" << std::string(request->method.p, request->method.len) << "]"
           << " uri [" << std::string(request->uri.p, request->uri.len) << "]"
           << " proto [" << std::string(request->proto.p, request->proto.len) << "]"
           << " query [" << std::string(request->query_string.p, request->query_string.len) << "]"
           << " body [" << std::string(request->body.p, request->body.len) << "]";
}

void MetricHttpServer::EventHandler(struct mg_connection* conn, int event, void* p_data) {
  if (event == MG_EV_HTTP_REQUEST) {
    if (conn == NULL || conn->mgr == NULL || p_data == NULL) {
      LOG(WARNING) << "[MetricHttpServer] handle invalid request.";
      return;
    }

    // get user data
    void* user_data = conn->mgr->user_data;
    if (user_data == NULL) {
      LOG(WARNING) << "[MetricHttpServer] Connection missing user data.";
      return;
    }

    MetricHttpServer* server = reinterpret_cast<MetricHttpServer*>(user_data);
    struct http_message* request = reinterpret_cast<struct http_message*>(p_data);
    server->HandleHttpRequest(conn, request);
  }
  // ignore other events
}

MetricHttpServer::MetricHttpServer() : is_running_(false), stop_(false), listen_port_(-1) {}

MetricHttpServer::~MetricHttpServer() { Stop(); }

bool MetricHttpServer::Start(int32_t listen_port) {
  if (listen_port <= 0) {
    LOG(WARNING) << "[MetricHttpServer] Start got invalid listen port: " << listen_port;
    return false;
  }

  MutexLock lock(&mutex_);
  if (IsRunning()) {
    LOG(WARNING) << "[MetricHttpServer] Server is already running, listening: " << listen_port_;
    return false;
  }

  // init mongoose use this as user_data
  mg_mgr_init(&mongoose_mgr_, this);

  // bind listen port
  std::string bind_addr = std::to_string(listen_port);
  struct mg_connection* conn =
      mg_bind(&mongoose_mgr_, bind_addr.c_str(), &MetricHttpServer::EventHandler);

  if (conn == NULL) {
    LOG(WARNING) << "[MetricHttpServer] Bind port [" << listen_port << "] failed.";
    mg_mgr_free(&mongoose_mgr_);
    return false;
  }

  mg_set_protocol_http_websocket(conn);
  LOG(INFO) << "[MetricHttpServer] Bind port [" << listen_port << "] success.";

  stop_.store(false);
  bg_thread_ = std::thread{&MetricHttpServer::BackgroundWorkWrapper, this};
  return true;
}

void MetricHttpServer::Stop() {
  MutexLock lock(&mutex_);
  if (!IsRunning()) {
    return;
  }

  stop_.store(true);
  bg_thread_.join();
  listen_port_ = -1;
}

void MetricHttpServer::BackgroundWorkWrapper() {
  LOG(INFO) << "[MetricHttpServer] Start background work";
  is_running_.store(true);
  while (!stop_.load()) {
    mg_mgr_poll(&mongoose_mgr_, kMongoosePollTimeoutMs);
  }
  is_running_.store(false);
  mg_mgr_free(&mongoose_mgr_);
  LOG(INFO) << "[MetricHttpServer] Exit background work";
}

void MetricHttpServer::HandleHttpRequest(struct mg_connection* conn, struct http_message* request) {
  int64_t start_ts = get_micros();
  LogRequest(request);

  // select real handler based on uri
  std::string uri(request->uri.p, request->uri.len);
  if (uri == "/metrics") {
    HandleMetrics(conn, request);
  } else {
    HandleUnknowUri(conn, request);
  }
  int64_t end_ts = get_micros();
  VLOG(16) << "[MetricHttpServer] Handle uri [" << uri << "] cost [" << (end_ts - start_ts)
           << "] us.";
}

void MetricHttpServer::HandleUnknowUri(struct mg_connection* conn, struct http_message* request) {
  VLOG(16) << "[MetricHttpServer] Handle unknow uri ["
           << std::string(request->uri.p, request->uri.len) << "] ...";
  mg_send_head(conn, 404, 0, "Content-Type: text/plain");
}

void MetricHttpServer::HandleMetrics(struct mg_connection* conn, struct http_message* request) {
  std::string body(GetResponseBody());
  mg_printf(conn, "HTTP/1.1 200 OK\r\nContent-Type: %s\r\n", "text/plain");
  mg_printf(conn, "Content-Length: %lu\r\n\r\n", static_cast<unsigned long>(body.size()));
  mg_send(conn, body.data(), body.size());
}

string MetricHttpServer::GetResponseBody() {
  int64_t start_ts = get_millis();
  std::shared_ptr<SubscriberReport> cur_report =
      CollectorReportPublisher::GetInstance().GetSubscriberReport();

  if (!cur_report) {
    LOG(WARNING) << "[MetricHttpServer] Subscriber Report Is Empty";
    return "";
  }

  // pair<MetricId, TimeValueQueue>
  using MetricIdValuePair = SubscriberReport::value_type;
  // Vector of pair<MetricId, TimeValueQueue>
  using MetricIdValueVec = std::vector<const MetricIdValuePair*>;
  // MetricNameMap: map< metric_name, vector< pair<metric_id, value> > >
  using MetricNameMap = std::unordered_map<std::string, MetricIdValueVec>;

  MetricNameMap metric_name_map;

  for (const auto& report_item : *cur_report) {
    const std::string& metric_name = report_item.first.GetName();
    metric_name_map[metric_name].push_back(&report_item);
  }

  std::string body;
  // fill MetricFamilyVec
  for (const auto& metric_item : metric_name_map) {
    ResponseBodyBuilder::BuildHelp(&body, metric_item.first, metric_item.first);
    ResponseBodyBuilder::BuildType(&body, metric_item.first, "gauge");

    const MetricIdValueVec& metric_vec = metric_item.second;

    std::for_each(metric_vec.begin(), metric_vec.end(), [&body, this](const MetricIdValuePair* x) {
      ResponseBodyBuilder::BuildMetricItem(&body, x->first, x->second);
    });
  }
  VLOG(12) << "[MetricHttpServer] Get Response Body cost: " << get_millis() - start_ts << " ms";
  return std::move(body);
}
}  // end namespace tera

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
