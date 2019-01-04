// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "common/metric/metric_id.h"

#include <boost/algorithm/string.hpp>
#include <sstream>

namespace tera {

static const std::string kInvalidLabel = "";

MetricId::MetricId(const std::string& name, const std::string& label_str) {
  ParseFromStringWithThrow(name, label_str, this);
}

static std::string MetricLabelsToString(const MetricLabels& label_map) {
  if (label_map.empty()) {
    return "";
  }
  std::ostringstream label_oss;
  auto iter = label_map.begin();
  // do not append kLabelPairDelimiter for the first pair
  label_oss << iter->first << kLabelKVDelimiter << iter->second;
  ++iter;

  for (; iter != label_map.end(); ++iter) {
    label_oss << kLabelPairDelimiter << iter->first << kLabelKVDelimiter << iter->second;
  }
  return label_oss.str();
}

std::string MetricId::GenMetricIdStr(const std::string& name, const MetricLabels& label_map) {
  if (label_map.empty()) {
    return name;
  }

  std::ostringstream id_oss;
  id_oss << name << kNameLabelsDelimiter << MetricLabelsToString(label_map);
  return id_oss.str();
}

void MetricId::ParseFromStringWithThrow(const std::string& name, const std::string& label_str,
                                        MetricId* metric_id) throw(std::invalid_argument) {
  if (metric_id == NULL) {
    throw std::invalid_argument("metric_id is invalid");
  }
  if (name.empty()) {
    throw std::invalid_argument("metric name is invalid");
  }

  metric_id->name_ = name;
  metric_id->labels_.clear();

  if (label_str.empty()) {
    metric_id->id_str_ = metric_id->name_;
    return;
  }

  // label_str format: k1:v1,k2:v2,...
  std::vector<std::string> label_str_splits;
  boost::algorithm::split(label_str_splits, label_str,
                          boost::algorithm::is_any_of(kLabelPairDelimiter));
  for (const std::string& label_kv_str : label_str_splits) {
    std::vector<std::string> label_kv_splits;
    boost::algorithm::split(label_kv_splits, label_kv_str,
                            boost::algorithm::is_any_of(kLabelKVDelimiter));
    if (label_kv_splits.size() != 2) {
      // invalid label str format
      throw std::invalid_argument("label_str");
    }

    metric_id->labels_.insert(std::make_pair(label_kv_splits[0], label_kv_splits[1]));
  }

  // gen identifier string
  metric_id->id_str_ = metric_id->name_ + kNameLabelsDelimiter + label_str;
  return;
}

bool MetricId::ParseFromString(const std::string& name, const std::string& label_str,
                               MetricId* metric_id) throw() {
  try {
    ParseFromStringWithThrow(name, label_str, metric_id);
    return true;
  } catch (std::invalid_argument&) {
    return false;
  }
}

MetricId::MetricId() : name_(), labels_(), id_str_() {}

MetricId::MetricId(const std::string& name)
    : name_(name), labels_(), id_str_(GenMetricIdStr(name_, labels_)) {}

MetricId::MetricId(const std::string& name, const MetricLabels& label_map)
    : name_(name), labels_(label_map), id_str_(GenMetricIdStr(name_, labels_)) {}

MetricId::MetricId(const MetricId& other)
    : name_(other.name_), labels_(other.labels_), id_str_(other.id_str_) {}

MetricId::~MetricId() {}

MetricId& MetricId::operator=(const MetricId& other) {
  name_ = other.name_;
  labels_ = other.labels_;
  id_str_ = other.id_str_;
  return *this;
}

const std::string& MetricId::GetLabel(const std::string& name) const {
  auto iter = labels_.find(name);
  if (iter == labels_.end()) {
    return kInvalidLabel;
  } else {
    return iter->second;
  }
}

bool MetricId::ExistLabel(const std::string& name) const {
  return labels_.find(name) != labels_.end();
}

bool MetricId::CheckLabel(const std::string& name, const std::string& expected_value) const {
  auto iter = labels_.find(name);
  if (iter == labels_.end()) {
    return false;
  } else {
    return (iter->second == expected_value);
  }
}

LabelStringBuilder& LabelStringBuilder::Append(const std::string& name, const std::string& value) {
  if (!name.empty() && !value.empty()) {
    labels_[name] = value;
  }
  return *this;
}

std::string LabelStringBuilder::ToString() const { return MetricLabelsToString(labels_); }

}  // end namespace tera

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
