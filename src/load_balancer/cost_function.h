// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_LOAD_BALANCER_COST_FUNCTION_H_
#define TERA_LOAD_BALANCER_COST_FUNCTION_H_

#include <math.h>

#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <memory>
#include <string>

#include "glog/logging.h"
#include "load_balancer/cluster.h"
#include "load_balancer/options.h"

namespace tera {
namespace load_balancer {

class CostFunction {
 public:
  CostFunction(const LBOptions& options, const std::string& name)
      : lb_options_(options), name_(name) {}

  virtual ~CostFunction() {}

  virtual double Cost() = 0;

  virtual void Init(const std::shared_ptr<Cluster>& cluster) { cluster_ = cluster; }

  double GetWeight() const { return weight_; }

  void SetWeight(double w) { weight_ = w; }

  std::string Name() const { return name_; }

 protected:
  double Scale(double min, double max, double value) {
    VLOG(20) << "[lb] Scale begin, min:" << min << " max:" << max << " value:" << value;
    if (max <= min || value <= min) {
      return 0.0;
    }
    if (max - min == 0) {
      return 0.0;
    }

    double scaled = std::max(0.0, std::min(1.0, (value - min) / (max - min)));
    VLOG(20) << "[lb] Scale end, scaled:" << scaled;
    return scaled;
  }

  double ScaleFromArray(const std::vector<double>& stats) {
    if (lb_options_.debug_mode_enabled) {
      std::string line;
      for (const auto& s : stats) {
        line += std::to_string(s);
        line += " ";
      }
      LOG(INFO) << "[lb] stats:" << line;
    }

    double total_cost = 0;
    double total = GetSum(stats);

    double count = stats.size();
    double mean = total / count;

    double max = ((count - 1) * mean) + (total - mean);

    double min;
    if (count > total) {
      min = ((count - total) * mean) + ((1 - mean) * total);
    } else {
      int num_high = (int)(total - (floor(mean) * count));
      int num_low = (int)(count - num_high);

      min = (num_high * (ceil(mean) - mean)) + (num_low * (mean - floor(mean)));
    }
    min = std::max(0.0, min);
    for (size_t i = 0; i < stats.size(); i++) {
      double n = stats[i];
      double diff = std::abs(mean - n);
      total_cost += diff;
    }

    return Scale(min, max, total_cost);
  }

 private:
  double GetSum(const std::vector<double>& stats) {
    double total = 0;
    for (const auto& s : stats) {
      total += s;
    }
    return total;
  }

 protected:
  std::shared_ptr<Cluster> cluster_;

 private:
  double weight_;
  LBOptions lb_options_;
  std::string name_;
};

}  // namespace load_balancer
}  // namespace tera

#endif  // TERA_LOAD_BALANCER_COST_FUNCTION_H_
