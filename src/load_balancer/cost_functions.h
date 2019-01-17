// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_LOAD_BALANCER_COST_FUNCTIONS_H_
#define TERA_LOAD_BALANCER_COST_FUNCTIONS_H_

#include "load_balancer/cost_function.h"

namespace tera {
namespace load_balancer {

class MoveCountCostFunction : public CostFunction {
 public:
  MoveCountCostFunction(const LBOptions& options);
  virtual ~MoveCountCostFunction();

  virtual double Cost() override;

 private:
  const double kExpensiveCost;
  uint32_t tablet_max_move_num_;
};

class TabletCountCostFunction : public CostFunction {
 public:
  TabletCountCostFunction(const LBOptions& options);
  virtual ~TabletCountCostFunction();

  virtual double Cost() override;
};

class SizeCostFunction : public CostFunction {
 public:
  SizeCostFunction(const LBOptions& options);
  virtual ~SizeCostFunction();

  virtual double Cost() override;
};

class FlashSizeCostFunction : public CostFunction {
 public:
  FlashSizeCostFunction(const LBOptions& options);
  virtual ~FlashSizeCostFunction();

  virtual double Cost() override;
};

class ReadLoadCostFunction : public CostFunction {
 public:
  ReadLoadCostFunction(const LBOptions& options);
  virtual ~ReadLoadCostFunction();

  virtual double Cost() override;
};

class WriteLoadCostFunction : public CostFunction {
 public:
  WriteLoadCostFunction(const LBOptions& options);
  virtual ~WriteLoadCostFunction();

  virtual double Cost() override;
};

class ScanLoadCostFunction : public CostFunction {
 public:
  ScanLoadCostFunction(const LBOptions& options);
  virtual ~ScanLoadCostFunction();

  virtual double Cost() override;
};

class LReadCostFunction : public CostFunction {
 public:
  LReadCostFunction(const LBOptions& options);
  virtual ~LReadCostFunction();

  virtual double Cost() override;
};

}  // namespace load_balancer
}  // namespace tera

#endif  // TERA_LOAD_BALANCER_COST_FUNCTIONS_H_
