// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_LOAD_BALANCER_COST_FUNCTIONS_H_
#define TERA_LOAD_BALANCER_COST_FUNCTIONS_H_

#include "load_balancer/cost_function.h"

namespace tera {
namespace load_balancer {

// moving too many tablets will cost high
class MoveCountCostFunction : public CostFunction {
public:
    MoveCountCostFunction(const LBOptions& options);
    virtual ~MoveCountCostFunction();

    virtual double Cost() override;

private:
    const double kExpensiveCost;
    uint32_t tablet_max_move_num_;
    double tablet_max_move_percent_;
};

// moving tablet oo frequently will cost high
class MoveFrequencyCostFunction : public CostFunction {
public:
    MoveFrequencyCostFunction(const LBOptions& options);
    virtual ~MoveFrequencyCostFunction();

    virtual double Cost() override;

private:
    const double kExpensiveCost;
};

// moving a tablet to an abnormal node will cost high
class AbnormalNodeCostFunction : public CostFunction {
public:
    AbnormalNodeCostFunction(const LBOptions& options);
    virtual ~AbnormalNodeCostFunction();

    virtual double Cost() override;

private:
    const double kExpensiveCost;
};

// moving a tablet to a read pending node will cost high
class ReadPendingNodeCostFunction : public CostFunction {
public:
    ReadPendingNodeCostFunction(const LBOptions& options);
    virtual ~ReadPendingNodeCostFunction();

    virtual double Cost() override;

private:
    const double kExpensiveCost;
};

// moving a tablet to a write pending node will cost high
class WritePendingNodeCostFunction : public CostFunction {
public:
    WritePendingNodeCostFunction(const LBOptions& options);
    virtual ~WritePendingNodeCostFunction();

    virtual double Cost() override;

private:
    const double kExpensiveCost;
};

// moving a tablet to a scan pending node will cost high
class ScanPendingNodeCostFunction : public CostFunction {
public:
    ScanPendingNodeCostFunction(const LBOptions& options);
    virtual ~ScanPendingNodeCostFunction();

    virtual double Cost() override;

private:
    const double kExpensiveCost;
};

// balance the tablets num for each tablet node
class TabletCountCostFunction : public CostFunction {
public:
    TabletCountCostFunction(const LBOptions& options);
    virtual ~TabletCountCostFunction();

    virtual double Cost() override;
};

// banlance the data size for each tablet node
class SizeCostFunction : public CostFunction {
public:
    SizeCostFunction(const LBOptions& options);
    virtual ~SizeCostFunction();

    virtual double Cost() override;
};

// banlance the read load for each tablet node
class ReadLoadCostFunction : public CostFunction {
public:
    ReadLoadCostFunction(const LBOptions& options);
    virtual ~ReadLoadCostFunction();

    virtual double Cost() override;
};

// banlance the write load for each tablet node
class WriteLoadCostFunction : public CostFunction {
public:
    WriteLoadCostFunction(const LBOptions& options);
    virtual ~WriteLoadCostFunction();

    virtual double Cost() override;
};

// banlance the scan load for each tablet node
class ScanLoadCostFunction : public CostFunction {
public:
    ScanLoadCostFunction(const LBOptions& options);
    virtual ~ScanLoadCostFunction();

    virtual double Cost() override;
};

} // namespace load_balancer
} // namespace tera

#endif // TERA_LOAD_BALANCER_COST_FUNCTIONS_H_
