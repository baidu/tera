// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_LOAD_BALANCER_UNITY_BALANCER_H_
#define TERA_LOAD_BALANCER_UNITY_BALANCER_H_

#include <memory>
#include <vector>

#include "load_balancer/action_generators.h"
#include "load_balancer/actions.h"
#include "load_balancer/balancer.h"
#include "load_balancer/cluster.h"
#include "load_balancer/cost_functions.h"

namespace tera {
namespace load_balancer {

class UnityBalancer : public Balancer {
public:
    explicit UnityBalancer(const LBOptions& options);
    virtual ~UnityBalancer();

    virtual bool BalanceCluster(
            const std::vector<std::shared_ptr<LBTabletNode>>& lb_nodes,
            std::vector<Plan>* plans) override;

    // if table_name is empty, balance whole culster,
    // otherwhise balance the specified table of table_name
    virtual bool BalanceCluster(
            const std::string& table_name,
            const std::vector<std::shared_ptr<LBTabletNode>>& lb_nodes,
            std::vector<Plan>* plans) override;

    virtual bool NeedBalance(const std::shared_ptr<Cluster>& cluster);

protected:
    virtual void InitCostFunctions(const std::shared_ptr<Cluster>& cluster);

    virtual double ComputeCost(double previous_cost);

    virtual Action* NextAction(const std::shared_ptr<Cluster>& cluster);

    // diff the initial cluster state with the current cluster state, then create plans
    virtual void CreatePlans(const std::shared_ptr<Cluster>& cluster, std::vector<Plan>* plans);

private:
    std::vector<std::shared_ptr<CostFunction>> cost_functions_;
    std::vector<std::shared_ptr<ActionGenerator>> action_generators_;

    LBOptions lb_options_;
};

} // namespace load_balancer
} // namespace tera

#endif // TERA_LOAD_BALANCER_UNITY_BALANCER_H_
