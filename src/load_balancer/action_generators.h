// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_LOAD_BALANCER_ACTION_GENERATORS_H_
#define TERA_LOAD_BALANCER_ACTION_GENERATORS_H_

#include <memory>

#include "load_balancer/action_generator.h"
#include "load_balancer/actions.h"

namespace tera {
namespace load_balancer {

// move a random tablet of a random node to another random node
class RandomActionGenerator : public ActionGenerator {
public:
    RandomActionGenerator();
    virtual ~RandomActionGenerator();

    // generate a random move action
    virtual Action* Generate(const std::shared_ptr<Cluster>& cluster) override;

    virtual std::string Name() override;

private:
    std::string name_;
};

// move a tablet
// from the node holding most tablets
// to the node holding least tablets
class TabletCountActionGenerator : public ActionGenerator {
public:
    TabletCountActionGenerator();
    virtual ~TabletCountActionGenerator();

    virtual Action* Generate(const std::shared_ptr<Cluster>& cluster) override;

    virtual std::string Name() override;

private:
    uint32_t PickMostTabletsNode(const std::shared_ptr<Cluster>& cluster);
    uint32_t PickLeastTabletsNode(const std::shared_ptr<Cluster>& cluster);

private:
    std::string name_;
};

// move a tablet
// from the node holding largest data size
// to the node holding smallest data size
class SizeActionGenerator : public ActionGenerator {
public:
    SizeActionGenerator();
    virtual ~SizeActionGenerator();

    virtual Action* Generate(const std::shared_ptr<Cluster>& cluster) override;

    virtual std::string Name() override;

private:
    uint32_t PickLargestSizeNode(const std::shared_ptr<Cluster>& cluster);
    uint32_t PickSmallestSizeNode(const std::shared_ptr<Cluster>& cluster);

private:
    std::string name_;
};

// move a tablet
// from the node has most read load
// to the node has least read load
class ReadLoadActionGenerator : public ActionGenerator {
public:
    ReadLoadActionGenerator();
    virtual ~ReadLoadActionGenerator();

    virtual Action* Generate(const std::shared_ptr<Cluster>& cluster) override;

    virtual std::string Name() override;

private:
    uint32_t PickMostReadNode(const std::shared_ptr<Cluster>& cluster);
    uint32_t PickLeastReadNode(const std::shared_ptr<Cluster>& cluster);

private:
    std::string name_;
};

// move a tablet
// from the node has most write load
// to the node has least write load
class WriteLoadActionGenerator : public ActionGenerator {
public:
    WriteLoadActionGenerator();
    virtual ~WriteLoadActionGenerator();

    virtual Action* Generate(const std::shared_ptr<Cluster>& cluster) override;

    virtual std::string Name() override;

private:
    uint32_t PickMostWriteNode(const std::shared_ptr<Cluster>& cluster);
    uint32_t PickLeastWriteNode(const std::shared_ptr<Cluster>& cluster);

private:
    std::string name_;
};

// move a tablet
// from the node has most scan load
// to the node has least scan load
class ScanLoadActionGenerator : public ActionGenerator {
public:
    ScanLoadActionGenerator();
    virtual ~ScanLoadActionGenerator();

    virtual Action* Generate(const std::shared_ptr<Cluster>& cluster) override;

    virtual std::string Name() override;

private:
    uint32_t PickMostScanNode(const std::shared_ptr<Cluster>& cluster);
    uint32_t PickLeastScanNode(const std::shared_ptr<Cluster>& cluster);

private:
    std::string name_;
};

} // namespace load_balancer
} // namespace tera

#endif // TERA_LOAD_BALANCER_ACTION_GENERATORS_H_
