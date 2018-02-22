// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_LOAD_BALANCER_ACTIONS_H_
#define TERA_LOAD_BALANCER_ACTIONS_H_

#include <string>

#include "load_balancer/action.h"

namespace tera {
namespace load_balancer {

class EmptyAction : public Action {
public:
    EmptyAction();
    virtual ~EmptyAction();

    virtual Action* UndoAction() override;

    virtual std::string ToString() const override;
};

class MoveAction : public Action {
public:
    MoveAction(uint32_t tablet_index, uint32_t source_node_index, uint32_t dest_node_index);
    virtual ~MoveAction();

    virtual Action* UndoAction() override;

    virtual std::string ToString() const override;

public:
    uint32_t tablet_index_;
    uint32_t source_node_index_;
    uint32_t dest_node_index_;
};

} // namespace load_balancer
} // namespace tera

#endif // TERA_LOAD_BALANCER_ACTIONS_H_
