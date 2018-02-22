// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <string>

#include "load_balancer/actions.h"

namespace tera {
namespace load_balancer {

EmptyAction::EmptyAction() :
    Action(Action::Type::EMPTY) {
}

EmptyAction::~EmptyAction() {
}

Action* EmptyAction::UndoAction() {
    return new EmptyAction();
}

std::string EmptyAction::ToString() const {
    return "EmptyAction";
}

MoveAction::MoveAction(uint32_t tablet_index, uint32_t source_node_index, uint32_t dest_node_index) :
    Action(Action::Type::MOVE),
    tablet_index_(tablet_index),
    source_node_index_(source_node_index),
    dest_node_index_(dest_node_index) {
}

MoveAction::~MoveAction() {
}

Action* MoveAction::UndoAction() {
    return new MoveAction(tablet_index_, dest_node_index_, source_node_index_);
}

std::string MoveAction::ToString() const {
    return "move " + std::to_string(tablet_index_) + " from "
            + std::to_string(source_node_index_) + " to " + std::to_string(dest_node_index_);
}

} // namespace load_balancer
} // namespace tera
