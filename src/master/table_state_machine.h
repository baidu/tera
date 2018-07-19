// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include <iostream>
#include <fstream>
#include "proto/status_code.pb.h"
#include "master/state_machine.h"
#include "master/tablet_state_machine.h"

namespace tera {
namespace master {

enum class TableEvent {
    kEnableTable,
    kDisableTable,
    kDeleteTable,
};

std::ostream& operator<< (std::ostream& o, const TableEvent event);

class TableStateMachine {
public:
    TableStateMachine(TableStatus init_status) : curr_status_(init_status) {}
    ~TableStateMachine() {}

    bool DoStateTransition(const TableEvent event) {
        TableStatus post_status;
        if (state_transitions_.DoStateTransition(curr_status_, event, &post_status)) {
            curr_status_ = post_status;
            return true;
        }
        return false;
    };
    
    TableStatus GetStatus() { return curr_status_; }
    void SetStatus(TableStatus status) { curr_status_ = status; }

    typedef StateTransitionRules<TableStatus, TableEvent> TableStateTransitionRulesType;
private:
    TableStatus curr_status_;
    const static TableStateTransitionRulesType state_transitions_;

};
}
}

