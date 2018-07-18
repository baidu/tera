// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "table_state_machine.h"

namespace tera {
namespace master {

static TableStateMachine::TableStateTransitionRulesType s_table_transition_rules;

const TableStateMachine::TableStateTransitionRulesType TableStateMachine::state_transitions_(
    std::move(
        s_table_transition_rules
        .AddTransitionRule(kTableEnable,    TableEvent::kDisableTable,      kTableDisable)
        .AddTransitionRule(kTableDisable,   TableEvent::kEnableTable,       kTableEnable)
        .AddTransitionRule(kTableDisable,   TableEvent::kDeleteTable,       kTableDeleting)
        .AddTransitionRule(kTableDeleting,  TableEvent::kDisableTable,      kTableDisable)
    ));

std::ostream& operator<< (std::ostream& o, const TableEvent event) {
    static const char* msg[] = {"TableEvent::kEnableTable", 
                                "TableEvent::kDisableTable", 
                                "TableEvent::kDeleteTable", 
                                "TableEvent::kUnknown"};
    static uint32_t msg_size = sizeof(msg) / sizeof(const char*);
    typedef std::underlying_type<TableEvent>::type UnderType;
    uint32_t index = static_cast<UnderType>(event) - static_cast<UnderType>(TableEvent::kEnableTable);
    index = index < msg_size ? index : msg_size - 1;
    o << msg[index];
    return o;
}

}
}

