// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "common/timer.h"
#include "master/tablet_manager.h"
#include "tablet_state_machine.h"

namespace tera {
namespace master {

static TabletStateMachine::TabletStateTransitionRulesType s_tablet_transition_rules;

const TabletStateMachine::TabletStateTransitionRulesType TabletStateMachine::state_transitions_(
    std::move(s_tablet_transition_rules
                  // state transition rules from kTableOffLine
                  .AddTransitionRule(TabletMeta::kTabletOffline, TabletEvent::kLoadTablet,
                                     TabletMeta::kTabletLoading)
                  .AddTransitionRule(TabletMeta::kTabletOffline, TabletEvent::kTsDelayOffline,
                                     TabletMeta::kTabletDelayOffline)
                  .AddTransitionRule(TabletMeta::kTabletOffline, TabletEvent::kTabletLoadFail,
                                     TabletMeta::kTabletLoadFail)
                  .AddTransitionRule(TabletMeta::kTabletOffline, TabletEvent::kTsOffline,
                                     TabletMeta::kTabletOffline)
                  .AddTransitionRule(TabletMeta::kTabletOffline, TabletEvent::kTableDisable,
                                     TabletMeta::kTabletDisable)
                  .AddTransitionRule(TabletMeta::kTabletOffline, TabletEvent::kFinishSplitTablet,
                                     TabletMeta::kTabletSplitted)
                  .AddTransitionRule(TabletMeta::kTabletOffline, TabletEvent::kFinishMergeTablet,
                                     TabletMeta::kTabletMerged)
                  // state transition rules from kTabletPending
                  .AddTransitionRule(TabletMeta::kTabletDelayOffline, TabletEvent::kTsOffline,
                                     TabletMeta::kTabletOffline)
                  .AddTransitionRule(TabletMeta::kTabletDelayOffline, TabletEvent::kTsRestart,
                                     TabletMeta::kTabletOffline)
                  // state transition rules from kTableOnLoad
                  .AddTransitionRule(TabletMeta::kTabletLoading, TabletEvent::kTsLoadSucc,
                                     TabletMeta::kTabletReady)
                  .AddTransitionRule(TabletMeta::kTabletLoading, TabletEvent::kTsLoadFail,
                                     TabletMeta::kTabletLoadFail)
                  .AddTransitionRule(TabletMeta::kTabletLoading, TabletEvent::kTsDelayOffline,
                                     TabletMeta::kTabletDelayOffline)
                  .AddTransitionRule(TabletMeta::kTabletLoading, TabletEvent::kTsOffline,
                                     TabletMeta::kTabletOffline)
                  .AddTransitionRule(TabletMeta::kTabletLoading, TabletEvent::kTsRestart,
                                     TabletMeta::kTabletOffline)
                  // state transition rules from kTableUnloading
                  .AddTransitionRule(TabletMeta::kTabletUnloading, TabletEvent::kTsUnLoadSucc,
                                     TabletMeta::kTabletOffline)
                  .AddTransitionRule(TabletMeta::kTabletUnloading, TabletEvent::kTsUnLoadFail,
                                     TabletMeta::kTabletUnloadFail)
                  .AddTransitionRule(TabletMeta::kTabletUnloading, TabletEvent::kTsOffline,
                                     TabletMeta::kTabletOffline)
                  // state transition rules from kTableReady
                  .AddTransitionRule(TabletMeta::kTabletReady, TabletEvent::kUnLoadTablet,
                                     TabletMeta::kTabletUnloading)
                  .AddTransitionRule(TabletMeta::kTabletReady, TabletEvent::kTsOffline,
                                     TabletMeta::kTabletOffline)
                  .AddTransitionRule(TabletMeta::kTabletReady, TabletEvent::kTsDelayOffline,
                                     TabletMeta::kTabletDelayOffline)
                  // state transition rules from kTableUnLoadFail
                  .AddTransitionRule(TabletMeta::kTabletUnloadFail, TabletEvent::kTsOffline,
                                     TabletMeta::kTabletOffline)
                  // state transition rules from kTableLoadFail
                  .AddTransitionRule(TabletMeta::kTabletLoadFail, TabletEvent::kLoadTablet,
                                     TabletMeta::kTabletLoading)
                  .AddTransitionRule(TabletMeta::kTabletLoadFail, TabletEvent::kTsOffline,
                                     TabletMeta::kTabletOffline)
                  .AddTransitionRule(TabletMeta::kTabletLoadFail, TabletEvent::kTableDisable,
                                     TabletMeta::kTabletDisable)
                  // state transition rules from kTabletDisable
                  .AddTransitionRule(TabletMeta::kTabletDisable, TabletEvent::kTableEnable,
                                     TabletMeta::kTabletOffline)));

TabletStateMachine::TabletStateMachine(const TabletMeta::TabletStatus init_state)
    : curr_state_(init_state), ready_time_(std::numeric_limits<int64_t>::max()) {}

bool TabletStateMachine::DoStateTransition(const TabletEvent& event) {
  TabletMeta::TabletStatus post_state;
  if (!state_transitions_.DoStateTransition(curr_state_, event, &post_state)) {
    return false;
  }
  curr_state_ = post_state;
  ready_time_ = (curr_state_ == TabletMeta::kTabletReady ? get_micros()
                                                         : std::numeric_limits<int64_t>::max());
  return true;
}

void TabletStateMachine::SetStatus(const TabletMeta::TabletStatus state) {
  curr_state_ = state;
  ready_time_ = (curr_state_ == TabletMeta::kTabletReady ? get_micros()
                                                         : std::numeric_limits<int64_t>::max());
}

TabletMeta::TabletStatus TabletStateMachine::GetStatus() const { return curr_state_; }

int64_t TabletStateMachine::ReadyTime() { return ready_time_; }

std::ostream& operator<<(std::ostream& o, const TabletEvent& event) {
  static const char* msg[] = {"TabletEvent::kLoadTablet",        "TabletEvent::kUnLoadTablet",
                              "TabletEvent::kFinishSplitTablet", "TabletEvent::kFinishMergeTablet",
                              "TabletEvent::kUpdateMeta",        "TabletEvent::kTsLoadSucc",
                              "TabletEvent::kTsLoadFail",        "TabletEvent::kTsLoadBusy",
                              "TabletEvent::kTsUnloadBusy",      "TabletEvent::kTsUnLoadSucc",
                              "TabletEvent::kTsUnLoadFail",      "TabletEvent::kTsOffline",
                              "TabletEvent::kTsRestart",         "TabletEvent::kWaitRpcResponse",
                              "TabletEvent::kTabletLoadFail",    "TabletEvent::kTsDelayOffline",
                              "TabletEvent::kTableDisable",      "TabletEvent::kTableEnable",
                              "TabletEvent::kEofEvent",          "TabletEvent::kUnknown"};
  static uint32_t msg_size = sizeof(msg) / sizeof(const char*);
  typedef std::underlying_type<TabletEvent>::type UnderType;
  uint32_t index = static_cast<UnderType>(event) - static_cast<UnderType>(TabletEvent::kLoadTablet);
  index = index < msg_size ? index : (msg_size - 1);
  o << msg[index];
  return o;
}
}
}
