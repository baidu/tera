// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once
#include <map>

namespace tera {
namespace master {

template <typename StateType, typename EventType>
class StateTransitionRules {
 public:
  StateTransitionRules(){};
  ~StateTransitionRules(){};
  StateTransitionRules(const StateTransitionRules&) = delete;
  StateTransitionRules& operator=(const StateTransitionRules&) = delete;

  StateTransitionRules(StateTransitionRules&& transitions) noexcept {
    transition_rules_.swap(transitions.transition_rules_);
  }

  StateTransitionRules& operator=(StateTransitionRules&& transitions) {
    if (this != &transitions) {
      transition_rules_.swap(transitions.transition_rules_);
    }
    return *this;
  }
  // add a new transition rule representing object's state transfer from
  // "curr_state" to "dest_state" driven by "event"
  StateTransitionRules& AddTransitionRule(const StateType& curr_state, const EventType& event,
                                          const StateType& dest_state) {
    transition_rules_[curr_state][event] = dest_state;
    return *this;
  }

  // return true and the associated post_state if there is a valid transition
  // rule for <curr_state, event>
  // else return false
  bool DoStateTransition(const StateType& curr_state, const EventType& event,
                         StateType* post_state) const;

 private:
  // this map is used to save all transition rules
  // key represent PrevState, value of type std::map<EventType, SateType>
  // represent all events supported by PreState
  // and the associated PostState PreState will transfered to driven by event
  std::map<StateType, std::map<EventType, StateType>> transition_rules_;
};

template <typename StateType, typename EventType>
bool StateTransitionRules<StateType, EventType>::DoStateTransition(const StateType& curr_state,
                                                                   const EventType& event,
                                                                   StateType* post_state) const {
  auto transition_rule = transition_rules_.find(curr_state);
  if (transition_rule == transition_rules_.end()) {
    return false;
  }
  auto transition = transition_rule->second.find(event);
  if (transition == transition_rule->second.end()) {
    return false;
  }
  *post_state = transition->second;
  return true;
}
}
}
