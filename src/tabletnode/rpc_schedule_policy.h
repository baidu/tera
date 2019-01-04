// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: Li Kang (com@baidu.com)

#ifndef TERA_TABLETNODE_RPC_SCHEDULE_POLICY_H_
#define TERA_TABLETNODE_RPC_SCHEDULE_POLICY_H_

#include <map>
#include <string>

#include "common/base/stdint.h"

namespace tera {
namespace tabletnode {

struct ScheduleEntity {
  void* user_ptr;

  ScheduleEntity(void* user_ptr) : user_ptr(user_ptr) {}
  virtual ~ScheduleEntity() {}
};

class SchedulePolicy {
 public:
  typedef std::string TableName;
  typedef std::map<TableName, ScheduleEntity*> ScheduleEntityList;

  SchedulePolicy() {}
  virtual ~SchedulePolicy() {}

  virtual ScheduleEntity* NewScheEntity(void* user_ptr = NULL) = 0;

  virtual ScheduleEntityList::iterator Pick(ScheduleEntityList* entity_list) = 0;

  virtual void Done(ScheduleEntity* entity) = 0;

  virtual void Enable(ScheduleEntity* entity) = 0;

  virtual void Disable(ScheduleEntity* entity) = 0;
};

struct FairScheduleEntity : public ScheduleEntity {
  bool pickable;
  int64_t last_update_time;
  int64_t elapse_time;
  int64_t running_count;

  FairScheduleEntity(void* user_ptr)
      : ScheduleEntity(user_ptr),
        pickable(false),
        last_update_time(0),
        elapse_time(0),
        running_count(0) {}
};

class FairSchedulePolicy : public SchedulePolicy {
 public:
  FairSchedulePolicy();

  ~FairSchedulePolicy();

  ScheduleEntity* NewScheEntity(void* user_ptr = NULL);

  ScheduleEntityList::iterator Pick(ScheduleEntityList* entity_list);

  void Done(ScheduleEntity* entity);

  void Enable(ScheduleEntity* entity);

  void Disable(ScheduleEntity* entity);

 private:
  void UpdateEntity(FairScheduleEntity* entity);

  int64_t min_elapse_time_;
};

}  // namespace tabletnode
}  // namespace tera

#endif  // TERA_TABLETNODE_RPC_SCHEDULE_POLICY_H_
