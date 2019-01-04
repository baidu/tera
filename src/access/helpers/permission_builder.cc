// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "access/helpers/permission_builder.h"
#include <memory>

namespace tera {
namespace auth {

namespace {
static const std::string empty_str{""};
static bool CheckActionLegal(Permission::Action action) {
  using action_type = std::underlying_type<Permission::Action>::type;
  uint32_t action_num = static_cast<action_type>(action);
  uint32_t action_min = static_cast<action_type>(Permission::kRead);
  uint32_t action_max = static_cast<action_type>(Permission::kAdmin);
  return (action_num >= action_min) && (action_num <= action_max);
}
}

Permission* PermissionBuilder::NewPermission(Permission::Action action) {
  if (!CheckActionLegal(action)) {
    return nullptr;
  }
  std::unique_ptr<Permission> permission(new Permission);
  permission->set_type(Permission::kGlobal);
  permission->mutable_global_permission()->set_action(action);
  return permission.release();
}

Permission* PermissionBuilder::NewPermission(Permission::Action action,
                                             const std::string& namespace_name) {
  if (!CheckActionLegal(action) || !namespace_name.compare(empty_str)) {
    return nullptr;
  }
  std::unique_ptr<Permission> permission(new Permission);
  permission->set_type(Permission::kNamespace);
  NamespacePermission* namespace_permission = permission->mutable_namespace_permission();
  namespace_permission->set_namespace_name(namespace_name);
  namespace_permission->set_action(action);
  return permission.release();
}

Permission* PermissionBuilder::NewPermission(Permission::Action action,
                                             const std::string& namespace_name,
                                             const std::string& table_name) {
  if (!CheckActionLegal(action) || !namespace_name.compare(empty_str) ||
      !table_name.compare(empty_str)) {
    return nullptr;
  }
  std::unique_ptr<Permission> permission(new Permission);
  permission->set_type(Permission::kTable);
  TablePermission* table_permission = permission->mutable_table_permission();
  table_permission->set_namespace_name(namespace_name);
  table_permission->set_table_name(table_name);
  table_permission->set_action(action);
  return permission.release();
}

Permission* PermissionBuilder::NewPermission(Permission::Action action,
                                             const std::string& namespace_name,
                                             const std::string& table_name,
                                             const std::string& family,
                                             const std::string& qualifier) {
  if (!CheckActionLegal(action) || !namespace_name.compare(empty_str) ||
      !table_name.compare(empty_str)) {
    return nullptr;
  }
  std::unique_ptr<Permission> permission(new Permission);
  permission->set_type(Permission::kTable);
  TablePermission* table_permission = permission->mutable_table_permission();
  table_permission->set_namespace_name(namespace_name);
  table_permission->set_table_name(table_name);
  table_permission->set_family(family);
  table_permission->set_qualifier(qualifier);
  table_permission->set_action(action);
  return permission.release();
}
}  // namespace auth
}  // namespace tera
