// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <memory>
#include <string>
#include "access/helpers/permission_builder.h"

DECLARE_string(tera_auth_name);
DECLARE_string(tera_auth_token);

namespace tera {
namespace auth {
namespace test {

static const std::string namespace_name{"sandbox"};
static const std::string table_name{"test"};
static const std::string family{"cf"};
static const std::string qualifier{"qu"};

class PermissionTest : public ::testing::Test {
 public:
  PermissionTest() {}
  virtual ~PermissionTest() {}
};

TEST_F(PermissionTest, GlobalPermissionBuilderTest) {
  std::unique_ptr<Permission> global_permission(
      PermissionBuilder::NewPermission(Permission::kRead));
  EXPECT_TRUE(global_permission->type() == Permission::kGlobal);

  std::unique_ptr<Permission> nullptr_permission(
      PermissionBuilder::NewPermission(static_cast<Permission::Action>(5)));
  EXPECT_TRUE(!nullptr_permission);
}

TEST_F(PermissionTest, NamespacePermissionBuilderTest) {
  std::unique_ptr<Permission> namespace_permission(
      PermissionBuilder::NewPermission(Permission::kRead, namespace_name));
  EXPECT_TRUE(namespace_permission->type() == Permission::kNamespace);
  EXPECT_TRUE(namespace_permission->namespace_permission().namespace_name() == namespace_name);
  std::unique_ptr<Permission> nullptr_permission(
      PermissionBuilder::NewPermission(static_cast<Permission::Action>(5), namespace_name));
  EXPECT_TRUE(!nullptr_permission);
}

TEST_F(PermissionTest, TablePermissionBuilderTest) {
  std::unique_ptr<Permission> table_permission(
      PermissionBuilder::NewPermission(Permission::kRead, namespace_name, table_name));
  EXPECT_TRUE(table_permission->type() == Permission::kTable);
  EXPECT_TRUE(table_permission->table_permission().namespace_name() == namespace_name);
  EXPECT_TRUE(table_permission->table_permission().table_name() == table_name);

  std::unique_ptr<Permission> nullptr_permission(PermissionBuilder::NewPermission(
      static_cast<Permission::Action>(5), namespace_name, table_name));
  EXPECT_TRUE(!nullptr_permission);
}

TEST_F(PermissionTest, TableCfQuPermissionBuilderTest) {
  std::unique_ptr<Permission> table_cf_qu_permission(PermissionBuilder::NewPermission(
      Permission::kRead, namespace_name, table_name, family, qualifier));
  EXPECT_TRUE(table_cf_qu_permission->type() == Permission::kTable);
  EXPECT_TRUE(table_cf_qu_permission->table_permission().namespace_name() == namespace_name);
  EXPECT_TRUE(table_cf_qu_permission->table_permission().table_name() == table_name);
  EXPECT_TRUE(table_cf_qu_permission->table_permission().family() == family);
  EXPECT_TRUE(table_cf_qu_permission->table_permission().qualifier() == qualifier);

  std::unique_ptr<Permission> nullptr_permission(PermissionBuilder::NewPermission(
      static_cast<Permission::Action>(5), namespace_name, table_name, family, qualifier));
  EXPECT_TRUE(!nullptr_permission);
}
}
}
}