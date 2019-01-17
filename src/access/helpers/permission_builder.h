// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include "proto/access_control.pb.h"
#include <string>

namespace tera {
namespace auth {

class PermissionBuilder {
 public:
  static Permission* NewPermission(Permission::Action action);
  static Permission* NewPermission(Permission::Action action, const std::string& namespace_name);
  static Permission* NewPermission(Permission::Action action, const std::string& namespace_name,
                                   const std::string& table_name);
  static Permission* NewPermission(Permission::Action action, const std::string& namespace_name,
                                   const std::string& table_name, const std::string& family,
                                   const std::string& qualifier);
};

}  // namespace auth
}  // namespace tera