// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
#pragma once
#include <string>

#pragma GCC visibility push(default)

namespace tera {
std::string MurmurHash(const std::string& user_key);
}
