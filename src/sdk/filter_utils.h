// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  TERA_SDK_FILTER_UTILS_H_
#define  TERA_SDK_FILTER_UTILS_H_

#include "proto/tabletnode_rpc.pb.h"

using std::string;

namespace tera {

bool CheckFilterString(const string& filter_str);

string RemoveInvisibleChar(const string& schema);

bool DefaultValueConverter(const string& in, const string& type, string* out);
} // namespace tera
#endif // TERA_SDK_FILTER_UTILS_H_
