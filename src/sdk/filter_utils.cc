// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "filter_utils.h"

#include <stdint.h>

#include <sstream>

#include <glog/logging.h>

#include "common/base/string_ext.h"
#include "common/base/string_number.h"
#include "io/coding.h"
#include "sdk/sdk_utils.h"

namespace tera {

bool CheckFilterString(const string& filter_str) {
    // check filter string syntax
    return true;
}

string RemoveInvisibleChar(const string& schema) {
    string ret;
    for (size_t i = 0; i < schema.size(); ++i) {
        if (schema[i] != '\n' && schema[i] != '\t' && schema[i] != ' ') {
            ret.append(1, schema[i]);
        }
    }
    return ret;
}

bool DefaultValueConverter(const string& in, const string& type, string* out) {
    if (out == NULL) {
        LOG(ERROR) << "null ptr: out";
        return false;
    }
    if (type == "int64") {
        int64_t value_int64;
        if (!StringToNumber(in.c_str(), &value_int64)) {
           LOG(ERROR) << "invalid Integer number Got: " << in;
           return false;
        }
        out->assign((char*)&value_int64, sizeof(int64_t));
    } else {
        LOG(ERROR) << "not supported type: " << type;
        return false;
    }
    return true;
}
} // namespace tera
