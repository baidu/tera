// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "filter_utils.h"

#include <sstream>
#include <limits>
#include <glog/logging.h>

#include <stdint.h>
#include "common/base/string_ext.h"

#include "tera/io/coding.h"
#include "tera/sdk/sdk_utils.h"

namespace tera {

bool CheckFilterString(const string& filter_str) {
    // check filter string syntax
    return true;
}

string RemoveInvisibleChar(const string& schema) {
    string ret;
    for (int i = 0; i < schema.size(); ++i) {
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
    std::istringstream mem(in);
    if (type == "int8") {
        char buffer[sizeof(uint8_t)];
        int8_t temp_data = 0;
        mem >> temp_data;
        uint8_t data = std::numeric_limits<char>::max() + temp_data;
        memcpy(buffer, &data, sizeof(uint8_t));
        out->assign(buffer, sizeof(uint8_t));
    } else if (type == "uint8") {
        char buffer[sizeof(uint8_t)];
        uint8_t data = 0;
        mem >> data;
        memcpy(buffer, &data, sizeof(uint8_t));
        out->assign(buffer, sizeof(uint8_t));
    } else if (type == "int32") {
        char buffer[sizeof(uint32_t)];
        int32_t temp_data = 0;
        mem >> temp_data;
        uint32_t data = std::numeric_limits<int>::max() + temp_data;
        io::EncodeBigEndian32(buffer, data);
        out->assign(buffer, sizeof(uint32_t));
    } else if (type == "uint32") {
        char buffer[sizeof(uint32_t)];
        uint32_t data = 0;
        mem >> data;
        io::EncodeBigEndian32(buffer, data);
        out->assign(buffer, sizeof(uint32_t));
    } else if (type == "int64") {
        char buffer[sizeof(uint64_t)];
        int64_t temp_data = 0;
        mem >> temp_data;
        uint64_t data = std::numeric_limits<long int>::max() + temp_data;
        io::EncodeBigEndian(buffer, data);
        out->assign(buffer, sizeof(uint64_t));
    } else if (type == "uint64") {
        char buffer[sizeof(uint64_t)];
        uint64_t data = 0;
        mem >> data;
        io::EncodeBigEndian(buffer, data);
        out->assign(buffer, sizeof(uint64_t));
    } else if (type == "double") {
        char buffer[sizeof(double)];
        double data = 0.0;
        mem >> data;
        memcpy(buffer, &data, sizeof(double));
        out->assign(buffer, sizeof(double));
    } else if (type == "bool") {
        char buffer[sizeof(bool)];
        bool data = 0;
        mem >> data;
        memcpy(buffer, &data, sizeof(bool));
        out->assign(buffer, sizeof(bool));
    } else if (type == "string" || type == "binary" || type == "") {
        out->assign(in);
    } else {
        LOG(ERROR) << "unknow type: " << type;
        return false;
    }
    return true;
}
} // namespace tera
