// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "proto/kv_helper.h"

#include "proto/proto_helper.h"

namespace tera {

// TABLE record

void MakeMetaTableKeyValue(const TableMeta& meta, std::string* key,
                           std::string* value) {
    const std::string& table_name = meta.table_name();
    MakeMetaTableKey(table_name, key);
    MakeMetaTableValue(meta, value);
}

void MakeMetaTableKey(const std::string& table_name, std::string* key) {
    if (NULL != key) {
        *key = "@" + table_name;
    }
}

void MakeMetaTableValue(const TableMeta& meta, std::string* value) {
    if (NULL != value) {
        meta.SerializeToString(value);
    }
}

void ParseMetaTableKeyValue(const std::string& key, const std::string& value,
                            TableMeta* meta) {
    ParseMetaTableValue(value, meta);
}

void ParseMetaTableKey(const std::string& key, std::string* table_name) {
    if (key.size() < 2 || key[0] != '@') {
        return;
    }
    size_t pos = key.find('@', 1);
    if (pos != std::string::npos) {
        return;
    }
    if (NULL != table_name) {
        table_name->assign(key, 1, std::string::npos);
    }
}

void ParseMetaTableValue(const std::string& value, TableMeta* meta) {
    if (NULL != meta) {
        meta->ParseFromString(value);
    }
}

// TABLET record

void MakeMetaTableKeyValue(const TabletMeta& meta, std::string* key,
                           std::string* value) {
    const std::string& table_name = meta.table_name();
    const std::string& key_start = meta.key_range().key_start();
    MakeMetaTableKey(table_name, key_start, key);
    MakeMetaTableValue(meta, value);
}

void MakeMetaTableKey(const std::string& table_name,
                      const std::string& key_start,
                      std::string* key) {
    if (NULL != key) {
        *key = table_name + "#" + key_start;
    }
}

void MakeMetaTableValue(const TabletMeta& meta, std::string* value) {
    if (NULL != value) {
        meta.SerializeToString(value);
    }
}

void ParseMetaTableKeyValue(const std::string& key, const std::string& value,
                            TabletMeta* meta) {
    ParseMetaTableValue(value, meta);
}

void ParseMetaTableKey(const std::string& key, std::string* table_name,
                       std::string* key_start) {
    size_t pos = key.find('#');
    if (NULL != table_name) {
        table_name->assign(key, 0, pos);
    }
    if (NULL != key_start) {
        if (pos != std::string::npos && pos + 1 < key.size()) {
            key_start->assign(key, pos + 1, std::string::npos);
        } else {
            key_start->clear();
        }
    }
}

void ParseMetaTableValue(const std::string& value, TabletMeta* meta) {
    if (NULL != meta) {
        meta->ParseFromString(value);
    }
}

void MetaTableScanRange(const std::string& table_name,
                        std::string* key_start, std::string* key_end) {
    if (NULL != key_start) {
        *key_start = table_name + "#";
    }
    if (NULL != key_end) {
        *key_end = table_name + "$";
    }
}

std::string NextKey(const std::string& key) {
    //return key + "\0";
    std::string next = key;
    next.push_back('\0');
    return next;
}

void MetaTableScanRange(const std::string& table_name,
                        const std::string& tablet_key_start,
                        const std::string& tablet_key_end,
                        std::string* key_start, std::string* key_end) {
    if (NULL != key_start) {
        *key_start = table_name + "#" + tablet_key_start;
    }
    if (NULL != key_end) {
        if (tablet_key_end.empty()) {
            *key_end = table_name + "$";
        } else {
            *key_end = table_name + "#" + tablet_key_end;
        }
    }
}

void MetaTableListScanRange(std::string* key_start, std::string* key_end) {
    key_start->assign("@");
    key_end->assign("@~");
}

} // namespace tera

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
