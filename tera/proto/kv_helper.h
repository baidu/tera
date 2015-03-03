// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  TERA_PROTO_KV_HELPER_H
#define  TERA_PROTO_KV_HELPER_H

#include <string>

#include "tera/proto/table_meta.pb.h"

namespace tera {

// table meta record
void MakeMetaTableKeyValue(const TableMeta& meta, std::string* key,
                           std::string* value);
void MakeMetaTableKey(const std::string& table_name, std::string* key);
void MakeMetaTableValue(const TableMeta& meta, std::string* value);

void ParseMetaTableKeyValue(const std::string& key, const std::string& value,
                            TableMeta* meta);
void ParseMetaTableKey(const std::string& key, std::string* table_name);
void ParseMetaTableValue(const std::string& value, TableMeta* meta);

// tablet meta record
void MakeMetaTableKeyValue(const TabletMeta& meta, std::string* key,
                           std::string* value);
void MakeMetaTableKey(const std::string& table_name,
                      const std::string& key_start, std::string* key);
void MakeMetaTableValue(const TabletMeta& meta, std::string* value);

void ParseMetaTableKeyValue(const std::string& key, const std::string& value,
                            TabletMeta* meta);
void ParseMetaTableKey(const std::string& key, std::string* table_name,
                       std::string* key_start);
void ParseMetaTableValue(const std::string& value, TabletMeta* meta);

void MetaTableScanRange(const std::string& table_name, std::string* key_start,
                        std::string* key_end);
void MetaTableScanRange(const std::string& table_name,
                        const std::string& tablet_key_start,
                        const std::string& tablet_key_end,
                        std::string* key_start, std::string* key_end);
void MetaTableListScanRange(std::string* key_start, std::string* key_end);
std::string NextKey(const std::string& key);

} // namespace tera

#endif  //TERA_PROTO_KV_HELPER_H

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
