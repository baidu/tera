// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: Xu Peilin (xupeilin@baidu.com)

#ifndef  TERA_SDK_SDK_UTILS_H_
#define  TERA_SDK_SDK_UTILS_H_

#include "proto/table_meta.pb.h"
#include "sdk/tera.h"
#include "utils/prop_tree.h"

using std::string;

namespace tera {

void ShowTableSchema(const TableSchema& schema, bool is_x = false);

void ShowTableMeta(const TableMeta& meta);

void ShowTableDescriptor(TableDescriptor& table_desc, bool is_x = false);

void TableDescToSchema(const TableDescriptor& desc, TableSchema* schema);

void TableSchemaToDesc(const TableSchema& schema, TableDescriptor* desc);

bool ParseSchemaSetTableDescriptor(const string& schema, TableDescriptor* desc, bool* update_lg_cf);

bool SetCfProperties(const string& name, const string& value,
                     ColumnFamilyDescriptor* desc);
bool SetLgProperties(const string& name, const string& value,
                     LocalityGroupDescriptor* desc);
bool SetTableProperties(const string& name, const string& value,
                        TableDescriptor* desc);

bool FillTableDescriptor(PropTree& schema_tree, TableDescriptor* desc);

bool ParseSchema(const string& schema, TableDescriptor* table_desc);

bool ParseScanSchema(const string& schema, ScanDescriptor* desc);

typedef std::pair<string, string> Property;
typedef std::vector<Property> PropertyList;

bool BuildSchema(TableDescriptor* table_desc, string* schema);

bool HasInvalidCharInSchema(const string& schema);

bool ParsePrefixPropertyValue(const string& pair, string& prefix, string& property, string& value);

string PrefixType(const std::string& property);

} // namespace tera
#endif // TERA_SDK_SDK_UTILS_H_
