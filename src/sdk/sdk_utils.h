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

bool SetCfProperties(const string& name, const string& value,
                     ColumnFamilyDescriptor* desc);
bool SetLgProperties(const string& name, const string& value,
                     LocalityGroupDescriptor* desc);
bool SetTableProperties(const string& name, const string& value,
                        TableDescriptor* desc);

bool FillTableDescriptor(PropTree& schema_tree, TableDescriptor* desc);
bool UpdateTableDescriptor(PropTree& schema_tree, TableDescriptor* table_desc, bool* is_update_lg_cf);

bool ParseTableSchema(const string& schema, TableDescriptor* table_desc);
bool ParseTableSchemaFile(const string& file, TableDescriptor* table_desc);

bool ParseScanSchema(const string& schema, ScanDescriptor* desc);

typedef std::pair<string, string> Property;
typedef std::vector<Property> PropertyList;

bool BuildSchema(TableDescriptor* table_desc, string* schema);
} // namespace tera
#endif // TERA_SDK_SDK_UTILS_H_
