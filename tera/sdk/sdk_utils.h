// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: Xu Peilin (xupeilin@baidu.com)

#ifndef  TERA_SDK_UTILS_H_
#define  TERA_SDK_UTILS_H_

#include "tera/sdk/tera.h"
#include "tera/proto/table_meta.pb.h"

using std::string;

namespace tera {

void ShowTableSchema(const TableSchema& schema);

void ShowTableMeta(const TableMeta& meta);

void ShowTableDescriptor(TableDescriptor& table_desc);

void TableDescToSchema(const TableDescriptor& desc, TableSchema* schema);

void TableSchemaToDesc(const TableSchema& schema, TableDescriptor* desc);

// length<256 && only-allow-chars-digits-'_' && not-allow-starting-with-digits
bool CheckName(const string& name);

// extract cf name and type from schema string
// e.g. schema string: cf_link<int>
//      cf name      : cf_link
//      cf type      : int
bool ParseCfNameType(const string& in, string* name, string* type);

bool ParseSchemaSetTableDescriptor(const string& schema, TableDescriptor* desc, bool* update_lg_cf);

bool ParseSchema(const string& schema, TableDescriptor* table_desc);

bool ParseKvSchema(const string& schema, TableDescriptor* table_desc, LocalityGroupDescriptor* lg_desc, ColumnFamilyDescriptor* cf_desc = NULL);

bool ParseScanSchema(const string& schema, ScanDescriptor* desc);

typedef std::pair<string, string> Property;
typedef std::vector<Property> PropertyList;
bool ParseProperty(const string& schema, string* name, PropertyList* prop_list);

bool SetCfProperties(const PropertyList& props, ColumnFamilyDescriptor* desc);

bool SetLgProperties(const PropertyList& props, LocalityGroupDescriptor* desc);

bool SetTableProperties(const PropertyList& props, TableDescriptor* desc);

bool ParseCfSchema(const string& schema, TableDescriptor* table_desc, LocalityGroupDescriptor* lg_desc);

bool ParseLgSchema(const string& schema, TableDescriptor* table_desc);

bool BuildSchema(TableDescriptor* table_desc, string* schema);

bool CommaInBracket(const string& full, string::size_type pos);

void SplitCfSchema(const string& full, std::vector<string>* result);

bool HasInvalidCharInSchema(const string& schema);

bool ParsePrefixPropertyValue(const string& pair, string& prefix, string& property, string& value);

string PrefixType(const std::string& property);

} // namespace tera
#endif // TERA_SDK_UTILS_H_
