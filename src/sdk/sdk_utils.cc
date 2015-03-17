// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: Xu Peilin (xupeilin@baidu.com)

#include "sdk/sdk_utils.h"

#include <iostream>

#include "common/base/string_ext.h"
#include "common/base/string_number.h"
#include "common/file/file_path.h"
#include "gflags/gflags.h"
#include "glog/logging.h"

#include "sdk/filter_utils.h"

namespace tera {

string LgProp2Str(bool type) {
    if (type) {
        return "snappy";
    } else {
        return "none";
    }
}

string LgProp2Str(StoreMedium type) {
    if (type == DiskStore) {
        return "disk";
    } else if (type == FlashStore) {
        return "flash";
    } else if (type == MemoryStore) {
        return "memory";
    } else {
        return "";
    }
}

string TableProp2Str(RawKey type) {
    if (type == Readable) {
        return "readable";
    } else if (type == Binary) {
        return "binary";
    } else if (type == TTLKv) {
        return "ttlkv";
    } else {
        return "";
    }
}

void ShowTableSchema(const TableSchema& schema) {
    if (schema.kv_only()) {
        std::cout << std::endl << schema.name() << " (kv): " << std::endl;
    } else {
        std::cout << std::endl << schema.name() << " : " << std::endl;
    }
    std::cout << "  {rawkey=" << TableProp2Str(schema.raw_key())
        << ", splitsize=" << schema.split_size()
        << ", mergesize=" << schema.merge_size() << "}#" << std::endl;

    size_t lg_num = schema.locality_groups_size();
    size_t cf_num = schema.column_families_size();
    for (size_t lg_no = 0; lg_no < lg_num; ++lg_no) {
        const LocalityGroupSchema& lg_schema = schema.locality_groups(lg_no);
        std::cout << "      " << lg_schema.name()
            << " {storage=" << LgProp2Str(lg_schema.store_type())
            << ",compress=" << LgProp2Str(lg_schema.compress_type())
            << ",blocksize=" << lg_schema.block_size()
            << ",use_memtable_on_leveldb=" << lg_schema.use_memtable_on_leveldb();
        if (lg_schema.use_memtable_on_leveldb()) {
            std::cout << ",memtable_ldb_write_buffer_size="
                      << lg_schema.memtable_ldb_write_buffer_size()
                      << ",memtable_ldb_block_size="
                      << lg_schema.memtable_ldb_block_size();
        }
        std::cout << "}:";
        int cf_cnt = 0;
        for (size_t cf_no = 0; cf_no < cf_num; ++cf_no) {
            const ColumnFamilySchema& cf_schema = schema.column_families(cf_no);
            if (cf_schema.locality_group() != lg_schema.name()) {
                continue;
            }
            cf_cnt++;
            if (cf_cnt > 1) {
                std::cout << ",\n";
            } else {
                std::cout << "\n";
            }
            std::cout << "          ";
            if (cf_schema.name() == "") {
                std::cout << "  ";
            } else {
                std::cout << cf_schema.name();
            }

            if (cf_schema.type() != "") {
                std::cout << "<" << cf_schema.type() << ">";
            }
            std::cout << " {maxversions=" << cf_schema.max_versions()
                << ",minversions=" << cf_schema.min_versions()
                << ",ttl=" << cf_schema.time_to_live()
                << "}";
        }
        if (lg_no < lg_num - 1) {
            std::cout << "|" << std::endl;
        }
    }
    std::cout << std::endl << std::endl;
}

void ShowTableMeta(const TableMeta& meta) {
    const TableSchema& schema = meta.schema();
    ShowTableSchema(schema);
    std::cout << "Snapshot:" << std::endl;
    for (uint32_t i = 0; i < meta.snapshot_list_size(); ++i) {
        std::cout << " " << meta.snapshot_list(i) << std::endl;
    }
    std::cout << std::endl;
}

void ShowTableDescriptor(TableDescriptor& table_desc) {
    TableSchema schema;
    TableDescToSchema(table_desc, &schema);
    ShowTableSchema(schema);
}

void TableDescToSchema(const TableDescriptor& desc, TableSchema* schema) {
    schema->set_name(desc.TableName());
    switch (desc.RawKey()) {
        case kBinary:
            schema->set_raw_key(Binary);
            break;
        case kTTLKv:
            schema->set_raw_key(TTLKv);
            break;
        default:
            schema->set_raw_key(Readable);
            break;
    }
    schema->set_split_size(desc.SplitSize());
    schema->set_merge_size(desc.MergeSize());
    schema->set_kv_only(desc.IsKv());

    // add lg
    int num = desc.LocalityGroupNum();
    for (int i = 0; i < num; ++i) {
        LocalityGroupSchema* lg = schema->add_locality_groups();
        const LocalityGroupDescriptor* lgdesc = desc.LocalityGroup(i);
        lg->set_block_size(lgdesc->BlockSize());
        lg->set_compress_type(lgdesc->Compress() != kNoneCompress);
        lg->set_name(lgdesc->Name());
        //printf("add lg %s\n", lgdesc->Name().c_str());
        switch (lgdesc->Store()) {
            case kInMemory:
                lg->set_store_type(MemoryStore);
                break;
            case kInFlash:
                lg->set_store_type(FlashStore);
                break;
            default:
                lg->set_store_type(DiskStore);
                break;
        }
        lg->set_use_memtable_on_leveldb(lgdesc->UseMemtableOnLeveldb());
        if (lgdesc->MemtableLdbBlockSize() > 0) {
            lg->set_memtable_ldb_write_buffer_size(lgdesc->MemtableLdbWriteBufferSize());
        }
        if (lgdesc->MemtableLdbBlockSize() > 0) {
            lg->set_memtable_ldb_block_size(lgdesc->MemtableLdbBlockSize());
        }
        lg->set_id(lgdesc->Id());
    }
    // add cf
    int cfnum = desc.ColumnFamilyNum();
    for (int i = 0; i < cfnum; ++i) {
        ColumnFamilySchema* cf = schema->add_column_families();
        const ColumnFamilyDescriptor* cf_desc = desc.ColumnFamily(i);
        const LocalityGroupDescriptor* lg_desc =
            desc.LocalityGroup(cf_desc->LocalityGroup());
        assert(lg_desc);
        cf->set_name(cf_desc->Name());
        cf->set_time_to_live(cf_desc->TimeToLive());
        cf->set_locality_group(cf_desc->LocalityGroup());
        cf->set_max_versions(cf_desc->MaxVersions());
        cf->set_min_versions(cf_desc->MinVersions());
        cf->set_type(cf_desc->Type());
    }
}

void TableSchemaToDesc(const TableSchema& schema, TableDescriptor* desc) {
    if (schema.kv_only()) {
        desc->SetKvOnly();
    }

    switch (schema.raw_key()) {
        case Binary:
            desc->SetRawKey(kBinary);
            break;
        default:
            desc->SetRawKey(kReadable);
    }

    if (schema.has_split_size()) {
        desc->SetSplitSize(schema.split_size());
    }
    if (schema.has_merge_size()) {
        desc->SetMergeSize(schema.merge_size());
    }

    int32_t lg_num = schema.locality_groups_size();
    for (int32_t i = 0; i < lg_num; i++) {
        const LocalityGroupSchema& lg = schema.locality_groups(i);
        LocalityGroupDescriptor* lgd = desc->AddLocalityGroup(lg.name());
        if (lgd == NULL) {
            continue;
        }
        lgd->SetBlockSize(lg.block_size());
        switch (lg.store_type()) {
            case MemoryStore:
                lgd->SetStore(kInMemory);
                break;
            case FlashStore:
                lgd->SetStore(kInFlash);
                break;
            default:
                lgd->SetStore(kInDisk);
                break;
        }
        lgd->SetCompress(lg.compress_type() ? kSnappyCompress : kNoneCompress);
        lgd->SetUseBloomfilter(lg.use_bloom_filter());
    }
    int32_t cf_num = schema.column_families_size();
    for (int32_t i = 0; i < cf_num; i++) {
        const ColumnFamilySchema& cf = schema.column_families(i);
        ColumnFamilyDescriptor* cfd =
            desc->AddColumnFamily(cf.name(), cf.locality_group());
        if (cfd == NULL) {
            continue;
        }
        cfd->SetDiskQuota(cf.disk_quota());
        cfd->SetMaxVersions(cf.max_versions());
        cfd->SetMinVersions(cf.min_versions());
        cfd->SetTimeToLive(cf.time_to_live());
        cfd->SetType(cf.type());
    }
}

bool CheckName(const string& name) {
    if (name.size() == 0) {
        return true;
    }
    if (name.length() >= 256) {
        LOG(ERROR) << name << " has too many chars: " << name.length();
        return false;
    }
    for (size_t i = 0; i < name.length(); ++i) {
        if (!(name[i] <= '9' && name[i] >= '0')
            && !(name[i] <= 'z' && name[i] >= 'a')
            && !(name[i] <= 'Z' && name[i] >= 'A')
            && !(name[i] == '_')
            && !(name[i] == '-')) {
            LOG(ERROR) << name << " has illegal chars \"" << name[i] << "\"";
            return false;
        }
    }
    if (name[0] <='9' && name[0] >= '0') {
        LOG(ERROR) << "name do not allow starting with digits: " << name[0];
        return false;
    }
    return true;
}

bool ParseCfNameType(const string& in, string* name, string* type) {
    int len = in.size();
    string name_t, type_t;
    if (name == NULL) {
        name = &name_t;
    }
    if (type == NULL) {
        type = &type_t;
    }
    if (len < 3 || in[len-1] != '>') {
        if (CheckName(in)) {
            *name = in;
            *type = "";
            return true;
        } else {
            LOG(ERROR) << "illegal cf name: " << in;
            return false;
        }
    }

    string::size_type pos = in.find('<');
    if (pos == string::npos) {
        LOG(ERROR) << "illegal cf name, need '<': " << in;
        return false;
    }
    *name = in.substr(0, pos);
    if (!CheckName(*name)) {
        LOG(ERROR) << "illegal cf name, need '<': " << in;
        return false;
    }
    *type = in.substr(pos + 1, len - pos - 2);
    return true;
}

// property syntax: name{prop1=value1[,prop=value]}
// name or property may be empty
// e.g. "lg0{disk,blocksize=4K}", "{disk}", "lg0"
bool ParseProperty(const string& schema, string* name, PropertyList* prop_list) {
    string::size_type pos_s = schema.find('{');
    string::size_type pos_e = schema.find('}');
    if (pos_s == string::npos && pos_e == string::npos) {
        // deal with non-property
        if (name) {
            *name = schema;
        }
        prop_list->clear();
        return true;
    }
    if (pos_s == string::npos || pos_e == string::npos) {
        LOG(ERROR) << "property should be included by \"{}\": " << schema;
        return false;
    }
    string tmp_name = schema.substr(0, pos_s);
    if (!CheckName(tmp_name)) {
        return false;
    }
    if (name) {
        *name = tmp_name;
    }
    if (prop_list == NULL) {
        return true;
    }
    string prop_str = schema.substr(pos_s + 1, pos_e - pos_s -1);
    std::vector<string> props;
    SplitString(prop_str, ",", &props);
    prop_list->clear();
    for (size_t i = 0; i < props.size(); ++i) {
        std::vector<string> p;
        SplitString(props[i], "=", &p);
        if (p.size() == 1) {
            prop_list->push_back(std::make_pair(p[0], ""));
        } else if (p.size() == 2) {
            prop_list->push_back(std::make_pair(p[0], p[1]));
        } else {
            return false;
        }
        if (!CheckName((*prop_list)[i].first)) {
            return false;
        }
    }
    return true;
}

bool SetCfProperties(const PropertyList& props, ColumnFamilyDescriptor* desc) {
    for (size_t i = 0; i < props.size(); ++i) {
        const Property& prop = props[i];
        if (prop.first == "ttl") {
            int32_t ttl = atoi(prop.second.c_str());
            if (ttl < 0){
                LOG(ERROR) << "illegal value: " << prop.second
                    << " for property: " << prop.first;
                return false;
            }
            desc->SetTimeToLive(ttl);
        } else if (prop.first == "maxversions") {
            int32_t versions = atol(prop.second.c_str());
            if (versions <= 0){
                LOG(ERROR) << "illegal value: " << prop.second
                    << " for property: " << prop.first;
                return false;
            }
            desc->SetMaxVersions(versions);
        } else if (prop.first == "minversions") {
            int32_t versions = atol(prop.second.c_str());
            if (versions <= 0){
                LOG(ERROR) << "illegal value: " << prop.second
                    << " for property: " << prop.first;
                return false;
            }
            desc->SetMinVersions(versions);
        } else if (prop.first == "diskquota") {
            int64_t quota = atol(prop.second.c_str());
            if (quota <= 0){
                LOG(ERROR) << "illegal value: " << prop.second
                    << " for property: " << prop.first;
                return false;
            }
            desc->SetDiskQuota(quota);
        } else {
            LOG(ERROR) << "illegal cf props: " << prop.first;
            return false;
        }
    }
    return true;
}

bool SetLgProperties(const PropertyList& props, LocalityGroupDescriptor* desc) {
    for (size_t i = 0; i < props.size(); ++i) {
        const Property& prop = props[i];
        if (prop.first == "compress") {
            if (prop.second == "none") {
                desc->SetCompress(kNoneCompress);
            } else if (prop.second == "snappy") {
                desc->SetCompress(kSnappyCompress);
            } else {
                LOG(ERROR) << "illegal value: " << prop.second
                    << " for property: " << prop.first;
                return false;
            }
        } else if (prop.first == "storage") {
            if (prop.second == "disk") {
                desc->SetStore(kInDisk);
            } else if (prop.second == "flash") {
                desc->SetStore(kInFlash);
            } else if (prop.second == "memory") {
                desc->SetStore(kInMemory);
            } else {
                LOG(ERROR) << "illegal value: " << prop.second
                    << " for property: " << prop.first;
                return false;
            }
        } else if (prop.first == "blocksize") {
            int blocksize = atoi(prop.second.c_str());
            if (blocksize <= 0){
                LOG(ERROR) << "illegal value: " << prop.second
                    << " for property: " << prop.first;
                return false;
            }
            desc->SetBlockSize(blocksize);
        } else if (prop.first == "use_memtable_on_leveldb") {
            if (prop.second == "true") {
                desc->SetUseMemtableOnLeveldb(true);
            } else if (prop.second == "false") {
                desc->SetUseMemtableOnLeveldb(false);
            } else {
                LOG(ERROR) << "illegal value: " << prop.second
                           << " for property: " << prop.first;
                return false;
            }
        } else if (prop.first == "memtable_ldb_write_buffer_size") {
            int32_t buffer_size = atoi(prop.second.c_str()); //MB
            if (buffer_size <= 0) {
                LOG(ERROR) << "illegal value: " << prop.second
                           << " for property: " << prop.first;
                return false;
            }
            desc->SetMemtableLdbWriterBufferSize(buffer_size);
        } else if (prop.first == "memtable_ldb_block_size") {
            int32_t block_size = atoi(prop.second.c_str()); //KB
            if (block_size <= 0) {
                LOG(ERROR) << "illegal value: " << prop.second
                           << " for property: " << prop.first;
                return false;
            }
            desc->SetMemtableLdbBlockSize(block_size);
        } else {
            LOG(ERROR) << "illegal lg property: " << prop.first;
            return false;
        }
    }
    return true;
}

bool SetTableProperties(const PropertyList& props, TableDescriptor* desc) {
    for (size_t i = 0; i < props.size(); ++i) {
        const Property& prop = props[i];
        if (prop.first == "rawkey") {
            if (prop.second == "readable") {
                desc->SetRawKey(kReadable);
            } else if (prop.second == "binary") {
                desc->SetRawKey(kBinary);
            } else {
                LOG(ERROR) << "illegal value: " << prop.second
                    << " for property: " << prop.first;
                return false;
            }
        } else if (prop.first == "splitsize") {
            int splitsize = atoi(prop.second.c_str());
            if (splitsize < 0) { // splitsize == 0 : split closed
                LOG(ERROR) << "illegal value: " << prop.second
                    << " for property: " << prop.first;
                return false;
            }
            desc->SetSplitSize(splitsize);
        } else if (prop.first == "mergesize") {
            int mergesize = atoi(prop.second.c_str());
            if (mergesize < 0) { // mergesize == 0 : merge closed
                LOG(ERROR) << "illegal value: " << prop.second
                    << " for property: " << prop.first;
                return false;
            }
            desc->SetMergeSize(mergesize);
        } else {
            LOG(ERROR) << "illegal table property: " << prop.first;
            return false;
        }
    }
    return true;
}

bool ParseCfSchema(const string& schema,
                   TableDescriptor* table_desc,
                   LocalityGroupDescriptor* lg_desc) {
    string name_type, cf_name, cf_type;
    PropertyList cf_props;
    if (!ParseProperty(schema, &name_type, &cf_props)) {
        return false;
    }
    if (!ParseCfNameType(name_type, &cf_name, &cf_type)) {
        return false;
    }

    ColumnFamilyDescriptor* cf_desc;
    cf_desc = table_desc->AddColumnFamily(cf_name, lg_desc->Name());
    if (cf_desc == NULL) {
        LOG(ERROR) << "fail to add column family: " << cf_name;
        return false;
    }
    cf_desc->SetType(cf_type);

    if (!SetCfProperties(cf_props, cf_desc)) {
        LOG(ERROR) << "fail to set cf properties: " << cf_name;
        return false;
    }
    return true;
}

bool CommaInBracket(const string& full, string::size_type pos) {
    string::size_type l_pos = 0;
    string::size_type r_pos = string::npos;

    l_pos = full.find_last_of("{", pos);
    r_pos = full.find_first_of("}", pos);
    if (l_pos == string::npos || r_pos == string::npos) {
        return false;
    }
    if (r_pos > full.find_first_of("}", l_pos)) {
        return false;
    }
    if (l_pos < full.find_last_of("{", r_pos)) {
        return false;
    }
    return true;
}

void SplitCfSchema(const string& full, std::vector<string>* result) {
    const string delim = ",";
    result->clear();
    if (full.empty()) {
        return;
    }

    string tmp;
    string::size_type pos_begin = full.find_first_not_of(delim);
    string::size_type comma_pos = pos_begin;

    while (pos_begin != string::npos) {
        do {
            comma_pos = full.find(delim, comma_pos + 1);
        } while (CommaInBracket(full, comma_pos));

        if (comma_pos != string::npos) {
            tmp = full.substr(pos_begin, comma_pos - pos_begin);
            pos_begin = comma_pos + delim.length();
        } else {
            tmp = full.substr(pos_begin);
            pos_begin = comma_pos;
        }

        if (!tmp.empty()) {
            result->push_back(tmp);
            tmp.clear();
        }
    }
}

bool ParseLgSchema(const string& schema, TableDescriptor* table_desc) {
    std::vector<string> parts;
    SplitString(schema, ":", &parts);
    if (parts.size() != 2) {
        LOG(ERROR) << "lg syntax error: " << schema;
        return false;
    }

    string lg_name;
    PropertyList lg_props;
    if (!ParseProperty(parts[0], &lg_name, &lg_props)) {
        return false;
    }

    LocalityGroupDescriptor* lg_desc;
    lg_desc = table_desc->AddLocalityGroup(lg_name);
    if (lg_desc == NULL) {
        LOG(ERROR) << "fail to add locality group: " << lg_name;
        return false;
    }

    if (!SetLgProperties(lg_props, lg_desc)) {
        LOG(ERROR) << "fail to set lg properties: " << lg_name;
        return false;
    }

    std::vector<string> cfs;
    SplitCfSchema(parts[1], &cfs);
    CHECK(cfs.size() > 0);
    for (size_t i = 0; i < cfs.size(); ++i) {
        if (!ParseCfSchema(cfs[i], table_desc, lg_desc)) {
            return false;
        }
    }
    return true;
}

//   prefix:property=value
bool ParsePrefixPropertyValue(const string& pair, string& prefix, string& property, string& value) {
    string::size_type i = pair.find(":");
    string::size_type k = pair.find("=");
    if (i == string::npos || k == string::npos ||
        i == 0 || i + 1 >= k || k == pair.length() - 1) {
        return false;
    }
    prefix = pair.substr(0, i);
    property = pair.substr(i + 1, k - i - 1);
    value = pair.substr(k + 1, pair.length() - 1);
    return true;
}

string PrefixType(const std::string& property) {
    string lg_prop[] = {"compress", "storage", "blocksize","use_memtable_on_leveldb",
                        "memtable_ldb_write_buffer_size", "memtable_ldb_block_size"};
    string cf_prop[] = {"ttl", "maxversions", "minversions", "diskquota"};

    std::set<string> lgset(lg_prop, lg_prop + sizeof(lg_prop) / sizeof(lg_prop[0]));
    std::set<string> cfset(cf_prop, cf_prop + sizeof(cf_prop) / sizeof(cf_prop[0]));
    if (lgset.find(property) != lgset.end()){
        return string("lg");
    } else if (cfset.find(property) != cfset.end()){
        return string("cf");
    }
    return string("unknown");
}

bool HasInvalidCharInSchema(const string& schema) {
    for (size_t i = 0; i < schema.length(); i++){
        char ch = schema[i];
        if (isalnum(ch) || ch == '_' || ch == ':' || ch == '=' || ch == ',') {
            continue;
        }
        return true; // has invalid char
    }
    return false;
}

bool CheckTableDescrptor(TableDescriptor* table_desc) {
    if (table_desc->SplitSize() < table_desc->MergeSize() * 5) {
        LOG(ERROR) << "splitsize should be 5 times larger than mergesize"
            << ", splitsize: " << table_desc->SplitSize()
            << ", mergesize: " << table_desc->MergeSize();
        return false;
    }
    return true;
}

/*
 * parses `schema', sets TableDescriptor and notes whether to update lg or cf.
 *
 * an example of schema:
 *   "table:splitsize=100,lg0:storage=disk,lg1:blocksize=5,cf6:ttl=0"
 */
bool ParseSchemaSetTableDescriptor(const string& schema, TableDescriptor* table_desc,
                                   bool* is_update_lg_cf){
    if (table_desc == NULL) {
        LOG(ERROR) << "parameter `table_desc' is NULL";
        return false;
    }
    std::vector<string> parts;
    string schema_in = RemoveInvisibleChar(schema);
    if (HasInvalidCharInSchema(schema)) {
        LOG(ERROR) << "illegal char(s) in schema: " << schema;
        return false;
    }
    SplitString(schema_in, ",", &parts);
    if (parts.size() == 0) {
        LOG(ERROR) << "illegal schema: " << schema;
        return false;
    }

    for (size_t i = 0; i < parts.size(); i++) {
        string prefix;// "table" | lg name | cf name
        string property; // splitsize/compress/ttl ...
        string value;
        if (!ParsePrefixPropertyValue(parts[i], prefix, property, value)) {
            LOG(ERROR) << "ParsePrefixPropertyValue:illegal schema: " << parts[i];
            return false;
        }
        if (prefix == "" || property == "" || value == "") {
            LOG(ERROR) << "illegal schema: " << parts[i];
            return false;
        }
        Property apair(property, value);
        PropertyList props;
        props.push_back(apair);
        if (prefix == "table") {
            if (property == "rawkey") {
                LOG(ERROR) << "oops, can't reset <rawkey>";
                return false;
            }
            if (!SetTableProperties(props, table_desc)){
                LOG(ERROR) << "SetTableProperties() failed";
                return false;
            }
        } else if (PrefixType(property) == "lg"){
            *is_update_lg_cf = true;
            LocalityGroupDescriptor* lg_desc =
                const_cast<LocalityGroupDescriptor*>(table_desc->LocalityGroup(prefix));
            if (lg_desc == NULL) {
                LOG(ERROR) << "illegal schema: " << parts[i];
                return false;
            }
            SetLgProperties(props, lg_desc);
        } else if (PrefixType(property) == "cf"){
            *is_update_lg_cf = true;
            ColumnFamilyDescriptor* cf_desc =
                const_cast<ColumnFamilyDescriptor*>(table_desc->ColumnFamily(prefix));
            if (cf_desc == NULL) {
                LOG(ERROR) << "illegal schema: " << parts[i];
                return false;
            }
            SetCfProperties(props, cf_desc);
        } else{
            LOG(ERROR) << "illegal schema: " << parts[i];
            return false;
        }
    }
    if (!CheckTableDescrptor(table_desc)) {
        return false;
    }
    return true;
}

bool ParseSchema(const string& schema, TableDescriptor* table_desc) {
    std::vector<string> parts;
    string schema_in = RemoveInvisibleChar(schema);
    SplitString(schema_in, "#", &parts);
    string lg_props;

    if (parts.size() == 2) {
        PropertyList props;
        if (!ParseProperty(parts[0], NULL, &props)) {
            return false;
        }
        if (!SetTableProperties(props, table_desc)) {
            return false;
        }
        lg_props = parts[1];
    } else {
        lg_props = parts[0];
    }

    std::vector<string> lgs;
    SplitString(lg_props, "|", &lgs);

    for (size_t i = 0; i < lgs.size(); ++i) {
        if (!ParseLgSchema(lgs[i], table_desc)) {
            return false;
        }
    }

    if (!CheckTableDescrptor(table_desc)) {
        return false;
    }
    return true;
}

bool ParseKvSchema(const string& schema, TableDescriptor* table_desc, LocalityGroupDescriptor* lg_desc, ColumnFamilyDescriptor* cf_desc) {
    PropertyList lg_props;
    if (!ParseProperty(schema, NULL, &lg_props)) {
        return false;
    }

    PropertyList cf_props;
    for (PropertyList::iterator iter = lg_props.begin(); iter != lg_props.end(); ) {
        Property& prop = *iter;
        if (prop.first == "ttl") {
            cf_props.push_back(prop);
            lg_props.erase(iter);
        } else {
            ++iter;
        }
    }

    if (!SetLgProperties(lg_props, lg_desc)) {
        return false;
    }

    // CF Descriptor for KV table.
    if (cf_desc && cf_props.size()) {
        if (!SetCfProperties(cf_props, cf_desc)) {
            return false;
        }
        table_desc->SetRawKey(kTTLKv); // TTL in schema
    } else {
        table_desc->SetRawKey(kReadable); // No-TTL in schema
    }

    if (!CheckTableDescrptor(table_desc)) {
        return false;
    }
    return true;
}

bool ParseScanSchema(const string& schema, ScanDescriptor* desc) {
    std::vector<string> cfs;
    string schema_in;
    string cf, col;
    string::size_type pos;
    if ((pos = schema.find("SELECT ")) != 0) {
        LOG(ERROR) << "illegal scan expression: should be begin with \"SELECT\"";
        return false;
    }
    if ((pos = schema.find(" WHERE ")) != string::npos) {
        schema_in = schema.substr(7, pos - 7);
        string filter_str = schema.substr(pos + 7, schema.size() - pos - 7);
        desc->SetFilterString(filter_str);
    } else {
        schema_in = schema.substr(7);
    }

    schema_in = RemoveInvisibleChar(schema_in);
    if (schema_in == "*") {
        return true;
    }
    SplitString(schema_in, ",", &cfs);
    for (size_t i = 0; i < cfs.size(); ++i) {
        if ((pos = cfs[i].find(":", 0)) == string::npos) {
            // add columnfamily
            desc->AddColumnFamily(cfs[i]);
            VLOG(10) << "add cf: " << cfs[i] << " to scan descriptor";
        } else {
            // add column
            cf = cfs[i].substr(0, pos);
            col = cfs[i].substr(pos + 1);
            desc->AddColumn(cf, col);
            VLOG(10) << "add column: " << cf << ":" << col << " to scan descriptor";
        }
    }
    return true;
}

bool BuildSchema(TableDescriptor* table_desc, string* schema) {
    // build schema string from table descriptor
    if (schema == NULL) {
        LOG(ERROR) << "schema string is NULL.";
        return false;
    }
    if (table_desc == NULL) {
        LOG(ERROR) << "table descriptor is NULL.";
        return false;
    }

    schema->clear();
    int lg_num = table_desc->LocalityGroupNum();
    int cf_num = table_desc->ColumnFamilyNum();
    for (size_t lg_no = 0; lg_no < lg_num; ++lg_no) {
        const LocalityGroupDescriptor* lg_desc = table_desc->LocalityGroup(lg_no);
        string lg_name = lg_desc->Name();
        if (lg_no > 0) {
            schema->append("|");
        }
        schema->append(lg_name);
        schema->append(":");
        int cf_cnt = 0;
        for (size_t cf_no = 0; cf_no < cf_num; ++cf_no) {
            const ColumnFamilyDescriptor* cf_desc = table_desc->ColumnFamily(cf_no);
            if (cf_desc->LocalityGroup() == lg_name && cf_desc->Name() != "") {
                if (cf_cnt++ > 0) {
                    schema->append(",");
                }
                schema->append(cf_desc->Name());
            }
        }
    }
    return true;
}
} // namespace tera
