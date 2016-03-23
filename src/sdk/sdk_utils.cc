// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: Xu Peilin (xupeilin@baidu.com)

#include "sdk/sdk_utils.h"

#include <fstream>
#include <iostream>

#include "common/base/string_ext.h"
#include "common/base/string_number.h"
#include "common/file/file_path.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "utils/string_util.h"

#include "sdk/filter_utils.h"

DECLARE_int64(tera_tablet_write_block_size);
DECLARE_int64(tera_tablet_ldb_sst_size);
DECLARE_int64(tera_master_merge_tablet_size);

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
    } else if (type == GeneralKv) {
        return "kv";
    } else {
        return "";
    }
}

string Switch2Str(bool enabled) {
    if (enabled) {
        return "on";
    } else {
        return "off";
    }
}

void ReplaceStringInPlace(std::string& subject,
                          const std::string& search,
                          const std::string& replace) {
    size_t pos = 0;
    while ((pos = subject.find(search, pos)) != std::string::npos) {
        subject.replace(pos, search.length(), replace);
        pos += replace.length();
    }
}

void ShowTableSchema(const TableSchema& s, bool is_x) {
    TableSchema schema = s;
    std::stringstream ss;
    std::string str;
    std::string table_alias = schema.name();
    if (!schema.alias().empty()) {
        table_alias = schema.alias();
    }
    if (schema.has_kv_only() && schema.kv_only()) {
        schema.set_raw_key(GeneralKv);
    }

    if (schema.raw_key() == TTLKv || schema.raw_key() == GeneralKv) {
        const LocalityGroupSchema& lg_schema = schema.locality_groups(0);
        ss << "\n  " << table_alias << " <";
        if (is_x || schema.raw_key() != Readable) {
            ss << "rawkey=" << TableProp2Str(schema.raw_key()) << ",";
        }
        ss << "splitsize=" << schema.split_size() << ",";
        if (is_x || schema.merge_size() != FLAGS_tera_master_merge_tablet_size) {
            ss << "mergesize=" << schema.merge_size() << ",";
        }
        if (is_x || schema.disable_wal()) {
            ss << "wal=" << Switch2Str(!schema.disable_wal()) << ",";
        }
        if (is_x || lg_schema.store_type() != DiskStore) {
            ss << "storage=" << LgProp2Str(lg_schema.store_type()) << ",";
        }
        if (is_x || lg_schema.block_size() != FLAGS_tera_tablet_write_block_size) {
            ss << "blocksize=" << lg_schema.block_size() << ",";
        }
        if (is_x && schema.admin_group() != "") {
            ss << "admin_group=" << schema.admin_group() << ",";
        }
        if (is_x && schema.admin() != "") {
            ss << "admin=" << schema.admin() << ",";
        }
        ss << "\b>\n" << "  (kv mode)\n";
        str = ss.str();
        ReplaceStringInPlace(str, ",\b", "");
        std::cout << str << std::endl;
        return;
    }

    ss << "\n  " << table_alias << " <";
    if (is_x || schema.raw_key() != Readable) {
        ss << "rawkey=" << TableProp2Str(schema.raw_key()) << ",";
    }
    ss << "splitsize=" << schema.split_size() << ",";
    if (is_x || schema.merge_size() != FLAGS_tera_master_merge_tablet_size) {
        ss << "mergesize=" << schema.merge_size() << ",";
    }
    if (is_x && schema.admin_group() != "") {
        ss << "admin_group=" << schema.admin_group() << ",";
    }
    if (is_x && schema.admin() != "") {
        ss << "admin=" << schema.admin() << ",";
    }
    if (is_x || schema.disable_wal()) {
        ss << "wal=" << Switch2Str(!schema.disable_wal()) << ",";
    }
    ss << "\b> {" << std::endl;

    size_t lg_num = schema.locality_groups_size();
    size_t cf_num = schema.column_families_size();
    for (size_t lg_no = 0; lg_no < lg_num; ++lg_no) {
        const LocalityGroupSchema& lg_schema = schema.locality_groups(lg_no);
        ss << "      " << lg_schema.name() << " <";
        ss << "storage=" << LgProp2Str(lg_schema.store_type()) << ",";
        if (is_x || lg_schema.block_size() != FLAGS_tera_tablet_write_block_size) {
            ss << "blocksize=" << lg_schema.block_size() << ",";
        }
        if (is_x) {
            ss << "sst_size=" << (lg_schema.sst_size() >> 20) << ",";
        }
        if (lg_schema.use_memtable_on_leveldb()) {
            ss << "use_memtable_on_leveldb=true"
                << ",memtable_ldb_write_buffer_size="
                << lg_schema.memtable_ldb_write_buffer_size()
                << ",memtable_ldb_block_size="
                << lg_schema.memtable_ldb_block_size() << ",";
        }
        ss << "\b> {" << std::endl;
        for (size_t cf_no = 0; cf_no < cf_num; ++cf_no) {
            const ColumnFamilySchema& cf_schema = schema.column_families(cf_no);
            if (cf_schema.locality_group() != lg_schema.name()) {
                continue;
            }
            ss << "          " << cf_schema.name();
            std::stringstream cf_ss;
            cf_ss << " <";
            if (is_x || cf_schema.max_versions() != 1) {
                cf_ss << "maxversions=" << cf_schema.max_versions() << ",";
            }
            if (is_x || cf_schema.min_versions() != 1) {
                cf_ss << "minversions=" << cf_schema.min_versions() << ",";
            }
            if (is_x || cf_schema.time_to_live() != 0) {
                cf_ss << "ttl=" << cf_schema.time_to_live() << ",";
            }
            if (is_x || (cf_schema.type() != "bytes" && cf_schema.type() != "")) {
                if (cf_schema.type() != "") {
                    cf_ss << "type=" << cf_schema.type() << ",";
                } else {
                    cf_ss << "type=bytes" << ",";
                }
            }
            cf_ss << "\b>";
            if (cf_ss.str().size() > 5) {
                ss << cf_ss.str();
            }
            ss << "," << std::endl;
        }
        ss << "      }," << std::endl;
    }
    ss << "  }" << std::endl;
    str = ss.str();
    ReplaceStringInPlace(str, ",\b", "");
    std::cout << str << std::endl;
}

void ShowTableMeta(const TableMeta& meta) {
    const TableSchema& schema = meta.schema();
    ShowTableSchema(schema);
    std::cout << "Snapshot:" << std::endl;
    for (int32_t i = 0; i < meta.snapshot_list_size(); ++i) {
        std::cout << " " << meta.snapshot_list(i) << std::endl;
    }
    std::cout << std::endl;
}

void ShowTableDescriptor(TableDescriptor& table_desc, bool is_x) {
    TableSchema schema;
    TableDescToSchema(table_desc, &schema);
    ShowTableSchema(schema, is_x);
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
        case kGeneralKv:
            schema->set_raw_key(GeneralKv);
            // compat old code
            schema->set_kv_only(true);
            break;
        default:
            schema->set_raw_key(Readable);
            break;
    }
    schema->set_split_size(desc.SplitSize());
    schema->set_merge_size(desc.MergeSize());
    schema->set_admin_group(desc.AdminGroup());
    schema->set_admin(desc.Admin());
    schema->set_disable_wal(desc.IsWalDisabled());
    schema->set_alias(desc.Alias());
    // add lg
    int num = desc.LocalityGroupNum();
    for (int i = 0; i < num; ++i) {
        LocalityGroupSchema* lg = schema->add_locality_groups();
        const LocalityGroupDescriptor* lgdesc = desc.LocalityGroup(i);
        lg->set_block_size(lgdesc->BlockSize());
        lg->set_compress_type(lgdesc->Compress() != kNoneCompress);
        lg->set_name(lgdesc->Name());
        // printf("add lg %s\n", lgdesc->Name().c_str());
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
            lg->set_memtable_ldb_block_size(lgdesc->MemtableLdbBlockSize());
        }
        lg->set_sst_size(lgdesc->SstSize());
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
    switch (schema.raw_key()) {
        case Binary:
            desc->SetRawKey(kBinary);
            break;
        case TTLKv:
            desc->SetRawKey(kTTLKv);
            break;
        case GeneralKv:
            desc->SetRawKey(kGeneralKv);
            break;
        default:
            desc->SetRawKey(kReadable);
    }

    // for compatibility
    if (schema.kv_only()) {
        desc->SetRawKey(kGeneralKv);
    }

    if (schema.has_split_size()) {
        desc->SetSplitSize(schema.split_size());
    }
    if (schema.has_merge_size()) {
        desc->SetMergeSize(schema.merge_size());
    }
    if (schema.has_admin_group()) {
        desc->SetAdminGroup(schema.admin_group());
    }
    if (schema.has_admin()) {
        desc->SetAdmin(schema.admin());
    }
    if (schema.has_disable_wal() && schema.disable_wal()) {
        desc->DisableWal();
    }
    if (schema.has_alias()) {
        desc->SetAlias(schema.alias());
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
        lgd->SetUseMemtableOnLeveldb(lg.use_memtable_on_leveldb());
        lgd->SetMemtableLdbWriteBufferSize(lg.memtable_ldb_write_buffer_size());
        lgd->SetMemtableLdbBlockSize(lg.memtable_ldb_block_size());
        lgd->SetSstSize(lg.sst_size());
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

bool SetCfProperties(const string& name, const string& value,
                     ColumnFamilyDescriptor* desc) {
    if (desc == NULL) {
        return false;
    }
    if (name == "ttl") {
        int32_t ttl;
        if (!StringToNumber(value, &ttl) || (ttl < 0)) {
            return false;
        }
        desc->SetTimeToLive(ttl);
    } else if (name == "maxversions") {
        int32_t versions;
        if (!StringToNumber(value, &versions) || (versions <= 0)) {
            return false;
        }
        desc->SetMaxVersions(versions);
    } else if (name == "minversions") {
        int32_t versions;
        if (!StringToNumber(value, &versions) || (versions <= 0)) {
            return false;
        }
        desc->SetMinVersions(versions);
    } else if (name == "diskquota") {
        int64_t quota;
        if (!StringToNumber(value, &quota) || (quota <= 0)) {
            return false;
        }
        desc->SetDiskQuota(quota);
    } else if (name == "type") {
        if (value != "bytes") {
            return false;
        }
        desc->SetType(value);
    }else {
        return false;
    }
    return true;
}

bool SetLgProperties(const string& name, const string& value,
                     LocalityGroupDescriptor* desc) {
    if (desc == NULL) {
        return false;
    }
    if (name == "compress") {
        if (value == "none") {
            desc->SetCompress(kNoneCompress);
        } else if (value == "snappy") {
            desc->SetCompress(kSnappyCompress);
        } else {
            return false;
        }
    } else if (name == "storage") {
        if (value == "disk") {
            desc->SetStore(kInDisk);
        } else if (value == "flash") {
            desc->SetStore(kInFlash);
        } else if (value == "memory") {
            desc->SetStore(kInMemory);
        } else {
            return false;
        }
    } else if (name == "blocksize") {
        int blocksize;
        if (!StringToNumber(value, &blocksize) || (blocksize <= 0)){
            return false;
        }
        desc->SetBlockSize(blocksize);
    } else if (name == "use_memtable_on_leveldb") {
        if (value == "true") {
            desc->SetUseMemtableOnLeveldb(true);
        } else if (value == "false") {
            desc->SetUseMemtableOnLeveldb(false);
        } else {
            return false;
        }
    } else if (name == "memtable_ldb_write_buffer_size") {
        int32_t buffer_size; //KB
        if (!StringToNumber(value, &buffer_size) || (buffer_size <= 0)) {
            return false;
        }
        desc->SetMemtableLdbWriteBufferSize(buffer_size);
    } else if (name == "memtable_ldb_block_size") {
        int32_t block_size; //KB
        if (!StringToNumber(value, &block_size) || (block_size <= 0)) {
            return false;
        }
        desc->SetMemtableLdbBlockSize(block_size);
    } else if (name == "sst_size") {
        const int32_t SST_SIZE_MAX = 1024; // MB
        int32_t sst_size;
        if (!StringToNumber(value, &sst_size) || (sst_size <= 0) || (sst_size > SST_SIZE_MAX) ) {
            return false;
        }
        desc->SetSstSize(sst_size<<20); // display in MB, store in Bytes.
    } else {
        return false;
    }
    return true;
}

bool SetTableProperties(const string& name, const string& value,
                        TableDescriptor* desc) {
    if (desc == NULL) {
        return false;
    }
    if (name == "rawkey") {
        if (value == "readable") {
            desc->SetRawKey(kReadable);
        } else if (value == "binary") {
            desc->SetRawKey(kBinary);
        } else if (value == "ttlkv") {
            desc->SetRawKey(kTTLKv);
        } else if (value == "kv") {
            desc->SetRawKey(kGeneralKv);
        } else {
            return false;
        }
    } else if (name == "splitsize") {
        int splitsize; // MB
        if (!StringToNumber(value, &splitsize) || (splitsize < 0)) {
            return false;
        }
        desc->SetSplitSize(splitsize);
    } else if (name == "mergesize") {
        int mergesize; // MB
        if (!StringToNumber(value, &mergesize) || (mergesize < 0)) { // mergesize == 0 : merge closed
            return false;
        }
        desc->SetMergeSize(mergesize);
    } else if (name == "admin_group"){
        if (!IsValidGroupName(value)) {
            return false;
        }
        desc->SetAdminGroup(value);
    } else if (name == "admin") {
        if (!IsValidUserName(value)) {
            return false;
        }
        desc->SetAdmin(value);
    } else if (name == "wal") {
        if (value == "on") {
            // do nothing
        } else if (value == "off") {
            desc->DisableWal();
        } else {
            return false;
        }
    } else {
        return false;
    }
    return true;
}

bool CheckTableDescrptor(const TableDescriptor& desc, ErrorCode* err) {
    std::stringstream ss;
    if (desc.SplitSize() < desc.MergeSize() * 5) {
        ss << "splitsize should be 5 times larger than mergesize"
           << ", splitsize: " << desc.SplitSize()
           << ", mergesize: " << desc.MergeSize();
        if (err != NULL) {
            err->SetFailed(ErrorCode::kBadParam, ss.str());
        }
        return false;
    }
    if (!IsValidTableName(desc.TableName())) {
        if (err != NULL) {
            err->SetFailed(ErrorCode::kBadParam, " invalid tablename ");
        }
        return false;
    }
    for (int32_t i = 0; i < desc.ColumnFamilyNum(); ++i) {
        if (!IsValidColumnFamilyName(desc.ColumnFamily(i)->Name())) {
            ss << " invalid columnfamily name:" << desc.ColumnFamily(i)->Name();
            if (err != NULL) {
                err->SetFailed(ErrorCode::kBadParam, ss.str());
            }
            return false;
        }
    }
    return true;
}

bool UpdateCfProperties(const PropTree::Node* table_node, TableDescriptor* table_desc) {
    if (table_node == NULL || table_desc == NULL) {
        return false;
    }
    for (size_t i = 0; i < table_node->children_.size(); ++i) {
        PropTree::Node* lg_node = table_node->children_[i];
        LocalityGroupDescriptor* lg_desc;
        lg_desc = const_cast<LocalityGroupDescriptor*>
            (table_desc->LocalityGroup(lg_node->name_));
        if (lg_desc == NULL) {
            LOG(ERROR) << "[update] fail to get locality group: " << lg_node->name_;
            return false;
        }
        // add all column families and properties
        for (size_t j = 0; j < lg_node->children_.size(); ++j) {
            PropTree::Node* cf_node = lg_node->children_[j];
            ColumnFamilyDescriptor* cf_desc;
            cf_desc = const_cast<ColumnFamilyDescriptor*>
                (table_desc->ColumnFamily(cf_node->name_));
            for (std::map<string, string>::iterator it = cf_node->properties_.begin();
                 it != cf_node->properties_.end(); ++it) {
                if ((cf_desc == NULL) && (it->first == "op") && (it->second == "add")) {
                    cf_desc = table_desc->AddColumnFamily(cf_node->name_, lg_desc->Name());
                    if(cf_desc == NULL) {
                        LOG(ERROR) << "[update] fail to add column family";
                        return false;
                    }
                    LOG(INFO) << "[update] add cf: " << cf_node->name_;
                    continue;
                } else if ((it->first == "op") && (it->second == "del")) {
                    // del cf
                    table_desc->RemoveColumnFamily(cf_node->name_);
                    LOG(INFO) << "[update] try to del cf: " << cf_node->name_;
                    continue;
                }
                if (!SetCfProperties(it->first, it->second, cf_desc)) {
                    LOG(ERROR) << "[update] illegal value: " << it->second
                        << " for cf property: " << it->first;
                    return false;
                }
            }
        }
    }
    return true;
}

bool UpdateLgProperties(const PropTree::Node* table_node, TableDescriptor* table_desc) {
    if (table_node == NULL || table_desc == NULL) {
        return false;
    }
    for (size_t i = 0; i < table_node->children_.size(); ++i) {
        PropTree::Node* lg_node = table_node->children_[i];
        LocalityGroupDescriptor* lg_desc;
        lg_desc = const_cast<LocalityGroupDescriptor*>
            (table_desc->LocalityGroup(lg_node->name_));
        if (lg_desc == NULL) {
            LOG(ERROR) << "[update] fail to get locality group: " << lg_node->name_;
            return false;
        }
        // set locality group properties
        for (std::map<string, string>::iterator it_lg = lg_node->properties_.begin();
             it_lg != lg_node->properties_.end(); ++it_lg) {
            if (!SetLgProperties(it_lg->first, it_lg->second, lg_desc)) {
                LOG(ERROR) << "[update] illegal value: " << it_lg->second
                    << " for lg property: " << it_lg->first;
                return false;
            }
        }
    }
    return true;
}

bool UpdateTableProperties(const PropTree::Node* table_node, TableDescriptor* table_desc) {
    if (table_node == NULL || table_desc == NULL) {
        return false;
    }
    for (std::map<string, string>::const_iterator i = table_node->properties_.begin();
         i != table_node->properties_.end(); ++i) {
        if (i->first == "rawkey") {
            LOG(ERROR) << "[update] can't reset rawkey!";
            return false;
        }
        if (!SetTableProperties(i->first, i->second, table_desc)) {
            LOG(ERROR) << "[update] illegal value: " << i->second
                << " for table property: " << i->first;
            return false;
        }
    }
    return true;
}

bool UpdateKvTableProperties(const PropTree::Node* table_node, TableDescriptor* table_desc) {
    if (table_node == NULL || table_desc == NULL) {
        return false;
    }
    LocalityGroupDescriptor* lg_desc =
        const_cast<LocalityGroupDescriptor*>(table_desc->LocalityGroup("kv"));
    if (lg_desc == NULL) {
        LOG(ERROR) << "[update] fail to get locality group: kv";
        return false;
    }
    for (std::map<string, string>::const_iterator i = table_node->properties_.begin();
         i != table_node->properties_.end(); ++i) {
        if (i->first == "rawkey") {
            LOG(ERROR) << "[update] can't reset rawkey!";
            return false;
        }
        if (SetLgProperties(i->first, i->second, lg_desc)) {
            // do nothing
        } else if (!SetTableProperties(i->first, i->second, table_desc)) {
            LOG(ERROR) << "[update] illegal value: " << i->second
                << " for table property: " << i->first;
            return false;
        }
    }
    return true;
}

bool UpdateTableDescriptor(PropTree& schema_tree, TableDescriptor* table_desc, ErrorCode* err) {
    PropTree::Node* table_node = schema_tree.GetRootNode();
    if (table_node == NULL || table_desc == NULL) {
        return false;
    }
    bool is_ok = false;
    if (table_desc->RawKey() == kTTLKv || table_desc->RawKey() == kGeneralKv) {
        if (schema_tree.MaxDepth() != 1) {
            LOG(ERROR) << "invalid schema for kv table: " << table_node->name_;
            return false;
        }
        is_ok = UpdateKvTableProperties(table_node, table_desc);
    } else if (schema_tree.MaxDepth() == 1) {
        // updates table properties, no updates for lg & cf properties
        is_ok = UpdateTableProperties(table_node, table_desc);
    } else if (schema_tree.MaxDepth() == 2) {
        is_ok = UpdateLgProperties(table_node, table_desc)
                && UpdateTableProperties(table_node, table_desc);
    } else if (schema_tree.MaxDepth() == 3) {
        is_ok =  UpdateCfProperties(table_node, table_desc)
                 && UpdateLgProperties(table_node, table_desc)
                 && UpdateTableProperties(table_node, table_desc);
    } else {
        LOG(ERROR) << "invalid schema";
        return false;
    }
    if (is_ok) {
        return CheckTableDescrptor(*table_desc, err);
    }
    return false;
}

bool FillTableDescriptor(PropTree& schema_tree, TableDescriptor* table_desc) {
    PropTree::Node* table_node = schema_tree.GetRootNode();
    if (table_desc->TableName() != "" &&
        table_desc->TableName() != table_node->name_) {
        LOG(ERROR) << "table name error: " << table_desc->TableName()
            << ":" << table_node->name_;
        return false;
    }
    table_desc->SetTableName(schema_tree.GetRootNode()->name_);
    if (schema_tree.MaxDepth() != schema_tree.MinDepth() ||
        schema_tree.MaxDepth() == 0 || schema_tree.MaxDepth() > 3) {
        LOG(ERROR) << "schema error: " << schema_tree.FormatString();
        return false;
    }

    if (schema_tree.MaxDepth() == 1) {
        // kv mode, only have 1 locality group
        // e.g. table1<splitsize=1024, storage=flash>
        table_desc->SetRawKey(kGeneralKv);
        LocalityGroupDescriptor* lg_desc;
        lg_desc = table_desc->AddLocalityGroup("kv");
        if (lg_desc == NULL) {
            LOG(ERROR) << "fail to add locality group: kv";
            return false;
        }
        for (std::map<string, string>::iterator i = table_node->properties_.begin();
             i != table_node->properties_.end(); ++i) {
            if (!SetTableProperties(i->first, i->second, table_desc) &&
                !SetLgProperties(i->first, i->second, lg_desc)) {
                LOG(ERROR) << "illegal value: " << i->second
                    << " for table property: " << i->first;
                return false;
            }
        }
    } else if (schema_tree.MaxDepth() == 2) {
        // simple table mode, have 1 default lg
        // e.g. table1{cf1, cf2, cf3}
        LocalityGroupDescriptor* lg_desc;
        lg_desc = table_desc->AddLocalityGroup("lg0");
        if (lg_desc == NULL) {
            LOG(ERROR) << "fail to add locality group: lg0";
            return false;
        }
        // add all column families and properties
        for (size_t i = 0; i < table_node->children_.size(); ++i) {
            PropTree::Node* cf_node = table_node->children_[i];
            ColumnFamilyDescriptor* cf_desc;
            cf_desc = table_desc->AddColumnFamily(cf_node->name_, lg_desc->Name());
            if (cf_desc == NULL) {
                LOG(ERROR) << "fail to add column family: " << cf_node->name_;
                return false;
            }
            for (std::map<string, string>::iterator it = cf_node->properties_.begin();
                 it != cf_node->properties_.end(); ++it) {
                if (!SetCfProperties(it->first, it->second, cf_desc)) {
                    LOG(ERROR) << "illegal value: " << it->second
                        << " for cf property: " << it->first;
                    return false;
                }
            }
        }
        // set table properties
        for (std::map<string, string>::iterator i = table_node->properties_.begin();
             i != table_node->properties_.end(); ++i) {
            if (!SetTableProperties(i->first, i->second, table_desc)) {
                LOG(ERROR) << "illegal value: " << i->second
                    << " for table property: " << i->first;
                return false;
            }
        }
    } else if (schema_tree.MaxDepth() == 3) {
        // full mode, all elements are user-defined
        // e.g. table1<mergesize=100>{
        //          lg0<storage=memory>{
        //              cf1<maxversions=3>,
        //              cf2<ttl=100>
        //          },
        //          lg1{cf3}
        //      }
        for (size_t i = 0; i < table_node->children_.size(); ++i) {
            PropTree::Node* lg_node = table_node->children_[i];
            LocalityGroupDescriptor* lg_desc;
            lg_desc = table_desc->AddLocalityGroup(lg_node->name_);
            if (lg_desc == NULL) {
                LOG(ERROR) << "fail to add locality group: " << lg_node->name_;
                return false;
            }
            // add all column families and properties
            for (size_t j = 0; j < lg_node->children_.size(); ++j) {
                PropTree::Node* cf_node = lg_node->children_[j];
                ColumnFamilyDescriptor* cf_desc;
                cf_desc = table_desc->AddColumnFamily(cf_node->name_, lg_desc->Name());
                if (cf_desc == NULL) {
                    LOG(ERROR) << "fail to add column family: " << cf_node->name_;
                    return false;
                }
                for (std::map<string, string>::iterator it = cf_node->properties_.begin();
                     it != cf_node->properties_.end(); ++it) {
                    if (!SetCfProperties(it->first, it->second, cf_desc)) {
                        LOG(ERROR) << "illegal value: " << it->second
                            << " for cf property: " << it->first;
                        return false;
                    }
                }
            }
            // set locality group properties
            for (std::map<string, string>::iterator it_lg = lg_node->properties_.begin();
                 it_lg != lg_node->properties_.end(); ++it_lg) {
                if (!SetLgProperties(it_lg->first, it_lg->second, lg_desc)) {
                    LOG(ERROR) << "illegal value: " << it_lg->second
                        << " for lg property: " << it_lg->first;
                    return false;
                }
            }
        }
        // set table properties
        for (std::map<string, string>::iterator i = table_node->properties_.begin();
             i != table_node->properties_.end(); ++i) {
            if (!SetTableProperties(i->first, i->second, table_desc)) {
                LOG(ERROR) << "illegal value: " << i->second
                    << " for table property: " << i->first;
                return false;
            }
        }
    } else {
        LOG(FATAL) << "never here.";
    }
    return true;
}

bool ParseTableSchema(const string& schema, TableDescriptor* table_desc, ErrorCode* err) {
    PropTree schema_tree;
    if (!schema_tree.ParseFromString(schema)) {
        LOG(ERROR) << schema_tree.State();
        LOG(ERROR) << schema;
        return false;
    }

    VLOG(10) << "table to create: " << schema_tree.FormatString();
    if (FillTableDescriptor(schema_tree, table_desc) &&
        CheckTableDescrptor(*table_desc, err)) {
        return true;
    }
    return false;
}

bool ParseTableSchemaFile(const string& file, TableDescriptor* table_desc, ErrorCode* err) {
    PropTree schema_tree;
    if (!schema_tree.ParseFromFile(file)) {
        LOG(ERROR) << schema_tree.State();
        LOG(ERROR) << file;
        return false;
    }

    VLOG(10) << "table to create: " << schema_tree.FormatString();
    if (FillTableDescriptor(schema_tree, table_desc) &&
        CheckTableDescrptor(*table_desc, err)) {
        return true;
    }
    return false;
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
    int32_t lg_num = table_desc->LocalityGroupNum();
    int32_t cf_num = table_desc->ColumnFamilyNum();
    for (int32_t lg_no = 0; lg_no < lg_num; ++lg_no) {
        const LocalityGroupDescriptor* lg_desc = table_desc->LocalityGroup(lg_no);
        string lg_name = lg_desc->Name();
        if (lg_no > 0) {
            schema->append("|");
        }
        schema->append(lg_name);
        schema->append(":");
        int cf_cnt = 0;
        for (int32_t cf_no = 0; cf_no < cf_num; ++cf_no) {
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

bool ParseDelimiterFile(const string& filename, std::vector<string>* delims) {
    std::ifstream fin(filename.c_str());
    if (fin.fail()) {
        LOG(ERROR) << "fail to read delimiter file: " << filename;
        return false;
    }

    std::vector<string> delimiters;
    string str;
    while (fin >> str) {
        delimiters.push_back(str);
    }

    bool is_delim_error = false;
    for (size_t i = 1; i < delimiters.size(); i++) {
        if (delimiters[i] <= delimiters[i-1]) {
            LOG(ERROR) << "delimiter error: line: " << i + 1
                << ", [" << delimiters[i] << "]";
            is_delim_error = true;
        }
    }
    if (is_delim_error) {
        LOG(ERROR) << "create table fail, delimiter error.";
        return false;
    }
    delims->swap(delimiters);
    return true;
}
} // namespace tera
