// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "meta_helper.h"

#include "proto/tabletnode_client.h"
#include <glog/logging.h>

namespace tera {

MetaHelper::MetaHelper() {}

MetaHelper::~MetaHelper() {}

/*
 * old meta:
 * <tablename>#startkey  => [tablet]
 * @<tablename> =>          [table]
 * ~<username> =>           [user]
 *
 * new meta:
 * <tablename>#startkey =>  [tablet]
 * "@@tab@"   =>            [table]
 * "@\1use@"   =>           [user]
 * "@\1..."    =>           [...]
 */
MetaEntryType MetaHelper::GetMetaEntryType(const std::string& key) {
    if (key.size() < 2) {
        return kMetaEntryUnknown;
    }
    if (isupper(key[0]) || islower(key[0])) {
        return kMetaEntryTablet;
    }

    // old meta
    if ((key[0] == '@') && (isupper(key[1]) || islower(key[1]))) {
        return kMetaEntryTable;
    }
    if ((key[0] == '~') && (isupper(key[1]) || islower(key[1]))) {
        return kMetaEntryUser;
    }

    // new meta
    const int magic_len = 6;
    if (key.size() < magic_len + 1) { // magic + ...
        return kMetaEntryUnknown;
    }
    std::string magic(key, 0, magic_len);
    if (magic.compare(std::string("@@tab@", magic_len)) == 0) {
        return kMetaEntryTable;
    } else if (magic.compare(std::string("@\1use@", magic_len)) == 0) {
        return kMetaEntryUser;
    }
    return kMetaEntryUnknown;
}

// TABLE record
void MetaHelper::ParseEntryOfTable(const std::string& value, TableMeta* meta) {
    if (NULL != meta) {
        meta->ParseFromString(value);
    }
}

void MetaHelper::MakeEntryKeyOfTable(const std::string& table_name, std::string* key) {
    if (NULL != key) {
        *key = "@@tab@" + table_name;
    }
}

void MetaHelper::MakeEntryOfTable(const TableMeta& meta,
                                  std::string* key,
                                  std::string* value) {
    const std::string& table_name = meta.table_name();
    MakeEntryKeyOfTable(table_name, key);
    if (NULL != value) {
        meta.SerializeToString(value);
    }
}

// TABLET record
void MetaHelper::ParseEntryOfTablet(const std::string& value, TabletMeta* meta) {
    if (NULL != meta) {
        meta->ParseFromString(value);
    }
}

void MetaHelper::MakeEntryOfTablet(const TabletMeta& meta,
                                   std::string* key,
                                   std::string* value) {
    const std::string& table_name = meta.table_name();
    const std::string& key_start = meta.key_range().key_start();
    if (NULL != key) {
        *key = table_name + "#" + key_start;
    }
    if (NULL != value) {
        meta.SerializeToString(value);
    }
}

std::string MetaHelper::NextKey(const std::string& key) {
    //return key + "\0";
    std::string next = key;
    next.push_back('\0');
    return next;
}

void MetaHelper::MetaTableScanRange(const std::string& table_name,
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

} // namespace tera
