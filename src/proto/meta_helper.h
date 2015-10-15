// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_MASTER_META_HELPER_H_
#define TERA_MASTER_META_HELPER_H_

#include <glog/logging.h>
#include "common/mutex.h"
#include "proto/table_meta.pb.h"

namespace tera {

enum MetaEntryType {
    kMetaEntryUnknown = 0,
    kMetaEntryTable = 1,
    kMetaEntryTablet = 2,
    kMetaEntryUser = 3,
};


class MetaHelper {
public:
    MetaHelper();
    ~MetaHelper();

    static MetaEntryType GetMetaEntryType(const std::string& key);

    static void ParseEntryOfTable(const std::string& value, TableMeta* meta);
    static void MakeEntryOfTable(const TableMeta& meta,
                                 std::string* key,
                                 std::string* value);
    static void MakeEntryKeyOfTable(const std::string& table_name, std::string* key);

    static void ParseEntryOfTablet(const std::string& value, TabletMeta* meta);
    static void MakeEntryOfTablet(const TabletMeta& meta,
                                  std::string* key,
                                  std::string* value);
    static std::string NextKey(const std::string& key);
    static void MetaTableScanRange(const std::string& table_name,
                                   const std::string& tablet_key_start,
                                   const std::string& tablet_key_end,
                                   std::string* key_start, std::string* key_end);

private:
    MetaHelper(const MetaHelper&) {}
    MetaHelper& operator=(const MetaHelper&) {return *this;}
};

} // namespace tera

#endif // TERA_MASTER_META_HELPER_H_
