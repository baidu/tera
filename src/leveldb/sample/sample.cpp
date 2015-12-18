// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <cassert>
#include <iostream>
#include "leveldb/db.h"

int main()
{
    leveldb::DB* db;
    leveldb::Status status;
    leveldb::Options options;
    //options.error_if_exists = true;
    status = leveldb::DB::Open(options, ".db", &db);
    if (!status.ok()) {
        std::cerr << "Open db error: " << status.ToString() << std::endl;
        return -1;
    }

    std::string key("k1");
    std::string value;
    status = db->Get(leveldb::ReadOptions(), key, &value);
    if (status.ok()) {
        std::cout << "Get ok: [" << key << "] -> [" << value << "]" << std::endl;
        return 0;
    }

    status = db->Put(leveldb::WriteOptions(), key, "v1");
    if (!status.ok()) {
        std::cerr << "Put error: " << status.ToString() << std::endl;
        return -1;
    }

    status = db->Get(leveldb::ReadOptions(), key, &value);
    if (!status.ok()) {
        std::cerr << "Get error: " << status.ToString() << std::endl;
        return -1;
    }

    std::cout << "Get ok: [" << key << "] -> [" << value << "]" << std::endl;

    delete db;
    return 0;
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
