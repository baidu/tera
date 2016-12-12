// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <iostream>
#include <vector>
#include <string>
#include <assert.h>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "tera.h"
#include "common/base/string_ext.h"

DEFINE_string(tera_conf, "../conf/tera.flag", "tera conf");
DEFINE_string(table_name, "", "table name");
DEFINE_string(key, "", "row key");
DEFINE_uint64(snapshot, 0, "snapshot");
DEFINE_string(fields, "", "fields");

using namespace tera;

void GetAllReadFields(RowReader* reader)
{
    std::string field_delim("+");
    std::string qu_delim(":");
    std::vector<std::string> cf_list;
    SplitString(FLAGS_fields, field_delim, &cf_list);
    for (size_t i = 0; i < cf_list.size(); i++) {
        std::vector<std::string> qu_list;
        SplitString(cf_list[i], qu_delim, &qu_list);
        assert(qu_list.size() == 1 || qu_list.size() == 2);
        if (qu_list.size() == 1) {
            reader->AddColumnFamily(qu_list[0]);
        } else {
            reader->AddColumn(qu_list[0], qu_list[1]);
        }
    }
    if (FLAGS_snapshot != 0) {
        reader->SetSnapshot(FLAGS_snapshot);
    }
}

int main(int argc, char** argv)
{
    ::google::ParseCommandLineFlags(&argc, &argv, false);

    Client* client = Client::NewClient(FLAGS_tera_conf, NULL);
    if (!client) {
        LOG(ERROR) << "client instance not exist";
        return -1;
    }

    if (FLAGS_table_name == "") {
        LOG(ERROR) << "please set table name";
        return -1;
    }
    if (FLAGS_key == "") {
        LOG(ERROR) << "please set row key";
        return -1;
    }

    ErrorCode error_code;
    Table* target_table = NULL;
    if ((target_table = client->OpenTable(FLAGS_table_name, &error_code)) == NULL) {
        LOG(ERROR) << "open table " << FLAGS_table_name << "fail: " << strerr(error_code);
        return -1;
    }

    RowReader* reader = target_table->NewRowReader(FLAGS_key);
    GetAllReadFields(reader);
    target_table->Get(reader);
    while (!target_table->IsGetFinished())  ;

    while (!reader->Done()) {
        std::cout << reader->ColumnName() << ":" << reader->Timestamp()
                  << "  " << reader->Value() << std::endl;
        reader->Next();
    }
    delete reader;

    delete target_table;
    delete client;
    return 0;
}
