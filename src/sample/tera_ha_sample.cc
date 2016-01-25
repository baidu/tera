// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

 /**
 * @file tera_sample.cc
 * @author yanshiguang02@baidu.com
 * @date 2014/02/05 19:55:54
 * @brief Sample of Tera HA API
 *  每个表都有个默认的LocalityGroup "default" 要么被用户显示创建, 要么被系统创建
 *  每个表都有个某人的ColumnFamily ""
 *      要么被用户显示创建, 要么被系统创建, 默认属于lg default
 *      不包含列和版本
 *  这么创建表:
 *  create table {{localitygrop:{{"lg1":{"block_size":5}},{"lg2":{"store_type":"disk"}}},{"columnfamily":{"cf1":{}}}}}
 **/

#include <stdio.h>
#include <time.h>
#include <unistd.h>
#include <stdlib.h>
#include "gflags/gflags.h"

#include "sdk/tera.h"
#include "sdk/ha_tera.h"

/// 创建一个表格
int CreateTable(tera::Client* hclient) {
    // 创建一个表格的描述
    tera::TableDescriptor table_desc("webdb");

    // 创建LocalityGroup
    tera::LocalityGroupDescriptor* lgd0 = table_desc.AddLocalityGroup("lg0");
    lgd0->SetBlockSize(128*1024);
    lgd0->SetCompress(tera::kSnappyCompress);

    tera::LocalityGroupDescriptor* lgd1 = table_desc.AddLocalityGroup("lg1");
    lgd1->SetBlockSize(32*1024);
    lgd1->SetCompress(tera::kSnappyCompress);

    // 创建ColumnFamily
    tera::ColumnFamilyDescriptor* cfd1 = table_desc.AddColumnFamily("html", "lg0");
    cfd1->SetMaxVersions(5);
    cfd1->SetMinVersions(3);
    cfd1->SetTimeToLive(86400*100);
    tera::ColumnFamilyDescriptor* cfd2 = table_desc.AddColumnFamily("links", "lg1");
    cfd2->SetMaxVersions(5);
    cfd2->SetMinVersions(0);
    cfd2->SetTimeToLive(86400*100);
    tera::ColumnFamilyDescriptor* cfd3 = table_desc.AddColumnFamily("title", "lg1");
    cfd3->SetMaxVersions(5);
    table_desc.AddColumnFamily("anchor", "lg1");

    tera::ErrorCode error_code;
    if (!hclient->CreateTable(table_desc, &error_code)) {
        printf("Create Table fail: %s\n", tera::strerr(error_code));
    }
    return 0;
}

bool put_finish = false;
void WriteRowCallBack(tera::RowMutation* row_mu) {
    if (row_mu->GetError().GetType() != tera::ErrorCode::kOK) {
        printf ("async put failed! reason:%s\n", row_mu->GetError().GetReason().c_str());
    } else {
        printf ("async put ok!\n");
    }
    put_finish = true;
}

/// 修改一个表的内容
int ModifyTable(tera::Table* table) {
    tera::ErrorCode error_code;

    // 修改需要先创建一个 RowMutation

    tera::RowMutation* row = table->NewRowMutation("com.baidu.www/");
    // 写一个column
    row->Put("title", "abc", "Baidu.com");
    row->Put("title", "abd", "Baidu.com");
    row->Put("title", "abe", "Baidu.com");
    row->Put("title", "abf", "Baidu.com");
    row->Put("anchor", "www.hao123.com/", "百度");
    row->Put("html", "", "<html>Test content</html>");
    // row->Put("html", "", time(NULL), "<html>Test content</html>");
    // 删除一个column过去24小时内的所有版本
    // row->DeleteColumns("title", "abc", time(NULL), time(NULL) - 86400);
    // 删除一个column24小时之前的所有版本
    row->DeleteColumns("title", "abd", time(NULL) - 86400);
    // 删除一个column的所有版本
    row->DeleteColumns("title", "abe");
    // 删除一个columnfamily的所有列
    row->DeleteFamily("links");

    // 提交修改
    table->ApplyMutation(row);
    printf("Write to table : %s\n", tera::strerr(row->GetError()));
    delete row;

    // 批量提交修改
    tera::RowMutation* row2 = table->NewRowMutation("com.baidu.tieba/");
    // 删除一行的所有column family
    row2->DeleteRow();
    std::vector<tera::RowMutation*> mutation_list;
    mutation_list.push_back(row2);
    table->ApplyMutation(mutation_list);
    printf("Write to table : %s\n", tera::strerr(mutation_list[0]->GetError()));
    delete row2;

    // 异步修改
    tera::RowMutation* row3 = table->NewRowMutation("com.baidu.map/");
    row3->Put("content", "", 1, "<html>this is global map</html>");
    row3->SetCallBack(WriteRowCallBack);
    table->ApplyMutation(row3);

    while (!put_finish) {
        printf ("waitting async-put finish!\n");
        if (row3->IsFinished()) {
            printf ("row3 finish...\n");
        } else {
            printf ("row3 wroking...\n");
        }
        sleep(1);
    }
    return 0;
}

bool finish = false;

void ReadRowCallBack(tera::RowReader* row_reader) {
    printf("ReadRowCallBack call\n");
    while (!row_reader->Done()) {
        printf("ReadRowCallBack, Row: %s\%s\%ld\%s\n",
                row_reader->RowName().c_str(), row_reader->ColumnName().c_str(),
                row_reader->Timestamp(), row_reader->Value().c_str());
        row_reader->Next();
    }
    delete row_reader;
    finish = true;
}

int ReadRowFromTable(tera::Table* table) {
    tera::ErrorCode error_code;
    tera::RowReader* row_reader = table->NewRowReader("com.baidu.www/");
    row_reader->AddColumnFamily("html");
    row_reader->AddColumn("anchor", "www.hao123.com/");
    row_reader->SetMaxVersions(10);
    row_reader->SetAsync();
    row_reader->SetCallBack(ReadRowCallBack);
    // Async Read one row
    table->Get(row_reader);

    while (!finish) {
        sleep(1);
    }

    // Sync Read Batch Rows

    std::vector<tera::RowReader*> rows_reader;
    tera::RowReader* row_reader1 = table->NewRowReader("com.baidu.www/");
    row_reader1->AddColumnFamily("html");
    row_reader1->SetMaxVersions(3);
    row_reader1->SetTimeOut(5000);
    rows_reader.push_back(row_reader1);
    tera::RowReader* row_reader2 = table->NewRowReader("com.baidu.www/");
    row_reader2->AddColumnFamily("anchor");
    row_reader2->SetMaxVersions(3);
    row_reader2->SetTimeOut(5000);
    rows_reader.push_back(row_reader2);
    table->Get(rows_reader);

    tera::RowReader* row_reader3 = table->NewRowReader("com.baidu.www/");
    row_reader3->AddColumnFamily("html");
    row_reader3->SetMaxVersions(10);
    row_reader3->SetTimeOut(5000);
    tera::HATableImpl* ha_table = dynamic_cast<tera::HATableImpl*>(table);
    ha_table->LGet(row_reader3);

    if (row_reader1->GetError().GetType() != tera::ErrorCode::kOK) {
        printf("read1 failed! error: %d, %s\n",
               row_reader1->GetError().GetType(),
               row_reader1->GetError().GetReason().c_str());
    } else {
        while (!row_reader1->Done()) {
            printf("Row1: %s\%s\%ld\%s\n",
                   row_reader1->RowName().c_str(), row_reader1->ColumnName().c_str(),
                   row_reader1->Timestamp(), row_reader1->Value().c_str());
            row_reader1->Next();
        }
    }
    if (row_reader2->GetError().GetType() != tera::ErrorCode::kOK) {
        printf("read2 failed! error: %d, %s\n",
               row_reader2->GetError().GetType(),
               row_reader2->GetError().GetReason().c_str());
    } else {
        while (!row_reader2->Done()) {
            printf("Row2: %s\%s\%ld\%s\n",
                   row_reader2->RowName().c_str(), row_reader2->ColumnName().c_str(),
                   row_reader2->Timestamp(), row_reader2->Value().c_str());
            row_reader2->Next();
        }
    }
    if (row_reader3->GetError().GetType() != tera::ErrorCode::kOK) {
        printf("read3 failed! error: %d, %s\n",
               row_reader3->GetError().GetType(),
               row_reader3->GetError().GetReason().c_str());
    } else {
        while (!row_reader3->Done()) {
            printf("Row3: %s\%s\%ld\%s\n",
                   row_reader3->RowName().c_str(), row_reader3->ColumnName().c_str(),
                   row_reader3->Timestamp(), row_reader3->Value().c_str());
            row_reader3->Next();
        }
    }
    delete row_reader1;
    delete row_reader2;
    delete row_reader3;
    return 0;
}

/// 三维表格
int ShowBigTable(tera::Client* client) {
    tera::ErrorCode error_code;
    // Create
    CreateTable(client);
    // Open
    tera::Table* table = client->OpenTable("webdb", &error_code);
    if (table == NULL) {
        printf("Open table fail: %s\n", tera::strerr(error_code));
        return 1;
    }
    // Write
    ModifyTable(table);
    // Scan
    //ScanTable(table);
    // Read
    ReadRowFromTable(table);
    delete table;
    return 0;
}

int main(int argc, char *argv[]) {
    tera::ErrorCode error_code;
    // 解析命令行参数
    if (!google::ParseCommandLineFlags(&argc, &argv, true)) {
        return 0;
    }

    // 根据配置创建一个client
    std::string conf_path = "./tera_test.flag";
    tera::Client* client = tera::Client::NewClient(conf_path, "tera_sample", &error_code);
    if (client == NULL) {
        printf("New tera client fail: %s,%s\n", tera::strerr(error_code), error_code.GetReason().c_str());
        return 1;
    } else {
        printf("New tera client ok\n");
    }

    ShowBigTable(client);

    printf("Run Over!\n");

    return 0;
}
