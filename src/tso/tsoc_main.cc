// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <chrono>
#include <iostream>
#include <map>
#include <string>
#include <thread>

#include "common/counter.h"
#include "tso/tso.h"
#include "version.h"

using namespace std::placeholders;

static tera::tso::TimestampOracle* tso = NULL;

tera::tso::TimestampOracle* GetTsoInstance() {
    if (tso == NULL) {
        tso = new tera::tso::TimestampOracle;
    }
    return tso;
}

int Get(int argc, char** argv) {
    if (argc > 2) {
        std::cerr << "too many arguments for GET cmd" << std::endl;
        return -1;
    }
    int64_t ts = GetTsoInstance()->GetTimestamp();
    std::cout << ts << std::endl;
    return 0;
}

static common::Counter pending_counter;
static common::Counter finish_counter;

void PrintTestSpeed() {
    int64_t last_counter = 0;
    while (true) {
        int64_t tmp = finish_counter.Get();
        int64_t speed = tmp - last_counter;
        last_counter = tmp;
        std::cout << speed << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
}

void GetTestCallback(int64_t timestamp) {
    int64_t finish = finish_counter.Inc();
    if (finish % 100000 == 0) {
        std::cout << "finish " << finish << std::endl;
    }
    pending_counter.Dec();
}

int GetTest(int argc, char** argv) {
    if (argc > 2) {
        std::cerr << "too many arguments for GET cmd" << std::endl;
        return -1;
    }
    std::thread print_thread(PrintTestSpeed);
    while (true) {
        while (pending_counter.Get() > 1000000) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        pending_counter.Inc();
        GetTsoInstance()->GetTimestamp(std::bind(&GetTestCallback, _1));
    }
    return 0;
}

void PrintHelp(char* program) {
    std::cout << program << " "
              << "help\n"
              << "version\n"
              << "get\n"
              << "test"
              << std::endl;
}

typedef std::map<std::string, int(*)(int, char** argv)> CommandTable;
static CommandTable command_table;

static void InitializeCommandTable() {
    command_table["get"] = Get;
    command_table["test"] = GetTest;
}

int main(int argc, char** argv) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    if (argc == 1) {
        PrintHelp(argv[0]);
        return 0;
    }
    std::string cmd = argv[1];
    if (cmd == "help") {
        PrintHelp(argv[0]);
        return 0;
    }
    if (cmd == "version") {
        PrintSystemVersion();
        return 0;
    }
    InitializeCommandTable();
    if (command_table.find(cmd) == command_table.end()) {
        PrintHelp(argv[0]);
        return -1;
    }
    return command_table[cmd](argc, argv);
}
