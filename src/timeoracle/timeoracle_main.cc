/***************************************************************************
 * 
 * Copyright (c) 2017 Baidu.com, Inc. All Rights Reserved
 * $Id$ 
 * 
 **************************************************************************/
 
 
 
/**
 * @file timeoracle/main.cpp
 * @author chenzongjia(chenzongjia@baidu.com)
 * @date 2017/05/19 14:02:55
 * @version $Revision 
 * @brief 
 *  
 **/

#include <signal.h>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "common/base/scoped_ptr.h"
#include "tera_entry.h"
#include "utils/utils_cmd.h"
#include "version.h"
#include "timeoracle/timeoracle_entry.h"
#include <iostream>

DECLARE_string(tera_log_prefix);

volatile sig_atomic_t g_quit = 0;

static void SignalIntHandler(int sig) {
    g_quit = 1;
}

int main(int argc, char* argv[]) {
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    ::google::InitGoogleLogging(argv[0]);
    if (!FLAGS_tera_log_prefix.empty()) {
        tera::utils::SetupLog(FLAGS_tera_log_prefix);
    } else {
        tera::utils::SetupLog("timeoracle");
    }

    if (argc > 1) {
        std::string ext_cmd = argv[1];
        if (ext_cmd == "version") {
            PrintSystemVersion();
            return 0;
        }
    }

    signal(SIGINT, SignalIntHandler);
    signal(SIGTERM, SignalIntHandler);

    scoped_ptr<tera::timeoracle::TimeoracleEntry> entry(new tera::timeoracle::TimeoracleEntry());

    if (!entry->Start()) {
        return -1;
    }

    while (!g_quit) {
        if (!entry->Run()) {
            LOG(ERROR) << "Server run error ,and then exit now ";
            break;
        }
    }
    if (g_quit) {
        LOG(INFO) << "received interrupt signal from user, will stop";
    }

    if (!entry->Shutdown()) {
        return -1;
    }

    return 0;
}



/* vim: set ts=4 sw=4 sts=4 tw=100 */
