// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <readline/history.h>
#include <readline/readline.h>

#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <sstream>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "common/thread_pool.h"
#include "common/base/string_ext.h"
#include "common/base/string_number.h"
#include "common/console/progress_bar.h"
#include "common/file/file_path.h"
#include "io/coding.h"
#include "proto/kv_helper.h"
#include "proto/lb_client.h"
#include "proto/load_balancer_rpc.pb.h"
#include "proto/proto_helper.h"
#include "proto/tabletnode.pb.h"
#include "proto/tabletnode_client.h"
#include "sdk/client_impl.h"
#include "sdk/cookie.h"
#include "sdk/sdk_utils.h"
#include "sdk/sdk_zk.h"
#include "sdk/table_impl.h"
#include "tera.h"
#include "types.h"
#include "utils/crypt.h"
#include "utils/string_util.h"
#include "utils/tprinter.h"
#include "utils/utils_cmd.h"
#include "version.h"

DECLARE_string(flagfile);

// using FLAGS instead of isatty() for compatibility
DEFINE_bool(stdout_is_tty, true, "is stdout connected to a tty");
DEFINE_bool(reorder_tablets, false, "reorder tablets by ts list");
DEFINE_bool(readable, true, "readable input");

DECLARE_string(tera_lb_server_addr);
DECLARE_string(tera_lb_server_port);

tera::TPrinter::PrintOpt g_printer_opt;

using namespace tera;

typedef std::shared_ptr<Table> TablePtr;
typedef std::shared_ptr<TableImpl> TableImplPtr;
typedef std::map<std::string, int32_t(*)(Client*, int32_t, std::string*, ErrorCode*)> CommandTable;

static CommandTable& GetCommandTable() {
    static CommandTable command_table;
    return command_table;
}

static std::string GetServerAddr() {
    return FLAGS_tera_lb_server_addr + ":" + FLAGS_tera_lb_server_port;
}

const char* builtin_cmd_list[] = {
    "safemode",
    "safemode [enter | leave | get]",

    "help",
    "help [cmd]                                                           \n\
          show manual for a or all cmd(s)",

    "version",
    "version                                                              \n\
             show version info",
};

static void PrintCmdHelpInfo(const char* msg) {
    if (msg == NULL) {
        return;
    }
    int count = sizeof(builtin_cmd_list)/sizeof(char*);
    for (int i = 0; i < count; i+=2) {
        if(strncmp(msg, builtin_cmd_list[i], 32) == 0) {
            std::cout << builtin_cmd_list[i + 1] << std::endl;
            return;
        }
    }
}

static void PrintCmdHelpInfo(const std::string& msg) {
    PrintCmdHelpInfo(msg.c_str());
}

static void PrintAllCmd() {
    std::cout << "there is cmd list:" << std::endl;
    int count = sizeof(builtin_cmd_list)/sizeof(char*);
    bool newline = false;
    for (int i = 0; i < count; i+=2) {
        std::cout << std::setiosflags(std::ios::left) << std::setw(20) << builtin_cmd_list[i];
        if (newline) {
            std::cout << std::endl;
            newline = false;
        } else {
            newline = true;
        }
    }

    std::cout << std::endl << "help [cmd] for details." << std::endl;
}

// return false if similar command(s) not found
static bool PromptSimilarCmd(const char* msg) {
    if (msg == NULL) {
        return false;
    }
    bool found = false;
    int64_t len = strlen(msg);
    int64_t threshold = int64_t((len * 0.3 < 3) ? 3 : len * 0.3);
    int count = sizeof(builtin_cmd_list)/sizeof(char*);
    for (int i = 0; i < count; i+=2) {
        if (EditDistance(msg, builtin_cmd_list[i]) <= threshold) {
            if (!found) {
                std::cout << "Did you mean:" << std::endl;
                found = true;
            }
            std::cout << "    " << builtin_cmd_list[i] << std::endl;
        }
    }
    return found;
}

static void PrintUnknownCmdHelpInfo(const char* msg) {
    if (msg != NULL) {
        std::cout << "'" << msg << "' is not a valid command." << std::endl << std::endl;
    }
    if ((msg != NULL)
        && PromptSimilarCmd(msg)) {
        return;
    }
    PrintAllCmd();
}

int32_t SafemodeOp(Client* client, int32_t argc, std::string* argv, ErrorCode* err) {
    if (argc < 3) {
        PrintCmdHelpInfo(argv[1]);
        return -1;
    }

    std::string op = argv[2];
    if (op != "get" && op != "leave" && op != "enter") {
        PrintCmdHelpInfo(argv[1]);
        return -1;
    }

    load_balancer::LBClient lb_client(GetServerAddr());
    CmdCtrlRequest request;
    CmdCtrlResponse response;

    request.set_sequence_id(0);
    request.set_command("safemode");
    request.add_arg_list(op);

    string reason;
    if (lb_client.CmdCtrl(&request, &response)) {
        if (response.status() != tera::kLoadBalancerOk) {
            reason = StatusCodeToString(response.status());
            LOG(ERROR) << reason;
            std::cout << reason << std::endl;
            err->SetFailed(ErrorCode::kSystem, reason);
            return -1;
        }
        if (op == "get") {
            if (response.bool_result()) {
                std::cout << "true" << std::endl;
            } else {
                std::cout << "false" << std::endl;
            }
        }
        return 0;
    } else {
        reason = "fail to CmdCtrl";
        LOG(ERROR) << reason;
        std::cout << reason << std::endl;
        err->SetFailed(ErrorCode::kSystem, reason);
        return -1;
    }
}

int32_t HelpOp(Client*, int32_t argc, std::string* argv, ErrorCode*) {
    if (argc == 2) {
        PrintAllCmd();
    } else if (argc == 3) {
        PrintCmdHelpInfo(argv[2]);
    } else {
        PrintCmdHelpInfo("help");
    }
    return 0;
}

int32_t HelpOp(int32_t argc, char** argv) {
    std::vector<std::string> argv_svec(argv, argv + argc);
    return HelpOp(NULL, argc, &argv_svec[0], NULL);
}

bool ParseCommand(int argc, char** arg_list, std::vector<std::string>* parsed_arg_list) {
    for (int i = 0; i < argc; i++) {
        std::string parsed_arg = arg_list[i];
        if (FLAGS_readable && !ParseDebugString(arg_list[i], &parsed_arg)) {
            std::cout << "invalid debug format of argument: " << arg_list[i] << std::endl;
            return false;
        }
        parsed_arg_list->push_back(parsed_arg);
    }
    return true;
}

static void InitializeCommandTable(){
    CommandTable& command_table = GetCommandTable();
    command_table["safemode"] = SafemodeOp;
    command_table["help"] = HelpOp;
}

int ExecuteCommand(Client* client, int argc, char** arg_list) {
    int ret = 0;
    ErrorCode error_code;

    std::vector<std::string> parsed_arg_list;
    if (!ParseCommand(argc, arg_list, &parsed_arg_list)) {
        return 1;
    }
    std::string* argv = &parsed_arg_list[0];

    CommandTable& command_table = GetCommandTable();
    std::string cmd = argv[1];
    if (cmd == "version") {
        PrintSystemVersion();
    } else if (command_table.find(cmd) != command_table.end()) {
        ret = command_table[cmd](client, argc, argv, &error_code);
    } else {
        PrintUnknownCmdHelpInfo(argv[1].c_str());
        ret = 1;
    }

    if (error_code.GetType() != ErrorCode::kOK) {
        LOG(ERROR) << "fail reason: " << error_code.ToString();
    }
    return ret;
}

int main(int argc, char* argv[]) {
    FLAGS_minloglevel = 2;
    ::google::ParseCommandLineFlags(&argc, &argv, true);

    if (argc > 1 && std::string(argv[1]) == "version") {
        PrintSystemVersion();
        return 0;
    } else if (argc > 1 && std::string(argv[1]) == "help") {
        HelpOp(argc, argv);
        return 0;
    }

    Client* client = Client::NewClient(FLAGS_flagfile, NULL);
    if (client == NULL) {
        LOG(ERROR) << "client instance not exist";
        return -1;
    }
    g_printer_opt.print_head = FLAGS_stdout_is_tty;

    InitializeCommandTable();

    int ret  = 0;
    if (argc == 1) {
        char* line = NULL;
        while ((line = readline("lb> ")) != NULL) {
            char* line_copy = strdup(line);
            std::vector<char*> arg_list;
            arg_list.push_back(argv[0]);
            char* tmp = NULL;
            char* token = strtok_r(line, " \t", &tmp);
            while (token != NULL) {
                arg_list.push_back(token);
                token = strtok_r(NULL, " \t", &tmp);
            }
            if (arg_list.size() == 2 &&
                (strcmp(arg_list[1], "quit") == 0 || strcmp(arg_list[1], "exit") == 0)) {
                free(line_copy);
                free(line);
                break;
            }
            if (arg_list.size() > 1) {
                add_history(line_copy);
                ret = ExecuteCommand(client, arg_list.size(), &arg_list[0]);
            }
            free(line_copy);
            free(line);
        }
    } else {
        ret = ExecuteCommand(client, argc, argv);
    }

    delete client;
    return ret;
}
