// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//


#include <stdio.h>
#include <stdlib.h>

#include <iostream>
#include <fstream>
#include <limits>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "common/file/file_path.h"
#include "proto/monitor.pb.h"
#include "proto/tabletnode.pb.h"
#include "sdk/tera.h"
#include "utils/utils_cmd.h"
#include "utils/timer.h"

DEFINE_string(tera_monitor_default_request_filename, "tera_monitor.request", "");
DEFINE_string(tera_monitor_default_response_filename, "tera_monitor.response", "");

DECLARE_string(flagfile);
DECLARE_string(log_dir);
DECLARE_string(tera_master_stat_table_name);
DECLARE_string(tera_sdk_conf_file);
DECLARE_string(tera_zk_addr_list);
DECLARE_string(tera_zk_root_path);
DECLARE_string(tera_ins_addr_list);
DECLARE_string(tera_ins_root_path);
DECLARE_bool(tera_zk_enabled);
DECLARE_bool(tera_ins_enabled);
DECLARE_int64(tera_master_stat_table_interval);

using namespace tera;
using std::string;

void FillTabletNodeStat(const TabletNodeInfo& info, TabletNodeStat* stat) {
    stat->set_timestamp(info.timestamp());
    stat->set_load(info.load());
    stat->set_tablet_total(info.tablet_total());
    stat->set_tablet_onbusy(info.tablet_onbusy());

    stat->set_low_read_cell(info.low_read_cell());
    stat->set_scan_rows(info.scan_rows());
    stat->set_scan_size(info.scan_size());
    stat->set_read_rows(info.read_rows());
    stat->set_read_size(info.read_size());
    stat->set_write_rows(info.write_rows());
    stat->set_write_size(info.write_size());

    stat->set_mem_used(info.mem_used());
    stat->set_net_tx(info.net_tx());
    stat->set_net_rx(info.net_rx());
    stat->set_dfs_io_r(info.dfs_io_r());
    stat->set_dfs_io_w(info.dfs_io_w());
    stat->set_local_io_r(info.local_io_r());
    stat->set_local_io_w(info.local_io_w());

    stat->set_cpu_usage(info.cpu_usage());

    stat->set_status_m(info.status_m());
    stat->set_tablet_onload(info.tablet_onload());
    stat->set_tablet_onsplit(info.tablet_onsplit());

    stat->set_read_pending(info.read_pending());
    stat->set_write_pending(info.write_pending());
    stat->set_scan_pending(info.scan_pending());

    for (int i = 0; i < info.extra_info_size(); ++i) {
        ExtraStat* estat = stat->add_extra_stat();
        estat->set_name(info.extra_info(i).name());
        estat->set_value(info.extra_info(i).value());
    }
}

void FillTabletNodeStats(std::list<string>& raw_stats, TabletNodeStats* stat_list) {
    int64_t last_timestamp = 0;
    int64_t interval = FLAGS_tera_master_stat_table_interval * 1000000;
    std::list<string>::iterator it = raw_stats.begin();
    for (; it != raw_stats.end(); ++it) {
        TabletNodeStat* stat = stat_list->add_stat();
        TabletNodeInfo info;
        info.ParseFromString(*it);
        if (last_timestamp != 0) {
            while ((int64_t)info.timestamp() - last_timestamp > interval * 3 / 2) {
                last_timestamp += interval;
                FillTabletNodeStat(TabletNodeInfo(), stat);
                stat->set_timestamp(last_timestamp);
                stat = stat_list->add_stat();
            }
        }
        last_timestamp = info.timestamp();
        FillTabletNodeStat(info, stat);
    }
    if (stat_list->stat_size() > 0) {
        stat_list->set_av_ratio(raw_stats.size() * 1000000 / stat_list->stat_size());
    }
}

void ParseStartEndTime(const MonitorRequest& request,
                       int64_t* min_time,
                       int64_t* max_time) {
    *min_time = request.min_timestamp();
    if (request.max_timestamp() > 0) {
        *max_time = request.max_timestamp();
    } else {
        *max_time = std::numeric_limits<int64_t>::max();
    }
}

int ListTabletNodes(Table* table,
                    const MonitorRequest& request,
                    MonitorResponse* response) {
    ScanDescriptor desc("#");
    desc.SetEnd("$");
    desc.SetBufferSize((1024 << 10));
    desc.SetAsync(false);

    ErrorCode err;
    ResultStream* stream = table->Scan(desc, &err);
    while (!stream->Done()) {
        string addr = stream->RowName().substr(1, stream->RowName().size() - 1);
        TabletNodeStats* stat_list = response->add_stat_list();
        stat_list->set_addr(addr);
        stream->Next();
    }
    delete stream;
    return 0;
}

int GetPartTabletNodes(Table* table,
                        const MonitorRequest& request,
                        MonitorResponse* response) {
    int64_t min_time, max_time;
    ParseStartEndTime(request, &min_time, &max_time);
    ErrorCode err;
    int ts_num = request.tabletnodes_size();
    if (ts_num == 0) {
        response->set_errmsg("none tabletnodes");
        return -1;
    }

    for (int i = 0; i < ts_num; ++i) {
        const string& cur_ts = request.tabletnodes(i);
        ScanDescriptor desc(cur_ts);
        desc.SetEnd(cur_ts + "a");
        desc.SetBufferSize((1024 << 10));
        desc.SetAsync(false);

        ResultStream* stream = table->Scan(desc, &err);
        std::list<string> stats;
        while (!stream->Done()) {
            if (stream->Timestamp() > max_time) {
                // skip out-time-range records
            } else if (stream->Timestamp() < min_time ||
                       stream->RowName().find(cur_ts) == string::npos) {
                // skip rest records
                break;
            } else if (stream->Family() == "tsinfo") {
                stats.push_front(stream->Value());
            }
            stream->Next();
        }
        delete stream;

        TabletNodeStats* stat_list = response->add_stat_list();
        stat_list->set_addr(cur_ts);
        FillTabletNodeStats(stats, stat_list);
        // fill response
    }
    return 0;
}

int GetAllTabletNodes(Table* table,
                      const MonitorRequest& request,
                      MonitorResponse* response) {
    int64_t min_time, max_time;
    ParseStartEndTime(request, &min_time, &max_time);

    ScanDescriptor desc("A");
    desc.SetEnd("");
    desc.SetBufferSize((1024 << 10));
    desc.SetAsync(false);

    ErrorCode err;
    ResultStream* stream;
    stream = table->Scan(desc, &err);
    std::cout << err.GetReason() << std::endl;
    int ts_count = 0;
    while (!stream->Done()) {
        int slen = stream->RowName().size() - 16;
        string cur_ts = stream->RowName().substr(0, slen);
        std::list<string> stats;
        while (!stream->Done()) {
            if (string::npos == stream->RowName().find(cur_ts)) {
                break;
            }
            if (stream->Timestamp() >= max_time ||
                stream->Timestamp() < min_time) { // [min_time, max_time)
                // skip out-time-range records
            } else if (stream->Family() == "tsinfo") {
                stats.push_front(stream->Value());
            }
            stream->Next();
        }

        // fill response
        TabletNodeStats* stat_list = response->add_stat_list();
        stat_list->set_addr(cur_ts);
        FillTabletNodeStats(stats, stat_list);
        LOG(INFO) << "get stat finish: " << cur_ts << ", " << ts_count++;
    }
    delete stream;
    return 0;
}

int FillResponse(const MonitorRequest& request, MonitorResponse* response) {
    ErrorCode err_code;
    string tablename = FLAGS_tera_master_stat_table_name;

    Client* client = Client::NewClient(FLAGS_flagfile);
    if (client == NULL) {
        LOG(ERROR) << "client instance not exist";
        response->set_errmsg("system error");
        return -3;
    }
    Table* table = client->OpenTable(tablename, &err_code);
    if (table == NULL) {
        LOG(ERROR) << "fail to open stat table: " << tablename;
        response->set_errmsg("system error");
        return -4;
    }

    switch (request.cmd()) {
    case kList:
        ListTabletNodes(table, request, response);
        break;
    case kGetAll:
        GetAllTabletNodes(table, request, response);
        break;
    case kGetPart:
        GetPartTabletNodes(table, request, response);
        break;
    default:
        LOG(ERROR) << "request cmd error.";
        response->set_errmsg("cmd error");
        return -1;
    }
    delete table;
    delete client;
    return 0;
}

void InitFlags(int32_t argc, char** argv, const MonitorRequest& request) {
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    if (FLAGS_flagfile.empty()) {
        string found_path;
        if (!FLAGS_tera_sdk_conf_file.empty()) {
            found_path = FLAGS_tera_sdk_conf_file;
        } else {
            found_path = utils::GetValueFromEnv("tera_CONF");
            if (!found_path.empty() || found_path == "") {
                found_path = "tera.flag";
            }
        }

        if (!found_path.empty() && IsExist(found_path)) {
            VLOG(5) << "config file is not defined, use default one: "
                << found_path;
            FLAGS_flagfile = found_path;
        } else if (IsExist("./tera.flag")) {
            VLOG(5) << "config file is not defined, use default one: ./tera.flag";
            FLAGS_flagfile = "./tera.flag";
        }
    }

    // init log dir
    /*
    if (FLAGS_log_dir.empty()) {
            FLAGS_log_dir = "./";
    }

    ::google::ParseCommandLineFlags(&argc, &argv, true);
    ::google::InitGoogleLogging(argv[0]);
    utils::SetupLog(argv[0]);
    */
    if (request.use_nexus()) {
        if (request.has_tera_zk_addr()) {
            FLAGS_tera_ins_addr_list = request.tera_zk_addr();
        }
        if (request.has_tera_zk_root()) {
            FLAGS_tera_ins_root_path = request.tera_zk_root();
        }
        FLAGS_tera_ins_enabled = true;
        FLAGS_tera_zk_enabled = false;
    } else {
        if (request.has_tera_zk_addr()) {
            FLAGS_tera_zk_addr_list = request.tera_zk_addr();
        }
        if (request.has_tera_zk_root()) {
            FLAGS_tera_zk_root_path = request.tera_zk_root();
        }
    }
}

int DumpResponse(const string& resfile, const MonitorResponse& response) {
    string res;
    if (!response.SerializeToString(&res)) {
        LOG(ERROR) << "fail to serialize response to string.";
        return -1;
    }

    FILE* fp;
    if ((fp = fopen(resfile.data(), "wb")) == NULL) {
        LOG(ERROR) << "fail to open " << resfile;
        return -1;
    }
    fwrite(res.data(), 1, res.size(), fp);
    fclose(fp);
    return 0;
}

int ParseRequest(const string& reqfile, MonitorRequest* request) {
    FILE* fp;
    const int kLenMax = 1024000;
    char buf[kLenMax];
    int len;
    if ((fp = fopen(reqfile.data(), "rb")) == NULL) {
        LOG(ERROR) << "fail to open " << reqfile;
        return -1;
    }
    len = fread(buf, 1, kLenMax, fp);
    fclose(fp);

    if (!request->ParseFromString(string(buf, len))) {
        LOG(ERROR) << "fail to parse monitor request, file: " << reqfile
            << ", len: " << len;
        return -2;
    }
    return 0;
}

void PrintResponse(const MonitorResponse& response) {
    /*
    for (int i = 0; i < response.stat_list(0).stat_size(); ++i) {
        int64_t total = 0;
        uint64_t t_time = 0;
        int j;
        int ts_count = 0;
        for (j = 0; j < response.stat_list_size(); ++j) {
            const TabletNodeStats& stat_list = response.stat_list(j);
            if (stat_list.stat_size() <= i) {
                continue;
            }
            ts_count++;
            total += stat_list.stat(i).write_rows();
            t_time += stat_list.stat(i).timestamp();
        }
        printf("%20lu%10lu%14ld%6d\n",
                t_time / ts_count, total / ts_count, total, ts_count);
    }
      */
    for (int i = 0; i < response.stat_list_size(); ++i) {
        const TabletNodeStats& stat_list = response.stat_list(i);
        for (int j = 0; j < stat_list.stat_size(); ++j) {
            const TabletNodeStat& stat = stat_list.stat(j);
            std::cout << stat.ShortDebugString() << " ";
            for (int k = 0; k < stat.extra_stat_size(); ++k) {
                ExtraStat extra_stat = stat.extra_stat(k);
                if (extra_stat.name() == "rand_read_delay") {
                    std::cout << extra_stat.name() << ": " << extra_stat.value() << " ";
                }
            }
            std::cout << std::endl;
        }
    }
}

void PrintResponseFile(const string resfile) {
    FILE* fp;
    const int kLenMax = 1024000;
    char buf[kLenMax];
    string res;
    MonitorResponse response;
    int len = kLenMax;;
    if ((fp = fopen(resfile.data(), "rb")) == NULL) {
        LOG(ERROR) << "fail to open " << resfile;
        return;
    }
    while (len == kLenMax) {
        len = fread(buf, 1, kLenMax, fp);
        res.append(string(buf, len));
    }
    fclose(fp);

    if (!response.ParseFromString(res)) {
        LOG(ERROR) << "fail to parse monitor response, file: " << resfile
            << ", len: " << len;
        return;
    }
    PrintResponse(response);
}

void TEST_FillListRequest(MonitorRequest* request) {
    request->set_cmd(tera::kList);
}

void TEST_FillGetPartRequest(MonitorRequest* request) {
    request->set_cmd(tera::kGetPart);
    request->add_tabletnodes("nj02-stest-tera1.nj02.baidu.com:7702");
    uint64_t cur_time = get_micros() - 100 * 1000000;
    request->set_max_timestamp(cur_time);
    request->set_min_timestamp(cur_time - 30 * 60 * 1000000);
}

void TEST_FillGetAllRequest(MonitorRequest* request) {
    request->set_cmd(tera::kGetAll);
    request->set_min_timestamp(0);
    request->set_max_timestamp(std::numeric_limits<int64_t>::max());
}

void Eva_FillGetInfoRequest(MonitorRequest* request, const std::string& ts_start, const std::string& ts_end, const std::string& ts) {
    std::stringstream ss;
    int64_t start, end;
    ss << ts_start;
    ss >> start;
    std::stringstream se;
    se << ts_end;
    se >> end;
    request->set_min_timestamp(start);
    request->set_max_timestamp(end);
    if (ts != "") {
        std::ifstream in;
        in.open(ts.c_str());
        if (!in) {
            LOG(ERROR) << "fail to open file: " << ts;
            return;
        }
        while (!in.eof()) {
            std::string addr;
            in >> addr;
            request->add_tabletnodes(addr);
        }
        request->set_cmd(tera::kGetPart);
    } else {
        request->set_cmd(tera::kGetAll);
    }
}


int main(int argc, char* argv[]) {
    int ret = 0;
    string reqfile = FLAGS_tera_monitor_default_request_filename;
    string resfile = FLAGS_tera_monitor_default_response_filename;
    MonitorRequest request;
    MonitorResponse response;
    if (argc < 2) {
        // scan all
    } else if (string(argv[1]) == "print") {
        // print response file
        string resfile = argv[2];
        PrintResponseFile(resfile);
        return 0;
    } else if (string(argv[1]) == "testlist") {
        TEST_FillListRequest(&request);
    } else if (string(argv[1]) == "testgetpart") {
        TEST_FillGetPartRequest(&request);
    } else if (string(argv[1]) == "testgetall") {
        TEST_FillGetAllRequest(&request);
    } else if (string(argv[1]) == "eva") { // ./teramo eva timestamp_strat timestamp_end
        Eva_FillGetInfoRequest(&request, argv[2], argv[3], "");
    } else if (string(argv[1]) == "trace") { // ./teramo eva timestamp_strat timestamp_end
        Eva_FillGetInfoRequest(&request, argv[2], argv[3], argv[4]);
        resfile = string(argv[4]) + ".response";
    } else {
        reqfile = argv[1];
        if (argc >= 3) {
            resfile = argv[2];
        }
        ret = ParseRequest(reqfile, &request);
        if (ret < 0) {
            std::cout << ret << std::endl;
            return ret;
        }
    }
    InitFlags(argc, argv, request);

    ret = FillResponse(request, &response);
    if (ret < 0) {
        std::cout << ret << std::endl;
        return ret;
    }
    if (string(argv[1]) == "testlist" ||
        string(argv[1]) == "testgetpart" ||
        string(argv[1]) == "testgetall") {
        PrintResponse(response);
    }

    ret = DumpResponse(resfile, response);
    std::cout << ret << std::endl;

    return ret;
}
