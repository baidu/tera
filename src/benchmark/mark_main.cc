// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author likang01(com@baidu.com)

#include <iomanip>
#include <iostream>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <time.h>

#include "benchmark/mark.h"
#include "types.h"

DECLARE_string(flagfile);
DEFINE_string(tablename, "", "table_name");
DEFINE_string(mode, "w", "mode [w|r|s|m]");
DEFINE_string(type, "async", "type [sync|async]");
DEFINE_int64(pend_size, 100, "max_pending_size");
DEFINE_int64(pend_count, 100000, "max_pending_count");
DEFINE_string(start_key, "", "start_key(scan)");
DEFINE_string(end_key, "", "end_key(scan)");
DEFINE_string(cf_list, "", "cf_list(scan)");
DEFINE_bool(print, false, "print(scan)");
DEFINE_int32(buf_size, 65536, "scan_buffer_size");
DEFINE_bool(verify, true, "md5 verify(writer&read)");
DEFINE_int64(max_outflow, -1, "max_outflow");
DEFINE_int64(max_rate, -1, "max_rate");
DEFINE_bool(scan_streaming, false, "enable streaming scan");
DEFINE_int64(batch_count, 1, "batch_count(sync)");
DEFINE_int64(entry_limit, 0, "writing/reading speed limit");

int mode = 0;
int type = 0;

bool parse_row(const char* buffer, ssize_t size,
               int* op, std::string* row,
               std::map<std::string, std::set<std::string> >* column,
               uint64_t* largest_ts, uint64_t* smallest_ts,
               std::string* value) {
    if (size <= 0) {
        return false;
    }
    const char* end = buffer + size;

    // parse operation
    if (mode == MIX) {
        const char* delim = strchr(buffer, '\t');
        if (buffer == delim || end - 1 <= delim || NULL == delim) {
            return false;
        }
        if (3 != delim - buffer) {
            return false;
        }
        if (strncmp(buffer, "GET", 3) == 0) {
            *op = GET;
        } else if (strncmp(buffer, "PUT", 3) == 0) {
            *op = PUT;
        } else {
            return false;
        }
        buffer = delim + 1;
    }

    // parse row_key
    const char* delim = strchr(buffer, '\t');
    if (buffer == delim || end - 1 == delim) {
        return false;
    }
    if (NULL == delim || end < delim) {
        delim = end;
    }
    row->assign(buffer, delim - buffer);
    if ((delim == end && mode != WRITE && (mode != MIX || *op != PUT)) ||
        (delim == end && mode == DELETE)) {
        return true;
    }

    // parse value
    if (mode == WRITE || (mode == MIX && *op == PUT)) {
        if (delim == end) {
            return false;
        }
        buffer = delim + 1;
        delim = strchr(buffer, '\t');
        if (buffer == delim || end - 1 == delim) {
            return false;
        }
        if (NULL == delim || end < delim) {
            delim = end;
        }
        value->assign(buffer, delim - buffer);
        if (delim == end) {
            return true;
        }
    }

    // parse family:qualifier
    buffer = delim + 1;
    delim = strchr(buffer, '\t');
    if (buffer == delim || end - 1 == delim) {
        return false;
    }
    if (NULL == delim || end < delim) {
        delim = end;
    }
    column->clear();
    const char* column_buffer = buffer;
    if (column_buffer + 1 == delim && *column_buffer == ';') {
        // read whole row
        column_buffer = delim;
    }
    while (column_buffer < delim) {
        const char* semicolon = strchr(column_buffer, ';');
        if (semicolon == column_buffer || semicolon == delim - 1) {
            return false;
        }
        if (NULL == semicolon || semicolon >= delim) {
            semicolon = delim;
        }
        const char* colon = strchr(column_buffer, ':');
        if (colon == column_buffer) {
            return false;
        }
        if (NULL == colon || colon >= semicolon) {
            colon = semicolon;
        }
        std::string family(column_buffer, colon - column_buffer);
        (*column)[family];
        const char* qualifier_buffer = colon + 1;
        while (qualifier_buffer <= semicolon) {
            const char* comma = strchr(qualifier_buffer, ',');
            if (comma == NULL || comma >= semicolon) {
                comma = semicolon;
            }
            std::set<std::string>& qualifiers = (*column)[family];
            std::string qualifier(qualifier_buffer, comma - qualifier_buffer);
            qualifiers.insert(qualifier);
            qualifier_buffer = comma + 1;
        }
        column_buffer = semicolon + 1;
    }
    if (delim == end) {
        return true;
    }

    // parse largest timestamp
    buffer = delim + 1;
    delim = strchr(buffer, '\t');
    if (NULL != delim && end > delim) {
        return false;
    }
    const char* comma = strchr(buffer, ',');
    if (NULL == comma || comma >= end) {
        comma = end;
    }
    if (comma > buffer) {
        std::string time_str(buffer, comma - buffer);
        char* end_time_ptr = NULL;
        uint64_t time = strtoll(time_str.c_str(), &end_time_ptr, 10);
        if (*end_time_ptr != '\0') {
            return false;
        }
        *largest_ts = time;
    }
    if (comma == end) {
        return true;
    } else if (mode == WRITE || (mode == MIX && *op == PUT)) {
        return false;
    }

    // parse smallest timestamp
    buffer = comma + 1;
    if (end > buffer) {
        std::string time_str(buffer, end - buffer);
        char* end_time_ptr = NULL;
        uint64_t time = strtoll(time_str.c_str(), &end_time_ptr, 10);
        if (*end_time_ptr != '\0') {
            return false;
        }
        *smallest_ts = time;
    }
    return true;
}

bool get_next_row(int* op, std::string* row,
                  std::map<std::string, std::set<std::string> >* column,
                  uint64_t* largest_ts, uint64_t* smallest_ts,
                  std::string* value) {
    static size_t n = 10240;
    static char* buffer = new char[n];

    ssize_t line_size = 0;
    while ((line_size = getline(&buffer, &n, stdin)) != -1) {
        if (line_size > 0 && buffer[line_size - 1] == '\n') {
            line_size--;
        }
        if (line_size < 3) {
            std::cerr << "ignore empty line" << std::endl;
            continue;
        }
        if (!parse_row(buffer, line_size, op, row, column,
                       largest_ts, smallest_ts, value)) {
            std::cerr << "ignore invalid line: " << buffer << std::endl;
            continue;
        }
        return true;
    }
    return false;
}

void print_header() {
    std::cout << "HH:MM:SS OPT\t";
    if (mode != SCAN && type == ASYNC) {
        std::cout << "SENT [speed/total]\t\t";
    }
    std::cout << "FINISH [speed/total]\t\t";
    std::cout << "SUCCESS [speed/total]\t\t";
    if (mode != SCAN && type == ASYNC) {
        std::cout << "PENDING [count]";
    }
    std::cout << std::endl;
}

void print_time() {
    struct timeval now;
    gettimeofday(&now, NULL);
    struct tm now_tm;
    localtime_r(&now.tv_sec, &now_tm);
    std::cout << std::setfill('0') << std::setw(2) << now_tm.tm_hour << ":"
        << std::setfill('0') << std::setw(2) << now_tm.tm_min << ":"
        << std::setfill('0') << std::setw(2) << now_tm.tm_sec;
}

void print_opt(Statistic* statistic) {
    int opt = statistic->GetOpt();
    switch (opt) {
    case PUT:
        std::cout << "PUT";
        break;
    case GET:
        std::cout << "GET";
        break;
    case SCN:
        std::cout << "SCN";
        break;
    default:
        abort();
        break;
    }
}

const char unit[] = {'B', 'K', 'M', 'G', 'T', 'P', 'E'};
void print_size_and_count(int64_t size, int64_t count) {
    double dsize = (double)size;
    int unit_index = 0;
    while (dsize > 1024) {
        dsize /= 1024;
        unit_index++;
    }
    std::cout << std::fixed << std::setprecision(3) << dsize
        << unit[unit_index];
    std::cout << "(" << count << ")";
}

void print_statistic(Statistic* statistic) {
    int64_t old_total_count, old_finish_count, old_success_count;
    int64_t old_total_size, old_finish_size, old_success_size;
    statistic->GetLastStatistic(&old_total_count, &old_total_size,
                                &old_finish_count, &old_finish_size,
                                &old_success_count, &old_success_size);

    int64_t new_total_count, new_finish_count, new_success_count;
    int64_t new_total_size, new_finish_size, new_success_size;
    statistic->GetStatistic(&new_total_count, &new_total_size,
                            &new_finish_count, &new_finish_size,
                            &new_success_count, &new_success_size);

    int64_t total_count = new_total_count - old_total_count;
    int64_t finish_count = new_finish_count - old_finish_count;
    int64_t success_count = new_success_count - old_success_count;
    int64_t total_size = new_total_size - old_total_size;
    int64_t finish_size = new_finish_size - old_finish_size;
    int64_t success_size = new_success_size - old_success_size;

    int64_t total_pending_count = new_total_count - new_finish_count;
    // scan
    if (total_pending_count < 0) {
        total_pending_count = 0;
    }

    print_time();
    std::cout << " ";
    print_opt(statistic);
    std::cout << "\t";

    if (mode != SCAN && type == ASYNC) {
        print_size_and_count(new_total_size, new_total_count);
        std::cout << "/";
        print_size_and_count(total_size, total_count);
        std::cout << "\t\t";
    }

    print_size_and_count(new_finish_size, new_finish_count);
    std::cout << "/";
    print_size_and_count(finish_size, finish_count);
    std::cout << "\t\t";

    print_size_and_count(new_success_size, new_success_count);
    std::cout << "/";
    print_size_and_count(success_size, success_count);
    std::cout << "\t\t";

    if (mode != SCAN && type == ASYNC) {
        std::cout << total_pending_count;
    }
    std::cout << std::endl;
}

void print_marker(Marker* marker) {
    std::cout << "MinLatency: " << marker->MinLatency() << " "
              << "AverageLatency: " << marker->AverageLatency() << " "
              << "MaxLatency: " << marker->MaxLatency() << "\n"
              << "90thPercentileLatency: " << marker->PercentileLatency(90) << " "
              << "95thPercentileLatency: " << marker->PercentileLatency(95) << " "
              << "99thPercentileLatency: " << marker->PercentileLatency(99)
              << std::endl;
}

void print_marker(Statistic* statistic) {
    std::cout << " [FINISH]" << std::endl;
    Marker* finish_marker = statistic->GetFinishMarker();
    print_marker(finish_marker);
    std::cout << " [SUCCESS]" << std::endl;
    Marker* success_marker = statistic->GetSuccessMarker();
    print_marker(success_marker);
}

void* print_proc(void* param) {
    Adapter* adapter = (Adapter*)param;
    usleep(1000000);
    int64_t count = 0;
    for (;;) {
        if (count % 10 == 0) {
            std::cout << std::endl;
            print_header();
        }

        struct timeval now;
        gettimeofday(&now, NULL);
        usleep(1000000 - now.tv_usec);

        switch (mode) {
        case WRITE:
            print_statistic(adapter->GetWriteMarker());
            break;
        case DELETE:
            print_statistic(adapter->GetWriteMarker());
            break;
        case READ:
            print_statistic(adapter->GetReadMarker());
            break;
        case SCAN:
            print_statistic(adapter->GetScanMarker());
            break;
        case MIX:
            print_statistic(adapter->GetWriteMarker());
            print_statistic(adapter->GetReadMarker());
            break;
        default:
            abort();
            break;
        }

        if (count % 10 == 9) {
            std::cout << std::endl;
            switch (mode) {
            case WRITE:
                std::cout << "[PUT MARKER]" << std::endl;
                print_marker(adapter->GetWriteMarker());
                break;
            case DELETE:
                std::cout << "[DEL MARKER]" << std::endl;
                print_marker(adapter->GetWriteMarker());
                break;
            case READ:
                std::cout << "[GET MARKER]" << std::endl;
                print_marker(adapter->GetReadMarker());
                break;
            case SCAN:
                std::cout << "[SCN MARKER]" << std::endl;
                print_marker(adapter->GetScanMarker());
                break;
            case MIX:
                std::cout << "[PUT MARKER]" << std::endl;
                print_marker(adapter->GetWriteMarker());
                std::cout << "[GET MARKER]" << std::endl;
                print_marker(adapter->GetReadMarker());
                break;
            default:
                abort();
                break;
            }
        }

        count++;
    }
}

void print_summary(Statistic* marker, double duration) {
    int64_t total_count, finish_count, success_count;
    int64_t total_size, finish_size, success_size;
    marker->GetStatistic(&total_count, &total_size,
                         &finish_count, &finish_size,
                         &success_count, &success_size);

    print_opt(marker);
    std::cout.precision(3);
    std::cout << " Summary: " << std::fixed << duration << " s\n"
        << "    total: " << finish_size << " bytes "
                         << finish_count << " records "
                         << (double)finish_size / 1048576 / duration << " MB/s\n"
        << "     succ: " << success_size << " bytes "
                         << success_count << " records "
                         << (double)success_size / 1048576 / duration << " MB/s"
        << std::endl;
}

void print_summary_proc(Adapter* adapter, double duration) {
    switch (mode) {
    case WRITE:
        print_summary(adapter->GetWriteMarker(), duration);
        break;
    case DELETE:
        print_summary(adapter->GetWriteMarker(), duration);
        break;
    case READ:
        print_summary(adapter->GetReadMarker(), duration);
        break;
    case SCAN:
        print_summary(adapter->GetScanMarker(), duration);
        break;
    case MIX:
        print_summary(adapter->GetWriteMarker(), duration);
        print_summary(adapter->GetReadMarker(), duration);
        break;
    default:
        abort();
        break;
    }

    std::cout << std::endl;
    switch (mode) {
    case WRITE:
        std::cout << "[PUT MARKER]" << std::endl;
        print_marker(adapter->GetWriteMarker());
        break;
    case DELETE:
        std::cout << "[DEL MARKER]" << std::endl;
        print_marker(adapter->GetWriteMarker());
        break;
    case READ:
        std::cout << "[GET MARKER]" << std::endl;
        print_marker(adapter->GetReadMarker());
        break;
    case SCAN:
        std::cout << "[SCN MARKER]" << std::endl;
        print_marker(adapter->GetScanMarker());
        break;
    case MIX:
        std::cout << "[PUT MARKER]" << std::endl;
        print_marker(adapter->GetWriteMarker());
        std::cout << "[GET MARKER]" << std::endl;
        print_marker(adapter->GetReadMarker());
        break;
    default:
        abort();
        break;
    }
}

int main(int argc, char** argv) {
    ::google::ParseCommandLineFlags(&argc, &argv, true);

    tera::ErrorCode err;
    tera::Client* client = tera::Client::NewClient("", "tera_mark");
    if (NULL == client) {
        std::cerr << "fail to create client: " << tera::strerr(err) << std::endl;
        return -1;
    }

    tera::Table* table = client->OpenTable(FLAGS_tablename, &err);
    if (NULL == table) {
        std::cerr << "fail to open table: " << tera::strerr(err) << std::endl;
        return -1;
    }

    std::vector<std::string> scan_cf_list;
    if (FLAGS_mode.compare("w") == 0) {
        mode = WRITE;
    } else if (FLAGS_mode.compare("r") == 0) {
        mode = READ;
    } else if (FLAGS_mode.compare("d") == 0) {
        mode = DELETE;
    } else if (FLAGS_mode.compare("s") == 0) {
        mode = SCAN;
        size_t delim = 0;
        size_t cf_pos = 0;
        while (std::string::npos != (delim = FLAGS_cf_list.find(',', cf_pos))) {
            if (cf_pos < delim) {
                scan_cf_list.push_back(std::string(FLAGS_cf_list, cf_pos, delim - cf_pos));
            }
            cf_pos = delim + 1;
        }
    } else if (FLAGS_mode.compare("m") == 0) {
        mode = MIX;
    } else {
        std::cerr << "unsupport mode: " << FLAGS_mode << std::endl;
        return -1;
    }

    if (FLAGS_type.compare("sync") == 0) {
        type = SYNC;
    } else if (FLAGS_type.compare("async") == 0) {
        type = ASYNC;
    } else {
        std::cerr << "unsupport type: " << FLAGS_type << std::endl;
        return -1;
    }

    Adapter* adapter = new Adapter(table);

    pthread_t print_thread;
    if (0 != pthread_create(&print_thread, NULL, &print_proc, adapter)) {
        std::cerr << "cannot create thread";
        return -1;
    }

    std::cout << "begin ..." << std::endl;
    timeval start_time;
    gettimeofday(&start_time, NULL);

    int opt = NONE;
    std::string row;
    std::map<std::string, std::set<std::string> > column;
    uint64_t largest_ts = tera::kLatestTs;
    uint64_t smallest_ts = tera::kOldestTs;
    std::string value;

    int last_opt = NONE;
    bool finish = false;
    int64_t count = 0;
    while (true) {
        if (FLAGS_entry_limit != 0 && count == FLAGS_entry_limit) {
            struct timeval now;
            gettimeofday(&now, NULL);
            if (1000000 - now.tv_usec > 0)  {
                usleep(1000000 - now.tv_usec);
            }
            count = 0;
        }
        switch (mode) {
        case WRITE:
            opt = PUT;
            finish = !get_next_row(NULL, &row, &column, &largest_ts, NULL, &value);
            break;
        case READ:
            opt = GET;
            finish = !get_next_row(NULL, &row, &column, &largest_ts, &smallest_ts, NULL);
            break;
        case DELETE:
            opt = DEL;
            finish = !get_next_row(NULL, &row, &column, NULL, NULL, NULL);
            break;
        case MIX:
            finish = !get_next_row(&opt, &row, &column, &largest_ts, &smallest_ts, &value);
            break;
        case SCAN:
            adapter->Scan(FLAGS_start_key, FLAGS_end_key, scan_cf_list, FLAGS_print,
                          FLAGS_scan_streaming);
            finish = true;
            break;
        default:
            abort();
            break;
        }

        if (finish) {
            if (type == SYNC) {
                if (mode == WRITE || mode == DELETE || mode == MIX) {
                    adapter->CommitSyncWrite();
                }
                if (mode == READ || mode == MIX) {
                    adapter->CommitSyncRead();
                }
            }
            break;
        }

        switch (opt) {
        case PUT:
            if (type == SYNC && mode == MIX && last_opt == GET) {
                adapter->CommitSyncRead();
            }
            adapter->Write(row, column, largest_ts, value);
            break;
        case GET:
            if (type == SYNC && mode == MIX && last_opt == PUT) {
                adapter->CommitSyncWrite();
            }
            adapter->Read(row, column, largest_ts, smallest_ts);
            break;
        case DEL:
            adapter->Delete(row, column);
            break;
        default:
            abort();
            break;
        }
        last_opt = opt;

        opt = NONE;
        row.clear();
        column.clear();
        largest_ts = tera::kLatestTs;
        smallest_ts = tera::kOldestTs;
        value.clear();
        count += 1;
    }

    std::cout << "wait for completion..." << std::endl;
    adapter->WaitComplete();

    timeval finish_time;
    gettimeofday(&finish_time, NULL);
    double duration = (finish_time.tv_sec - start_time.tv_sec)
        + (double)(finish_time.tv_usec - start_time.tv_usec) / 1000000.0;

    pthread_cancel(print_thread);

    print_summary_proc(adapter, duration);
    delete adapter;

    usleep(100000);
    return 0;
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
