// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author likang01(com@baidu.com)

#include "sample.h"

#include <openssl/md5.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <time.h>

#include <iomanip>
#include <iostream>

DECLARE_string(flagfile);
DEFINE_string(tablename, "", "table_name");
DEFINE_string(mode, "w", "mode [w|r|s]");
DEFINE_string(type, "async", "type [sync|async]");
DEFINE_int64(pend_size, 100, "max_pending_size");
DEFINE_int64(pend_count, 100000, "max_pending_count");
DEFINE_string(start_key, "", "start_key(scan)");
DEFINE_string(end_key, "", "end_key(scan)");
DEFINE_string(cf_list, "", "cf_list(scan)");
DEFINE_bool(print, false, "print(scan)");
DEFINE_int32(buf_size, 65536, "scan_buffer_size");
DEFINE_bool(verify, true, "md5 verify(writer&read)");
DEFINE_int32(max_outflow, -1, "max_outflow");
DEFINE_int32(max_rate, -1, "max_rate");
DEFINE_bool(scan_streaming, false, "enable streaming scan");

void add_md5sum(const std::string& rowkey, const std::string& family,
                const std::string& qualifier, std::string* value) {
    if (value == NULL) {
        return;
    }
    MD5_CTX md5_ctx;
    MD5_Init(&md5_ctx);
    unsigned char md5_out[MD5_DIGEST_LENGTH] = {'\0'};
    MD5_Update(&md5_ctx, rowkey.data(), rowkey.size());
    MD5_Update(&md5_ctx, family.data(), family.size());
    MD5_Update(&md5_ctx, qualifier.data(), qualifier.size());
    MD5_Update(&md5_ctx, value->data(), value->size());

    MD5_Final(md5_out, &md5_ctx);
    value->append((char*)md5_out, MD5_DIGEST_LENGTH);
}

bool verify_md5sum(const std::string& rowkey, const std::string& family,
                   const std::string& qualifier, const std::string& value) {
    if (value.size() <= MD5_DIGEST_LENGTH) {
        return false;
    }
    MD5_CTX md5_ctx;
    MD5_Init(&md5_ctx);
    unsigned char md5_out[MD5_DIGEST_LENGTH] = {'\0'};
    MD5_Update(&md5_ctx, rowkey.data(), rowkey.size());
    MD5_Update(&md5_ctx, family.data(), family.size());
    MD5_Update(&md5_ctx, qualifier.data(), qualifier.size());
    MD5_Update(&md5_ctx, value.data(), value.size() - MD5_DIGEST_LENGTH);
    MD5_Final(md5_out, &md5_ctx);
    return (0 == strncmp((char*)md5_out,
                         value.data() + value.size() - MD5_DIGEST_LENGTH,
                         MD5_DIGEST_LENGTH));
}

bool parse_row(const char* buffer, ssize_t size, std::string* row,
               std::string* family, std::string* qualifier,
               uint64_t* timestamp, std::string* value) {
    if (size <= 0) {
        return false;
    }
    const char* end = buffer + size;

    // parse row_key
    const char* delim = strchr(buffer, '\t');
    if (buffer == delim || end - 1 == delim) {
        return false;
    }
    if (NULL == delim || end < delim) {
        delim = end;
    }
    if (row != NULL) {
        row->assign(buffer, delim - buffer);
    }
    if (NULL == value && delim == end) {
        return true;
    }

    // parse value
    if (value != NULL) {
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
    const char* colon = strchr(buffer, ':');
    if (NULL == colon || colon >= delim) {
        return false;
    }
    if (colon > buffer && family != NULL) {
        family->assign(buffer, colon - buffer);
    }
    if (colon < delim - 1 && qualifier != NULL) {
        qualifier->assign(colon + 1, delim - colon - 1);
    }
    if (delim == end) {
        return true;
    }

    // parse timestamp
    buffer = delim + 1;
    delim = strchr(buffer, '\t');
    if (NULL != delim && end > delim) {
        return false;
    }
    std::string time_str(buffer, end - buffer);
    char* end_time_ptr = NULL;
    uint64_t time = strtoll(time_str.c_str(), &end_time_ptr, 10);
    if (*end_time_ptr != '\0') {
        return false;
    }
    if (timestamp != NULL) {
        *timestamp = time;
    }
    return true;
}

bool get_next_row(std::string* row, std::string* family,
                  std::string* qualifier, uint64_t* timestamp,
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
        if (!parse_row(buffer, line_size, row, family, qualifier,
                       timestamp, value)) {
            std::cerr << "ignore invalid line: " << buffer << std::endl;
            continue;
        }
        return true;
    }
    return false;
}

void sync_write_proc(IWriter* writer) {
    std::string row;
    std::string family;
    std::string qualifier;
    uint64_t timestamp = 0;
    std::string value;
    while (get_next_row(&row, &family, &qualifier, &timestamp, &value)) {
        if (FLAGS_verify) {
            add_md5sum(row, family, qualifier, &value);
        }
        writer->Write(row, family, qualifier, timestamp, value);
        row.clear();
        family.clear();
        qualifier.clear();
        timestamp = 0;
        value.clear();
    }
}

void async_write_proc(IWriter* writer) {
    ((AsyncWriter*)writer)->SetMaxPendingSize(FLAGS_pend_size);
    ((AsyncWriter*)writer)->SetMaxPendingCount(FLAGS_pend_count);
    std::string row;
    std::string family;
    std::string qualifier;
    uint64_t timestamp = 0;
    std::string value;
    while (get_next_row(&row, &family, &qualifier, &timestamp, &value)) {
        if (FLAGS_verify) {
            add_md5sum(row, family, qualifier, &value);
        }
        writer->Write(row, family, qualifier, timestamp, value);
        row.clear();
        family.clear();
        qualifier.clear();
        timestamp = 0;
        value.clear();
    }
}

void* print_status_proc(void* arg) {
    IAdapter* adapter = (IAdapter*)arg;
    int64_t old_total_count = 0, old_finish_count = 0, old_success_count = 0;
    int64_t old_total_size = 0, old_finish_size = 0, old_success_size = 0;
    int64_t total_pending_count = 0;

    usleep(1000000);
    std::cout << "TIME [hh:mm:ss]\t\t"
        << "SENT [speed/total]\t\t\t"
        << "FINISH [speed/total]\t\t\t"
        << "SUCCESS [speed/total]\t\t\t"
        << "PENDING [count]" << std::endl;

    timeval start_time;
    gettimeofday(&start_time, NULL);

    for (;;) {
        struct timeval now;
        gettimeofday(&now, NULL);
        usleep(1000000 - now.tv_usec);

        int64_t new_total_count, new_finish_count, new_success_count;
        int64_t new_total_size, new_finish_size, new_success_size;
        adapter->CheckStatus(&new_total_count, &new_total_size,
                             &new_finish_count, &new_finish_size,
                             &new_success_count, &new_success_size);

        int64_t total_count = new_total_count - old_total_count;
        int64_t finish_count = new_finish_count - old_finish_count;
        int64_t success_count = new_success_count - old_success_count;
        int64_t total_size = new_total_size - old_total_size;
        int64_t finish_size = new_finish_size - old_finish_size;
        int64_t success_size = new_success_size - old_success_size;

        old_total_count = new_total_count;
        old_finish_count = new_finish_count;
        old_success_count= new_success_count;
        old_total_size = new_total_size;
        old_finish_size = new_finish_size;
        old_success_size = new_success_size;

        total_pending_count = new_total_count - new_finish_count;
        // scan
        if (total_pending_count < 0) {
            total_pending_count = 0;
        }

        struct tm now_tm;
        localtime_r(&now.tv_sec, &now_tm);
        std::cout << std::setfill('0') << std::setw(2) << now_tm.tm_hour << ":"
            << std::setfill('0') << std::setw(2) << now_tm.tm_min << ":"
            << std::setfill('0') << std::setw(2) << now_tm.tm_sec << "\t\t";
        std::cout << std::fixed << std::setprecision(3)
            << (double)total_size / 1048576 << "(" << total_count << ")/"
            << std::fixed << std::setprecision(3)
            << (double)new_total_size / 1048576 << "(" << new_total_count << ")\t\t"
            << std::fixed << std::setprecision(3)
            << (double)finish_size / 1048576 << "(" << finish_count << ")/"
            << std::fixed << std::setprecision(3)
            << (double)new_finish_size / 1048576 << "(" << new_finish_count << ")\t\t"
            << std::fixed << std::setprecision(3)
            << (double)success_size / 1048576 << "(" << success_count << ")/"
            << std::fixed << std::setprecision(3)
            << (double)new_success_size / 1048576 << "(" << new_success_count << ")\t\t"
            << total_pending_count
            << std::endl;
    }
}

int write_proc(tera::Table* table) {
    IWriter* writer = NULL;
    if (FLAGS_type.compare("sync") == 0) {
        writer = new SyncWriter(table);
    } else if (FLAGS_type.compare("async") == 0) {
        writer = new AsyncWriter(table);
    } else {
        std::cerr << "unsupport type: " << FLAGS_type << std::endl;
        return -1;
    }
    writer->SetMaxOutFlow(FLAGS_max_outflow);
    writer->SetMaxRate(FLAGS_max_rate);

    pthread_t print_thread;
    if (0 != pthread_create(&print_thread, NULL, &print_status_proc, writer)) {
        std::cerr << "cannot create thread";
        return -1;
    }

    std::cout << "begin to write" << std::endl;
    timeval start_time;
    gettimeofday(&start_time, NULL);
    if (FLAGS_type.compare("sync") == 0) {
        sync_write_proc(writer);
    } else {
        async_write_proc(writer);
    }

    std::cout << "wait for completion..." << std::endl;
    int64_t finish_count, finish_size, success_count, success_size;
    writer->WaitComplete(&finish_count, &finish_size, &success_count, &success_size);
    timeval finish_time;
    gettimeofday(&finish_time, NULL);

    pthread_cancel(print_thread);
    delete writer;

    double duration = (finish_time.tv_sec - start_time.tv_sec)
        + (double)(finish_time.tv_usec - start_time.tv_usec) / 1000000.0;
    std::cout.precision(3);
    std::cout << "Summary: " << std::fixed << duration << " s\n"
        << "    total: " << finish_size << " bytes "
                         << finish_count << " records "
                         << (double)finish_size / 1048576 / duration << " MB/s\n"
        << "     succ: " << success_size << " bytes "
                         << success_count << " records "
                         << (double)success_size / 1048576 / duration << " MB/s"
        << std::endl;
    usleep(100000);
    return 0;
}

void sync_read_proc(IReader* reader) {
    std::string row;
    std::string family;
    std::string qualifier;
    uint64_t timestamp = 0;
    while (get_next_row(&row, &family, &qualifier, &timestamp, NULL)) {
        reader->Read(row, family, qualifier, timestamp);
        row.clear();
        family.clear();
        qualifier.clear();
        timestamp = 0;
    }
}

void async_read_proc(IReader* reader) {
    ((AsyncReader*)reader)->SetMaxPendingSize(FLAGS_pend_size);
    ((AsyncReader*)reader)->SetMaxPendingCount(FLAGS_pend_count);
    std::string row;
    std::string family;
    std::string qualifier;
    uint64_t timestamp = 0;
    while (get_next_row(&row, &family, &qualifier, &timestamp, NULL)) {
        reader->Read(row, family, qualifier, timestamp);
        row.clear();
        family.clear();
        qualifier.clear();
        timestamp = 0;
    }
}

int read_proc(tera::Table* table) {
    IReader* reader = NULL;
    if (FLAGS_type.compare("sync") == 0) {
        reader = new SyncReader(table);
    } else if (FLAGS_type.compare("async") == 0) {
        reader = new AsyncReader(table);
    } else {
        std::cerr << "unsupport type: " << FLAGS_type << std::endl;
        return -1;
    }
    reader->SetMaxOutFlow(FLAGS_max_outflow);
    reader->SetMaxRate(FLAGS_max_rate);

    pthread_t print_thread;
    if (0 != pthread_create(&print_thread, NULL, &print_status_proc, reader)) {
        std::cerr << "cannot create thread";
        return -1;
    }

    std::cout << "begin to read" << std::endl;
    timeval start_time;
    gettimeofday(&start_time, NULL);
    if (FLAGS_type.compare("sync") == 0) {
        sync_read_proc(reader);
    } else {
        async_read_proc(reader);
    }

    std::cout << "wait for completion..." << std::endl;
    int64_t finish_count, finish_size, success_count, success_size;
    reader->WaitComplete(&finish_count, &finish_size, &success_count, &success_size);
    timeval finish_time;
    gettimeofday(&finish_time, NULL);

    pthread_cancel(print_thread);
    delete reader;

    double duration = (finish_time.tv_sec - start_time.tv_sec)
        + (double)(finish_time.tv_usec - start_time.tv_usec) / 1000000.0;
    std::cout.precision(3);
    std::cout << "Summary: " << std::fixed << duration << " s\n"
        << "    total: " << finish_size << " bytes "
                         << finish_count << " records "
                         << (double)finish_size / 1048576 / duration << " MB/s\n"
        << "     succ: " << success_size << " bytes "
                         << success_count << " records "
                         << (double)success_size / 1048576 / duration << " MB/s"
        << std::endl;
    usleep(100000);
    return 0;
}

int scan_proc(tera::Table* table) {
    Scanner* scanner = new Scanner(table);

    std::vector<std::string> cf_list;
    size_t delim = 0;
    size_t cf_pos = 0;
    while (std::string::npos != (delim = FLAGS_cf_list.find(',', cf_pos))) {
        if (cf_pos < delim) {
            cf_list.push_back(std::string(FLAGS_cf_list, cf_pos, delim - cf_pos));
        }
        cf_pos = delim + 1;
    }

    pthread_t print_thread;
    if (0 != pthread_create(&print_thread, NULL, &print_status_proc, scanner)) {
        std::cerr << "cannot create thread";
        return -1;
    }

    std::cout << "begin to scan (" << (FLAGS_scan_streaming? "async":"sync")
        << ")" << std::endl;
    timeval start_time;
    gettimeofday(&start_time, NULL);
    scanner->Scan(FLAGS_start_key, FLAGS_end_key, cf_list, FLAGS_print,
                  FLAGS_scan_streaming);

    std::cout << "wait for completion..." << std::endl;
    int64_t finish_count, finish_size, success_count, success_size;
    scanner->WaitComplete(&finish_count, &finish_size, &success_count, &success_size);
    timeval finish_time;
    gettimeofday(&finish_time, NULL);

    pthread_cancel(print_thread);
    delete scanner;

    double duration = (finish_time.tv_sec - start_time.tv_sec)
        + (double)(finish_time.tv_usec - start_time.tv_usec) / 1000000.0;
    std::cout.precision(3);
    std::cout << "Summary: " << std::fixed << duration << " s\n"
        << "    total: " << finish_size << " bytes "
                         << finish_count << " records "
                         << (double)finish_size / 1048576 / duration << " MB/s\n"
        << "     succ: " << success_size << " bytes "
                         << success_count << " records "
                         << (double)success_size / 1048576 / duration << " MB/s"
        << std::endl;
    usleep(100000);
    return 0;
}

int main(int argc, char** argv) {
    FLAGS_flagfile = "./tera.flag";
    ::google::ParseCommandLineFlags(&argc, &argv, true);

    tera::ErrorCode err;
    tera::Client* client = tera::Client::NewClient("./tera.flag", "sample");
    if (NULL == client) {
        std::cerr << "fail to create client: " << tera::strerr(err) << std::endl;
        return -1;
    }

    tera::Table* table = client->OpenTable(FLAGS_tablename, &err);
    if (NULL == table) {
        std::cerr << "fail to open table: " << tera::strerr(err) << std::endl;
        return -1;
    }

    if (FLAGS_mode.compare("w") == 0) {
        return write_proc(table);
    } else if (FLAGS_mode.compare("r") == 0) {
        return read_proc(table);
    } else if (FLAGS_mode.compare("s") == 0) {
        return scan_proc(table);
    } else {
        std::cerr << "unsupport mode: " << FLAGS_mode << std::endl;
        return -1;
    }
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
