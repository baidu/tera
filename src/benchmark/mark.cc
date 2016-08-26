// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "benchmark/mark.h"

#include <iostream>

#include "leveldb/util/crc32c.h"

DECLARE_bool(verify);
DECLARE_int32(buf_size);

int64_t Now() {
    struct timeval now;
    gettimeofday(&now, NULL);
    return now.tv_sec * 1000000 + now.tv_usec;
}

class Context {
public:
    Adapter* adapter;
    size_t size;
    int64_t time;

    Context(Adapter* a, size_t s, int64_t t)
        : adapter(a), size(s), time(t) {}
};

Adapter::Adapter(tera::Table* table)
    : table_(table),
      write_marker_(PUT),
      read_marker_(GET),
      scan_marker_(SCN) {
    pthread_mutex_init(&mutex_, NULL);
    pthread_cond_init(&cond_, NULL);
}

Adapter::~Adapter() {
    pthread_mutex_destroy(&mutex_);
    pthread_cond_destroy(&cond_);
}

void sdk_write_callback(tera::RowMutation* row_mu) {
    Context* ctx = (Context*)row_mu->GetContext();
    Adapter* adapter = ctx->adapter;
    size_t req_size = ctx->size;
    int64_t req_time = ctx->time;
    adapter->WriteCallback(row_mu, req_size, req_time);
}

void Adapter::Write(const std::string& row,
                    std::map<std::string, std::set<std::string> >& column,
                    uint64_t timestamp,
                    std::string& value) {
    tera::RowMutation* row_mu = table_->NewRowMutation(row);
    size_t req_size = row.size();

    if (column.size() == 0) {
        column[""].insert("");
    }
    std::map<std::string, std::set<std::string> >::iterator it;
    for (it = column.begin(); it != column.end(); ++it) {
        const std::string& family = it->first;
        std::set<std::string>& qualifiers = it->second;
        if (qualifiers.size() == 0) {
            qualifiers.insert("");
        }
        std::set<std::string>::const_iterator it2;
        for (it2 = qualifiers.begin(); it2 != qualifiers.end(); ++it2) {
            const std::string& qualifier = *it2;
            req_size += family.size() + qualifier.size() + sizeof(timestamp);
            req_size += value.size();
            if (FLAGS_verify) {
                add_checksum(row, family, qualifier, &value);
            }
            row_mu->Put(family, qualifier, timestamp, value);
            if (FLAGS_verify) {
                remove_checksum(&value);
            }
        }
    }

    write_marker_.CheckPending();
    write_marker_.CheckLimit();
    write_marker_.OnReceive(req_size);
    pending_num_.Inc();

    if (type == ASYNC) {
        int64_t req_time = Now();
        Context* ctx = new Context(this, req_size, req_time);
        row_mu->SetCallBack(sdk_write_callback);
        row_mu->SetContext(ctx);
        table_->ApplyMutation(row_mu);
    } else {
        sync_mutations_.push_back(row_mu);
        sync_req_sizes_.push_back(req_size);
        if (sync_mutations_.size() >= static_cast<unsigned long long>(FLAGS_batch_count)) {
            CommitSyncWrite();
        }
    }
}

void Adapter::CommitSyncWrite() {
    if (sync_mutations_.size() == 0) {
        return;
    }
    CHECK_EQ(sync_mutations_.size(), sync_req_sizes_.size());
    int64_t req_time = Now();
    table_->ApplyMutation(sync_mutations_);
    for (size_t i = 0; i < sync_mutations_.size(); i++) {
        WriteCallback(sync_mutations_[i], sync_req_sizes_[i], req_time);
    }
    sync_mutations_.clear();
    sync_req_sizes_.clear();
}

void Adapter::WriteCallback(tera::RowMutation* row_mu, size_t req_size,
                            int64_t req_time) {
    uint32_t latency = (Now() - req_time) / 1000;
    write_marker_.OnFinish(req_size, latency);
    tera::ErrorCode err = row_mu->GetError();
    if (err.GetType() == tera::ErrorCode::kOK) {
        write_marker_.OnSuccess(req_size, latency);
    } else {
        /*std::cerr << "fail to write: row=[" << row << "], column=["
            << family << ":" << qualifier << "], timestamp=["
            << timestamp << "], value=[" << value << "], status="
            << tera::strerr(err) << std::endl;*/
    }
    delete row_mu;

    if (0 == pending_num_.Dec()) {
        pthread_mutex_lock(&mutex_);
        pthread_cond_signal(&cond_);
        pthread_mutex_unlock(&mutex_);
    }
}

void sdk_read_callback(tera::RowReader* row_rd) {
    Context* ctx = (Context*)row_rd->GetContext();
    Adapter* adapter = ctx->adapter;
    size_t req_size = ctx->size;
    int64_t req_time = ctx->time;
    adapter->ReadCallback(row_rd, req_size, req_time);
    delete ctx;
}

void Adapter::Read(const std::string& row,
                   const std::map<std::string, std::set<std::string> >& column,
                   uint64_t largest_ts, uint64_t smallest_ts) {
    tera::RowReader* reader = table_->NewRowReader(row);
    size_t req_size = row.size();

    std::map<std::string, std::set<std::string> >::const_iterator it;
    for (it = column.begin(); it != column.end(); ++it) {
        const std::string& family = it->first;
        const std::set<std::string>& qualifiers = it->second;
        if (qualifiers.size() == 0) {
            reader->AddColumnFamily(family);
            req_size += family.size();
        } else {
            std::set<std::string>::const_iterator it2;
            for (it2 = qualifiers.begin(); it2 != qualifiers.end(); ++it2) {
                const std::string& qualifier = *it2;
                reader->AddColumn(family, qualifier);
                req_size += family.size() + qualifier.size();
            }
        }
    }
    reader->SetTimeRange(smallest_ts, largest_ts);
    req_size += sizeof(smallest_ts) + sizeof(largest_ts);

    read_marker_.CheckPending();
    read_marker_.CheckLimit();
    read_marker_.OnReceive(req_size);
    pending_num_.Inc();

    if (type == ASYNC) {
        int64_t req_time = Now();
        Context* ctx = new Context(this, req_size, req_time);
        reader->SetCallBack(sdk_read_callback);
        reader->SetContext(ctx);
        table_->Get(reader);
    } else {
        sync_readers_.push_back(reader);
        sync_req_sizes_.push_back(req_size);
        if (sync_readers_.size() >= static_cast<unsigned long long>(FLAGS_batch_count)) {
            CommitSyncRead();
        }
    }
}

void Adapter::CommitSyncRead() {
    if (sync_readers_.size() == 0) {
        return;
    }
    CHECK_EQ(sync_readers_.size(), sync_req_sizes_.size());
    int64_t req_time = Now();
    table_->Get(sync_readers_);
    for (size_t i = 0; i < sync_readers_.size(); i++) {
        ReadCallback(sync_readers_[i], sync_req_sizes_[i], req_time);
    }
    sync_readers_.clear();
    sync_req_sizes_.clear();
}

void Adapter::ReadCallback(tera::RowReader* reader, size_t req_size,
                           int64_t req_time) {
    uint32_t latency = (Now() - req_time) / 1000;
    read_marker_.OnFinish(req_size, latency);
    const std::string& row = reader->RowName();
    tera::ErrorCode err = reader->GetError();
    if (err.GetType() == tera::ErrorCode::kOK) {
        bool all_verified = true;
        while (!reader->Done()) {
            std::string cf = reader->Family();
            std::string cq = reader->Qualifier();
            int64_t ts = reader->Timestamp();
            std::string value = reader->Value();

            bool is_verified = (!FLAGS_verify) || verify_checksum(row, cf, cq, value);
            if (!is_verified) {
                all_verified = false;
                std::cerr << "fail to pass md5 verifying: row=[" << row << "], column=["
                    << cf << ":" << cq << "], timestamp=[" << ts << "]" << std::endl;
            }
            reader->Next();
        }
        if (all_verified) {
            read_marker_.OnSuccess(req_size, latency);
        }
    } else {
        std::cerr << "fail to read: row=[" << row << "], column=[";
        const tera::RowReader::ReadColumnList& read_list = reader->GetReadColumnList();
        std::map<std::string, std::set<std::string> >::const_iterator it;
        bool first_cf = true;
        for (it = read_list.begin(); it != read_list.end(); ++it) {
            const std::string& family = it->first;
            const std::set<std::string>& qualifiers = it->second;
            if (first_cf) {
                first_cf = false;
            } else {
                std::cerr << ";";
            }
            std::cerr << family;
            std::set<std::string>::const_iterator it2;
            bool first_cq = true;
            for (it2 = qualifiers.begin(); it2 != qualifiers.end(); ++it2) {
                const std::string& qualifier = *it2;
                if (first_cq) {
                    first_cq = false;
                    std::cerr << ":";
                } else {
                    std::cerr << ",";
                }
                std::cerr << qualifier;
            }
        }
        std::cerr << "], timestamp=[" << reader->GetTimestamp()
            << "], status=" << tera::strerr(err) << ":" << err.GetReason()  << std::endl;
    }
    delete reader;

    if (0 == pending_num_.Dec()) {
        pthread_mutex_lock(&mutex_);
        pthread_cond_signal(&cond_);
        pthread_mutex_unlock(&mutex_);
    }
}

void Adapter::Delete(const std::string& row,
                     std::map<std::string, std::set<std::string> >& column,
                     uint64_t ts) {
    tera::RowMutation* row_mu = table_->NewRowMutation(row);
    size_t req_size = row.size();

    if (column.size() == 0) {
        row_mu->DeleteRow();
    } else {
        std::map<std::string, std::set<std::string> >::iterator it;
        for (it = column.begin(); it != column.end(); ++it) {
            const std::string& family = it->first;
            std::set<std::string>& qualifiers = it->second;
            if (qualifiers.size() == 0) {
                qualifiers.insert("");
            }
            std::set<std::string>::const_iterator it2;
            for (it2 = qualifiers.begin(); it2 != qualifiers.end(); ++it2) {
                const std::string& qualifier = *it2;
                req_size += family.size() + qualifier.size();
                row_mu->DeleteColumn(family, qualifier, ts);
            }
        }
    }

    write_marker_.CheckPending();
    write_marker_.CheckLimit();
    write_marker_.OnReceive(req_size);
    pending_num_.Inc();

    if (type == ASYNC) {
        int64_t req_time = Now();
        Context* ctx = new Context(this, req_size, req_time);
        row_mu->SetCallBack(sdk_write_callback);
        row_mu->SetContext(ctx);
        table_->ApplyMutation(row_mu);
    } else {
        sync_mutations_.push_back(row_mu);
        sync_req_sizes_.push_back(req_size);
        if (sync_mutations_.size() >= static_cast<unsigned long long>(FLAGS_batch_count)) {
            CommitSyncWrite();
        }
    }
}

void Adapter::Scan(const std::string& start_key, const std::string& end_key,
                   const std::vector<std::string>& cf_list,
                   bool print, bool is_async) {
    tera::ScanDescriptor scan_desp(start_key);
    scan_desp.SetEnd(end_key);
    scan_desp.SetBufferSize(FLAGS_buf_size);
    scan_desp.SetAsync(is_async);
    for (size_t i = 0; i < cf_list.size(); i++) {
        scan_desp.AddColumnFamily(cf_list[i]);
    }
    tera::ErrorCode err;
    tera::ResultStream* result = table_->Scan(scan_desp, &err);
    if (result == NULL) {
        std::cerr << "fail to scan: " << tera::strerr(err);
        return;
    }

    uint64_t count = 0;
    while (!result->Done()) {
        if (print) {
            std::cerr << count++ << "\t" << result->RowName() << "\t" <<
                result->Family() << "\t" << result->Qualifier() << "\t" <<
                result->Timestamp() << "\t" << result->Value() << std::endl;
        }
        size_t size = result->RowName().size() + result->Family().size()
                    + result->Qualifier().size() + sizeof(result->Timestamp())
                    + result->Value().size();
        scan_marker_.OnFinish(size, 0);
        scan_marker_.OnSuccess(size, 0);
        result->Next();
    }
    delete result;
}

void Adapter::WaitComplete() {
    pthread_mutex_lock(&mutex_);
    while (0 != pending_num_.Get()) {
        pthread_cond_wait(&cond_, &mutex_);
    }
    pthread_mutex_unlock(&mutex_);
}

void add_checksum(const std::string& rowkey, const std::string& family,
                  const std::string& qualifier, std::string* value) {
    uint32_t crc = 0;
    crc = leveldb::crc32c::Extend(crc, rowkey.data(), rowkey.size());
    crc = leveldb::crc32c::Extend(crc, family.data(), family.size());
    crc = leveldb::crc32c::Extend(crc, qualifier.data(), qualifier.size());
    crc = leveldb::crc32c::Extend(crc, value->data(), value->size());
    value->append((char*)&crc, sizeof(uint32_t));
}

void remove_checksum(std::string* value) {
    value->resize(value->size() - sizeof(uint32_t));
}

bool verify_checksum(const std::string& rowkey, const std::string& family,
                     const std::string& qualifier, const std::string& value) {
    uint32_t crc = 0;
    crc = leveldb::crc32c::Extend(crc, rowkey.data(), rowkey.size());
    crc = leveldb::crc32c::Extend(crc, family.data(), family.size());
    crc = leveldb::crc32c::Extend(crc, qualifier.data(), qualifier.size());
    crc = leveldb::crc32c::Extend(crc, value.data(), value.size() - sizeof(uint32_t));
    return crc == *(uint32_t*)(value.data() + value.size() - sizeof(uint32_t));
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
