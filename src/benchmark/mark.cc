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
    : m_table(table),
      m_write_marker(PUT),
      m_read_marker(GET),
      m_scan_marker(SCN) {
    pthread_mutex_init(&m_mutex, NULL);
    pthread_cond_init(&m_cond, NULL);
}

Adapter::~Adapter() {
    pthread_mutex_destroy(&m_mutex);
    pthread_cond_destroy(&m_cond);
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
    tera::RowMutation* row_mu = m_table->NewRowMutation(row);
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

    m_write_marker.CheckPending();
    m_write_marker.CheckLimit();
    m_write_marker.OnReceive(req_size);
    m_pending_num.Inc();

    if (type == ASYNC) {
        int64_t req_time = Now();
        Context* ctx = new Context(this, req_size, req_time);
        row_mu->SetCallBack(sdk_write_callback);
        row_mu->SetContext(ctx);
        m_table->ApplyMutation(row_mu);
    } else {
        m_sync_mutations.push_back(row_mu);
        m_sync_req_sizes.push_back(req_size);
        if (m_sync_mutations.size() >= static_cast<unsigned long long>(FLAGS_batch_count)) {
            CommitSyncWrite();
        }
    }
}

void Adapter::CommitSyncWrite() {
    if (m_sync_mutations.size() == 0) {
        return;
    }
    CHECK_EQ(m_sync_mutations.size(), m_sync_req_sizes.size());
    int64_t req_time = Now();
    m_table->ApplyMutation(m_sync_mutations);
    for (size_t i = 0; i < m_sync_mutations.size(); i++) {
        WriteCallback(m_sync_mutations[i], m_sync_req_sizes[i], req_time);
    }
    m_sync_mutations.clear();
    m_sync_req_sizes.clear();
}

void Adapter::WriteCallback(tera::RowMutation* row_mu, size_t req_size,
                            int64_t req_time) {
    uint32_t latency = (Now() - req_time) / 1000;
    m_write_marker.OnFinish(req_size, latency);
    tera::ErrorCode err = row_mu->GetError();
    if (err.GetType() == tera::ErrorCode::kOK) {
        m_write_marker.OnSuccess(req_size, latency);
    } else {
        /*std::cerr << "fail to write: row=[" << row << "], column=["
            << family << ":" << qualifier << "], timestamp=["
            << timestamp << "], value=[" << value << "], status="
            << tera::strerr(err) << std::endl;*/
    }
    delete row_mu;

    if (0 == m_pending_num.Dec()) {
        pthread_mutex_lock(&m_mutex);
        pthread_cond_signal(&m_cond);
        pthread_mutex_unlock(&m_mutex);
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
    tera::RowReader* reader = m_table->NewRowReader(row);
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

    m_read_marker.CheckPending();
    m_read_marker.CheckLimit();
    m_read_marker.OnReceive(req_size);
    m_pending_num.Inc();

    if (type == ASYNC) {
        int64_t req_time = Now();
        Context* ctx = new Context(this, req_size, req_time);
        reader->SetCallBack(sdk_read_callback);
        reader->SetContext(ctx);
        m_table->Get(reader);
    } else {
        m_sync_readers.push_back(reader);
        m_sync_req_sizes.push_back(req_size);
        if (m_sync_readers.size() >= static_cast<unsigned long long>(FLAGS_batch_count)) {
            CommitSyncRead();
        }
    }
}

void Adapter::CommitSyncRead() {
    if (m_sync_readers.size() == 0) {
        return;
    }
    CHECK_EQ(m_sync_readers.size(), m_sync_req_sizes.size());
    int64_t req_time = Now();
    m_table->Get(m_sync_readers);
    for (size_t i = 0; i < m_sync_readers.size(); i++) {
        ReadCallback(m_sync_readers[i], m_sync_req_sizes[i], req_time);
    }
    m_sync_readers.clear();
    m_sync_req_sizes.clear();
}

void Adapter::ReadCallback(tera::RowReader* reader, size_t req_size,
                           int64_t req_time) {
    uint32_t latency = (Now() - req_time) / 1000;
    m_read_marker.OnFinish(req_size, latency);
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
            m_read_marker.OnSuccess(req_size, latency);
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

    if (0 == m_pending_num.Dec()) {
        pthread_mutex_lock(&m_mutex);
        pthread_cond_signal(&m_cond);
        pthread_mutex_unlock(&m_mutex);
    }
}

void Adapter::Delete(const std::string& row,
                     std::map<std::string, std::set<std::string> >& column) {
    tera::RowMutation* row_mu = m_table->NewRowMutation(row);
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
                row_mu->DeleteColumns(family, qualifier);
            }
        }
    }

    m_write_marker.CheckPending();
    m_write_marker.CheckLimit();
    m_write_marker.OnReceive(req_size);
    m_pending_num.Inc();

    if (type == ASYNC) {
        int64_t req_time = Now();
        Context* ctx = new Context(this, req_size, req_time);
        row_mu->SetCallBack(sdk_write_callback);
        row_mu->SetContext(ctx);
        m_table->ApplyMutation(row_mu);
    } else {
        m_sync_mutations.push_back(row_mu);
        m_sync_req_sizes.push_back(req_size);
        if (m_sync_mutations.size() >= static_cast<unsigned long long>(FLAGS_batch_count)) {
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
    tera::ResultStream* result = m_table->Scan(scan_desp, &err);
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
        m_scan_marker.OnFinish(size, 0);
        m_scan_marker.OnSuccess(size, 0);
        result->Next();
    }
    delete result;
}

void Adapter::WaitComplete() {
    pthread_mutex_lock(&m_mutex);
    while (0 != m_pending_num.Get()) {
        pthread_cond_wait(&m_cond, &m_mutex);
    }
    pthread_mutex_unlock(&m_mutex);
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
