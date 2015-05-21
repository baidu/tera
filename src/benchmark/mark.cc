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

    int64_t req_time = Now();
    if (type == ASYNC) {
        Context* ctx = new Context(this, req_size, req_time);
        row_mu->SetCallBack(sdk_write_callback);
        row_mu->SetContext(ctx);
    }

    m_pending_num.Inc();
    m_table->ApplyMutation(row_mu);
    if (type == SYNC) {
        WriteCallback(row_mu, req_size, req_time);
    }
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

    int64_t req_time = Now();
    if (type == ASYNC) {
        Context* ctx = new Context(this, req_size, req_time);
        reader->SetCallBack(sdk_read_callback);
        reader->SetContext(ctx);
    }

    m_pending_num.Inc();
    m_table->Get(reader);
    if (type == SYNC) {
        ReadCallback(reader, req_size, req_time);
    }
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
            << "], status=" << tera::strerr(err) << std::endl;
    }
    delete reader;

    if (0 == m_pending_num.Dec()) {
        pthread_mutex_lock(&m_mutex);
        pthread_cond_signal(&m_cond);
        pthread_mutex_unlock(&m_mutex);
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

/*
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

void remove_md5sum(std::string* value) {
    value->resize(value->size() - MD5_DIGEST_LENGTH);
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
*/

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
