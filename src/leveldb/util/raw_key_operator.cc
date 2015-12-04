// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "leveldb/raw_key_operator.h"

#include <pthread.h>

#include "coding.h"
#include "../utils/counter.h"

namespace leveldb {

// performance test
tera::Counter rawkey_compare_counter;

static inline void AppendTsAndType(std::string* tera_key,
                                   int64_t timestamp,
                                   TeraKeyType type) {
    timestamp &= 0x00FFFFFFFFFFFFFF;
    uint64_t n = ((1UL << 56) - 1 - timestamp) << 8 | (type & 0xFF);
    char str[8];
    EncodeBigEndian(str, n);
    tera_key->append(str, 8);
}

static inline void ExtractTsAndType(const Slice& tera_key,
                                    int64_t* timestamp,
                                    TeraKeyType* type) {
    uint64_t n = DecodeBigEndain(tera_key.data() + tera_key.size() - sizeof(uint64_t));
    if (type) {
        *type = static_cast<TeraKeyType>((n << 56) >> 56);
    }
    if (timestamp) {
        *timestamp = (1L << 56) - 1 - (n >> 8);
    }
}

static inline void AppendRowQualifierLength(std::string* tera_key,
                                            const std::string& row_key,
                                            const std::string& qualifier) {
    uint32_t rlen = row_key.size();
    uint32_t qlen = qualifier.size();
    uint32_t n = (rlen<<16) | qlen;
    char str[4];
    EncodeBigEndian32(str, n);
    tera_key->append(str, 4);
}


/**
 *  readable encoding format:
 *  [rowkey\0|column\0|qualifier\0|type|timestamp]
 *  [ rlen+1B| clen+1B| qlen+1B   | 1B | 7B      ]
 **/
class ReadableRawKeyOperatorImpl : public RawKeyOperator {
public:
    virtual void EncodeTeraKey(const std::string& row_key,
                               const std::string& family,
                               const std::string& qualifier,
                               int64_t timestamp,
                               TeraKeyType type,
                               std::string* tera_key) const {
        *tera_key = row_key;
        tera_key->push_back('\0');
        tera_key->append(family);
        tera_key->push_back('\0');
        tera_key->append(qualifier);
        tera_key->push_back('\0');
        AppendTsAndType(tera_key, timestamp, type);
    }

    virtual bool ExtractTeraKey(const Slice& tera_key,
                                Slice* row_key,
                                Slice* family,
                                Slice* qualifier,
                                int64_t* timestamp,
                                TeraKeyType* type) const {
        int key_len = strlen(tera_key.data());
        if (row_key) {
            *row_key = Slice(tera_key.data(), key_len);
        }

        int family_len = strlen(tera_key.data() + key_len + 1);
        Slice family_data(tera_key.data() + key_len + 1, family_len);
        if (family) {
            *family = family_data;
        }

        int qualifier_len = strlen(family_data.data() + family_len + 1);
        if (qualifier) {
            *qualifier = Slice(family_data.data() + family_len + 1, qualifier_len);
        }

        if (key_len + family_len + qualifier_len + 3 + sizeof(uint64_t) != tera_key.size()) {
            return false;
        }
        ExtractTsAndType(tera_key, timestamp, type);
        return true;
    }

    virtual int Compare(const Slice& key1, const Slice& key2) const {
        return key1.compare(key2);
    }
};

/**
 *  binary encoding format:
 *  [rowkey|column\0|qualifier|type|timestamp|rlen|qlen]
 *  [ rlen | clen+1B| qlen    | 1B |   7B    | 2B | 2B ]
 **/
class BinaryRawKeyOperatorImpl : public RawKeyOperator {
public:
    virtual void EncodeTeraKey(const std::string& row_key,
                               const std::string& family,
                               const std::string& qualifier,
                               int64_t timestamp,
                               TeraKeyType type,
                               std::string* tera_key) const {
        uint32_t rlen = row_key.size();
        uint32_t flen = family.size();
        uint32_t qlen = qualifier.size();

        tera_key->resize(rlen + flen + qlen + 13);
        char* key = (char*)(tera_key->data());

        // fill rowkey segment
        memcpy(key, row_key.data(), rlen);
        int pos = rlen;
        // fill column family segment
        memcpy(key + pos, family.data(), flen);
        pos += flen;
        key[pos] = '\0';
        pos++;

        // fill qualifier segment
        memcpy(key + pos, qualifier.data(), qlen);
        pos += qlen;

        // fill timestamp&type segment
        uint64_t n = ((1UL << 56) - 1 - timestamp) << 8 | (type & 0xFF);
        EncodeBigEndian(key + pos, n);
        pos += 8;

        // fill row len and qualifier len segment
        uint32_t m = (rlen << 16) | (qlen & 0xFFFF);
        EncodeBigEndian32(key + pos, m);
    }

    virtual bool ExtractTeraKey(const Slice& tera_key,
                                Slice* row_key,
                                Slice* family,
                                Slice* qualifier,
                                int64_t* timestamp,
                                TeraKeyType* type) const {
        uint32_t len = DecodeBigEndain32(tera_key.data() + tera_key.size() - sizeof(uint32_t));
        int key_len = static_cast<int>(len >> 16);
        int family_len = strlen(tera_key.data() + key_len);
        int qualifier_len = static_cast<int>(len & 0xFFFF);

        if (key_len + family_len + qualifier_len + 1 +
            sizeof(uint64_t) + sizeof(uint32_t) != tera_key.size()) {
            return false;
        }

        if (row_key) {
            *row_key = Slice(tera_key.data(), key_len);
        }
        Slice family_data(tera_key.data() + key_len, family_len);
        if (family) {
            *family = family_data;
        }
        if (qualifier) {
            *qualifier = Slice(family_data.data() + family_len + 1, qualifier_len);
        }

        Slice internal_tera_key = Slice(tera_key.data(), tera_key.size() - sizeof(uint32_t));
        ExtractTsAndType(internal_tera_key, timestamp, type);
        return true;
    }

    virtual int Compare(const Slice& key1, const Slice& key2) const {
        // for performance optimiztion
        // rawkey_compare_counter.Inc();
        uint32_t len1, len2, rlen1, rlen2, clen1, clen2, qlen1, qlen2;
        int ret;
        const char* data1 = key1.data();
        const char* data2 = key2.data();
        int size1 = key1.size();
        int size2 = key2.size();

        // decode rowlen and qualifierlen from raw key
        len1 = DecodeBigEndain32(data1 + size1 - 4);
        len2 = DecodeBigEndain32(data2 + size2 - 4);

        // rowkey compare, if ne, return
        rlen1 = static_cast<int>(len1 >> 16);
        rlen2 = static_cast<int>(len2 >> 16);
        Slice row1(data1, rlen1);
        Slice row2(data2, rlen2);
        ret = row1.compare(row2);
        if (ret != 0) {
            return ret;
        }

        // column family compare, if ne, return
        qlen1 = static_cast<int>(len1 & 0x00FF);
        qlen2 = static_cast<int>(len2 & 0x00FF);
        clen1 = size1 - rlen1 - qlen1 - 13;
        clen2 = size2 - rlen2 - qlen2 - 13;
        Slice col1(data1 + rlen1, clen1);
        Slice col2(data2 + rlen2, clen2);
        ret = col1.compare(col2);
        if (ret != 0) {
            return ret;
        }

        // qualifier compare, if ne, return
        Slice qual1(data1 + size1 - qlen1 - 12, qlen1);
        Slice qual2(data2 + size2 - qlen2 - 12, qlen2);
        ret = qual1.compare(qual2);
        if (ret != 0) {
            return ret;
        }

        // timestamp&type compared together
        Slice ts_type1(data1 + size1 - 12, 8);
        Slice ts_type2(data2 + size2 - 12, 8);
        return ts_type1.compare(ts_type2);
    }
};

// support KV-pair with TTL, Key's format :
// [row_key|expire_timestamp]
// [rlen|4B]
class KvRawKeyOperatorImpl : public RawKeyOperator {
public:
    virtual void EncodeTeraKey(const std::string& row_key,
                               const std::string& family,
                               const std::string& qualifier,
                               int64_t timestamp, // must >= 0
                               TeraKeyType type,
                               std::string* tera_key) const {
        char expire_timestamp[8];
        EncodeBigEndian(expire_timestamp, timestamp);
        tera_key->assign(row_key).append(expire_timestamp, 8);
    }

    virtual bool ExtractTeraKey(const Slice& tera_key,
                                Slice* row_key,
                                Slice* family,
                                Slice* qualifier,
                                int64_t* timestamp,
                                TeraKeyType* type) const {
        if (row_key) {
            *row_key = Slice(tera_key.data(), tera_key.size() - sizeof(int64_t));
        }
        if (timestamp) {
            *timestamp = DecodeBigEndain(tera_key.data() + tera_key.size() - sizeof(int64_t));
        }
        return true;
    }

    // only compare row_key
    virtual int Compare(const Slice& key1, const Slice& key2) const {
        size_t min_len = (key1.size() < key2.size()) ? key1.size() : key2.size();
        int r = memcmp(key1.data(), key2.data(), min_len);
        if (r == 0) {
            if (key1.size() < key2.size()) {
                r = -1;
            }
            else if (key1.size() > key2.size()) {
                r = +1;
            }
        }
        return r;
    }
};

static pthread_once_t once = PTHREAD_ONCE_INIT;
static const RawKeyOperator* readable_key;
static const RawKeyOperator* binary_key;
static const KvRawKeyOperatorImpl* kv_key;

static void InitModule() {
    readable_key = new ReadableRawKeyOperatorImpl;
    binary_key = new BinaryRawKeyOperatorImpl;
    kv_key = new KvRawKeyOperatorImpl;
}

const RawKeyOperator* ReadableRawKeyOperator() {
    pthread_once(&once, InitModule);
    return readable_key;
}

const RawKeyOperator* BinaryRawKeyOperator() {
    pthread_once(&once, InitModule);
    return binary_key;
}

const RawKeyOperator* KvRawKeyOperator() {
    pthread_once(&once, InitModule);
    return kv_key;
}

} // namespace leveldb
