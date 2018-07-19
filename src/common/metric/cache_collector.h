// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_COMMOM_METRIC_CACHE_COLLECTOR_H_
#define TERA_COMMOM_METRIC_CACHE_COLLECTOR_H_
 
#include <cmath> 
#include <string>
 
#include "common/metric/collector_report_publisher.h" 
#include "common/metric/collector.h"
#include "db/table_cache.h"
#include "leveldb/cache.h" 
 
namespace tera { 
 
enum class CacheCollectType {
    kHitRate,
    kEntries,
    kCharge,
};

class BaseCacheCollector : public Collector {
public:
    explicit BaseCacheCollector(CacheCollectType cache_type) : cache_type_(cache_type) {}
    virtual ~BaseCacheCollector() {}
    
    virtual int64_t Collect() {
        switch (cache_type_) {
            case CacheCollectType::kHitRate:
                return HitRate();
            case CacheCollectType::kEntries:
                return Entries();
            case CacheCollectType::kCharge:
                return TotalCharge();
            default:
                return 0;
        }
    }
    
protected:
    virtual int64_t HitRate() = 0;
    virtual int64_t Entries() = 0;
    virtual int64_t TotalCharge() = 0;
    
protected:
    CacheCollectType cache_type_;
};

class LRUCacheCollector : public BaseCacheCollector {
public:
    LRUCacheCollector(leveldb::Cache* cache, 
                      CacheCollectType cache_type):
        BaseCacheCollector(cache_type), 
        cache_(cache) {}

    virtual ~LRUCacheCollector() {}

protected:
    int64_t HitRate() override {
        if (cache_ == NULL) {
            return 0;
        }
        
        double hit_rate = cache_->HitRate(true);
        return isnan(hit_rate) ? -1 : static_cast<int64_t>(hit_rate * 100.0d);
    }
    
    int64_t Entries() override { return cache_ == NULL ? 0 : static_cast<int64_t>(cache_->Entries()); }
    
    int64_t TotalCharge() override { return cache_ == NULL ? 0 : static_cast<int64_t>(cache_->TotalCharge()); }
private:
    leveldb::Cache* cache_;
};

class TableCacheCollector : public BaseCacheCollector {
public:
    TableCacheCollector(leveldb::TableCache* cache, 
                        CacheCollectType cache_type):
        BaseCacheCollector(cache_type), 
        cache_(cache) {}

    virtual ~TableCacheCollector() {}

protected:    
    int64_t HitRate() override {
        if (cache_ == NULL) {
            return 0;
        }
        
        double hit_rate = cache_->HitRate(true);
        return isnan(hit_rate) ? -1 : static_cast<int64_t>(hit_rate * 100.0d);
    }
    
    int64_t Entries() override { return cache_ == NULL ? 0 : static_cast<int64_t>(cache_->TableEntries()); }
    
    int64_t TotalCharge() override { return cache_ == NULL ? 0 : static_cast<int64_t>(cache_->ByteSize()); }
private:
    leveldb::TableCache* cache_;
}; 
 
} // end namespace tera 
 
#endif // TERA_COMMOM_METRIC_CACHE_COLLECTOR_H_
 
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */

