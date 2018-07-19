// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include <list>

#include "common/counter.h"
#include "leveldb/env.h"
#include "leveldb/statistics.h"
#include "leveldb/status.h"
#include "port/port.h"
#include "util/mutexlock.h"

namespace leveldb {

class FlashBlockCacheWriteBuffer {
public:
    FlashBlockCacheWriteBuffer(const std::string& path,
                          const std::string& file,
                          int block_size)
        : offset_(0),
        block_size_(block_size),
        block_idx_(0),
        storage_(NULL),
        path_(path),
        file_(file) {
    }

    ~FlashBlockCacheWriteBuffer() {
        assert(block_list_.empty());
    }

    uint32_t NumFullBlock() { // use for BGFlush
        MutexLock l(&mu_);
        if (block_list_.empty()) {
            return 0;
        } else if ((block_list_.back())->size() < block_size_) {
            return block_list_.size() - 1;
        } else {
            return block_list_.size();
        }
    }

    Status Append(const Slice& data) {
        MutexLock l(&mu_);
        if (storage_ == NULL) {
            storage_ = new std::string();
            block_list_.push_back(storage_);
        }
        uint32_t begin = offset_ / block_size_;
        uint32_t end = (offset_ + data.size()) / block_size_;
        if (begin == end) { // in the same block
            storage_->append(data.data(), data.size());
        } else {
            uint32_t tmp_size = block_size_ - (offset_ % block_size_);
            storage_->append(data.data(), tmp_size);
            assert(storage_->size() == block_size_);
            Slice buf(data.data() + tmp_size, data.size() - tmp_size);
            for (uint32_t i = begin + 1; i <= end; ++i) {
                storage_ = new std::string();
                block_list_.push_back(storage_);
                if (i < end) {
                    storage_->append(buf.data(), block_size_);
                    buf.remove_prefix(block_size_);
                } else { // last block
                    storage_->append(buf.data(), buf.size());
                    buf.remove_prefix(buf.size());
                }
                //Log("[%s] add tmp_storage %s: offset: %lu, buf_size: %lu, idx %u\n",
                    //path_.c_str(),
                    //file_.c_str(),
                    //offset_,
                    //buf.size(), i);
            }
        }
        offset_ += data.size();
        //Log("[%s] add record: %s, begin: %u, end: %u, offset: %lu, data_size: %lu, block_size: %u\n",
            //path_.c_str(),
            //file_.c_str(),
            //begin, end,
            //offset_ - data.size() , data.size(), block_size_);
        return Status::OK();
    }

    std::string* PopFrontBlock(uint64_t* block_idx) {
        MutexLock l(&mu_);
        if (block_list_.empty()) {
            return NULL;
        }
        std::string* block = block_list_.front();
        assert(block->size() <= block_size_);
        if (block->size() != block_size_) {
            return NULL;
        }
        block_list_.pop_front();
        *block_idx = block_idx_;
        block_idx_++;
        return block;
    }

    std::string* PopBackBlock(uint64_t* block_idx) {
        MutexLock l(&mu_);
        if (block_list_.empty()) {
            return NULL;
        }
        std::string* block = block_list_.back();
        block_list_.pop_back();
        *block_idx = offset_ / block_size_;
        return block;
    }

    void ReleaseBlock(std::string* block) {
        delete block;
    }

private:
    port::Mutex mu_;
    uint64_t offset_;
    uint32_t block_size_;
    uint64_t block_idx_;
    std::string* storage_;
    std::list<std::string*> block_list_; // kBlockSize
    std::string path_;
    std::string file_;
};

}  // namespace leveldb

