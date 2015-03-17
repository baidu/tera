// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef LEVELDB_DB_LG_WRITE_THREAD_H_
#define LEVELDB_DB_LG_WRITE_THREAD_H_

#include "db/db_impl.h"
#include "leveldb/write_batch.h"
#include "util/thread.h"

namespace leveldb {

class LGWriteThread : public Thread {
public:
    LGWriteThread(uint32_t lg_id, DBImpl* impl,
                  const WriteOptions& opts, WriteBatch* bench)
        : wopts_(opts), wbench_(bench);
    virtual ~LGWriteThread() {}

    virtual void Run(void* params) {
        std::cout << "LG Thread #" << lg_id_ << ": Write()" << std::endl;
        lg_impl_->Write(wopts_, wbatch_);
    }

    Status GetResult() {
        return ret_;
    }

private:
    uint32_t lg_id_;
    DBImpl* lg_impl_;
    const WriteOptions& wopts_;
    WriteBatch* wbatch_;
    Status ret_;
};

} // namespace leveldb

#endif // LEVELDB_DB_LG_WRITE_THREAD_H_
