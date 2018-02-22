// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: baorenyi@baidu.com

#ifndef  TERA_SDK_TEST_MOCK_TABLE_H_
#define  TERA_SDK_TEST_MOCK_TABLE_H_

#include <iostream>
#include <string>
#include <vector>

#include "gflags/gflags.h"
#include "glog/logging.h"

#include "sdk/read_impl.h"
#include "sdk/mutate_impl.h"
#include "sdk/table_impl.h"

namespace tera {

struct MockReaderResult {
    RowResult result;
    ErrorCode status;
};

class MockTable: public TableImpl {
public:
    MockTable(const std::string& table_name, 
              common::ThreadPool* thread_pool, 
              sdk::ClusterFinder* cluster = NULL)
        : TableImpl(table_name, thread_pool, cluster) {
        reader_err_.clear();
        mu_err_.clear();
        reader_pos_ = 0;
        mu_pos_ = 0;
    }
    void ApplyMutation(RowMutation* row_mu) {
        RowMutationImpl* mu = static_cast<RowMutationImpl*>(row_mu);
        mu->SetError(mu_err_[mu_pos_++].GetType(),"");
        mu->RunCallback();
    }

    void Get(RowReader* reader) {
        RowReaderImpl* r = static_cast<RowReaderImpl*>(reader);
        if (reader_result_.size() > 0) {
            r->SetResult(reader_result_[reader_pos_].result);    
            r->SetError(reader_result_[reader_pos_++].status.GetType(), "");
        } else {
            r->SetError(reader_err_[reader_pos_++].GetType(), "");
        }
        r->RunCallback();
    }

    void AddReaderResult(const std::vector<MockReaderResult>& results) {
        reader_result_.insert(reader_result_.end(),
                results.begin(), results.end());    
    }
    
    void AddReaderErrors(const std::vector<ErrorCode>& errs) {
        reader_err_.insert(reader_err_.end(), errs.begin(), errs.end());
    }

    void AddMutationErrors(const std::vector<ErrorCode>& errs) {
        mu_err_.insert(mu_err_.end(), errs.begin(), errs.end());
    }
private:
    std::vector<ErrorCode> reader_err_;
    std::vector<ErrorCode> mu_err_;
    std::vector<MockReaderResult> reader_result_;
    int reader_pos_;
    int mu_pos_;
};

} // namespace tera

#endif  // TERA_SDK_TEST_MOCK_TABLE_H_
