// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_TABLETNODE_RPC_COMPACTOR_H
#define TERA_TABLETNODE_RPC_COMPACTOR_H

#include "common/base/scoped_ptr.h"
#include "common/mutex.h"
#include "glog/logging.h"

#include "proto/master_rpc.pb.h"
#include "proto/tabletnode_rpc.pb.h"

namespace tera {
namespace tabletnode {

template<class ResponseType>
class RpcCompactor {
public:
    RpcCompactor()
        : m_last_success_response(new ResponseType()) {}
    ~RpcCompactor() {}

    bool RpcExceptionHappened(uint64_t request_sequence_id,
                              ResponseType* response,
                              google::protobuf::Closure* done) {
        MutexLock lock(&m_mutex);
        if (request_sequence_id < m_last_success_sequence_id) {
            LOG(ERROR) << "invalid sequence id: " << request_sequence_id
                << ", last succeed sequence is: " << m_last_success_sequence_id;
            response->set_status(kInvalidSequenceId);
            done->Run();
            return false;
        } else if (request_sequence_id == m_last_success_sequence_id) {
            response->CopyFrom(*m_last_success_response);
            done->Run();
            return false;
        } else if (request_sequence_id < m_curr_sequence_id) {
            LOG(ERROR) << "invalid sequence id: " << request_sequence_id
                << ", current is: " << m_curr_sequence_id;
            response->set_status(kInvalidSequenceId);
            done->Run();
        } else if (request_sequence_id == m_curr_sequence_id) {
            LOG(WARNING) << "same sequence id: " << request_sequence_id;
            ResponseNode response_node(response, done);
            m_done_list.push_back(response_node);
            return true;
        } else {
            LOG(WARNING) << "sequence id: " << request_sequence_id
                << " should not larger than current: " << m_curr_sequence_id;
        }
        CHECK(m_done_list.size() == 0);
        m_curr_sequence_id = request_sequence_id;
        ResponseNode response_node(response, done);
        m_done_list.push_back(response_node);

        return true;
    }

    void FillResponseAndDone(ResponseType* response) {
        MutexLock lock(&m_mutex);
        m_last_success_response->CopyFrom(*response);
        m_last_success_sequence_id = m_curr_sequence_id;

        for (uint32_t i = 0; i < m_done_list.size(); ++i) {
            m_done_list[i].first->CopyFrom(*response);
            m_done_list[i].second->Run();
        }
        m_done_list.clear();
    }

private:
    typedef std::pair<ResponseType*, google::protobuf::Closure*> ResponseNode;

    mutable Mutex m_mutex;
    std::vector<ResponseNode> m_done_list;
    uint64_t m_last_success_sequence_id;
    uint64_t m_curr_sequence_id;
    scoped_ptr<ResponseType> m_last_success_response;
};

} // namespace tabletnode
} // namespace tera

#endif // TERA_TABLETNODE_RPC_COMPACTOR_H
