// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_RPC_CLIENT_ASYNC_H_
#define TERA_RPC_CLIENT_ASYNC_H_

#include <string>

#include <boost/bind.hpp>
#include <glog/logging.h>
#include <sofa/pbrpc/pbrpc.h>

#include "common/base/scoped_ptr.h"
#include "common/event.h"
#include "common/net/ip_address.h"
#include "common/this_thread.h"
#include "common/thread_pool.h"
#include "proto/proto_helper.h"
#include "proto/status_code.pb.h"

namespace tera {

template <class Request, class Response, class Callback>
struct RpcCallbackParam {
    sofa::pbrpc::RpcController* rpc_controller;
    const Request* request;
    Response* response;
    Callback* closure;
    std::string tips;
    ThreadPool* thread_pool;

    RpcCallbackParam(sofa::pbrpc::RpcController* ctrler, const Request* req,
                     Response* resp, Callback* cb, const std::string& str,
                     ThreadPool* tpool)
        : rpc_controller(ctrler), request(req), response(resp),
          closure(cb), tips(str), thread_pool(tpool) {}
};

class RpcClientBase {
public:
    void SetOption(int32_t max_inflow, int32_t max_outflow,
                   int32_t pending_buffer_size) {
        if (-1 != max_inflow) {
            m_rpc_client_options.max_throughput_in = max_inflow;
        }
        if (-1 != max_outflow) {
            m_rpc_client_options.max_throughput_out = max_outflow;
        }
        if (-1 != pending_buffer_size) {
            m_rpc_client_options.max_pending_buffer_size = pending_buffer_size;
        }
        m_rpc_client->ResetOptions(m_rpc_client_options);

        sofa::pbrpc::RpcClientOptions new_options = m_rpc_client->GetOptions();
        LOG(INFO) << "set rpc option: ("
            << "max_inflow: " << new_options.max_throughput_in
            << " MB/s, max_outflow: " << new_options.max_throughput_out
            << " MB/s, max_pending_buffer_size: " << new_options.max_pending_buffer_size
            << " MB, work_thread_num: " << new_options.work_thread_num
            << ")";
    }

    RpcClientBase(ThreadPool* thread_pool = NULL,
                  int32_t thread_num = -1) : m_thread_pool(thread_pool) {
        // pbrpc doesn't support reset thread_num,
        // we have to set it before start
        m_rpc_client_options.callback_thread_num = 1;
        if (thread_num > 0) {
            m_rpc_client_options.work_thread_num = thread_num;
        }
        m_rpc_client.reset(new sofa::pbrpc::RpcClient(m_rpc_client_options));
    }
    virtual ~RpcClientBase() {}

    sofa::pbrpc::RpcClient* GetRpcClient() { return m_rpc_client.get(); }
    ThreadPool* GetThreadPool() { return m_thread_pool; }

private:
    sofa::pbrpc::RpcClientOptions m_rpc_client_options;
    scoped_ptr<sofa::pbrpc::RpcClient> m_rpc_client;
    ThreadPool* m_thread_pool;
};

void InitDefaultRpcClientBase(ThreadPool* thread_pool = NULL, int32_t thread_num = -1);
void SetDefaultRpcClientBaseOption(int32_t max_inflow, int32_t max_outflow,
                                   int32_t pending_buffer_size);
RpcClientBase* GetDefaultRpcClientBase();

template<class ServerType>
class RpcClient {
public:
    RpcClient(RpcClientBase* base, const std::string& server_addr) {
        m_base = base;
        sofa::pbrpc::RpcChannelOptions channel_options;
        m_rpc_channel.reset(new sofa::pbrpc::RpcChannel(m_base->GetRpcClient(),
                                                        server_addr, channel_options));
        m_server_client.reset(new ServerType(m_rpc_channel.get()));
        m_server_addr = server_addr;
    }
    virtual ~RpcClient() {}

    std::string GetConnectAddr() const {
        return m_server_addr;
    }

protected:
    template <class Request, class Response, class Callback>
    bool SendMessageWithRetry(void(ServerType::*func)(
                              google::protobuf::RpcController*, const Request*,
                              Response*, google::protobuf::Closure*),
                              const Request* request, Response* response,
                              Callback* closure, const std::string& tips,
                              int32_t rpc_timeout, ThreadPool* thread_pool = NULL) {
        if (thread_pool == NULL) {
            thread_pool = m_base->GetThreadPool();
        }
        if (NULL == m_server_client.get()) {
            // sync call
            if (closure == NULL) {
                return false;
            }

            // async call
            ThreadPool::Task callback =
                boost::bind(&RpcClient::template UserCallback<Request, Response, Callback>,
                request, response, closure, true,
                (int)sofa::pbrpc::RPC_ERROR_RESOLVE_ADDRESS);
            thread_pool->AddTask(callback);
            return true;
        }
        sofa::pbrpc::RpcController* rpc_controller =
            new sofa::pbrpc::RpcController;
        rpc_controller->SetTimeout(rpc_timeout);
        RpcCallbackParam<Request, Response, Callback>* param =
            new RpcCallbackParam<Request, Response, Callback>(rpc_controller,
                    request, response, closure, tips, thread_pool);
        google::protobuf::Closure* done = google::protobuf::NewCallback(
            &RpcClient::template RpcCallback<Request, Response, Callback>,
            this, param);
        (m_server_client.get()->*func)(rpc_controller, request, response, done);

        // sync call
        if (closure == NULL) {
            sync_call_event.Wait();
            return (!sync_call_failed);
        }

        // async call
        return true;
    }

    template <class Request, class Response, class Callback>
    static void RpcCallback(RpcClient<ServerType>* client,
                            RpcCallbackParam<Request, Response, Callback>* param) {
        sofa::pbrpc::RpcController* rpc_controller = param->rpc_controller;
        const Request* request = param->request;
        Response* response = param->response;
        Callback* closure = param->closure;
        ThreadPool* thread_pool = param->thread_pool;

        bool failed = rpc_controller->Failed();
        int error = rpc_controller->ErrorCode();
        if (failed) {
            // LOG(ERROR) << "RpcRequest failed: " << param->tip
            //    << ". Reason: " << rpc_controller->ErrorText();
        }
        delete rpc_controller;
        delete param;

        // sync call
        if (closure == NULL) {
            client->sync_call_failed = failed;
            client->sync_call_event.Set();
            return;
        }

        // async call
        ThreadPool::Task done =
            boost::bind(&RpcClient::template UserCallback<Request, Response, Callback>,
            request, response, closure, failed, error);
        thread_pool->AddTask(done);
    }

    template <class Request, class Response, class Callback>
    static void UserCallback(const Request* request, Response* response,
                             Callback* closure, bool failed, int error) {
        closure->Run((Request*)request, response, failed, error);
    }

private:
    RpcClientBase* m_base;
    scoped_ptr<sofa::pbrpc::RpcChannel> m_rpc_channel;
    scoped_ptr<ServerType> m_server_client;
    std::string m_server_addr;

    bool sync_call_failed;
    AutoResetEvent sync_call_event;
};

} // namespace tera

#endif // TERA_RPC_CLIENT_ASYNC_H_
