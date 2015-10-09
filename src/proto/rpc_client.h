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
    static void SetOption(int32_t max_inflow, int32_t max_outflow,
                          int32_t pending_buffer_size, int32_t thread_num) {
        if (-1 != max_inflow) {
            m_rpc_client_options.max_throughput_in = max_inflow;
        }
        if (-1 != max_outflow) {
            m_rpc_client_options.max_throughput_out = max_outflow;
        }
        if (-1 != pending_buffer_size) {
            m_rpc_client_options.max_pending_buffer_size = pending_buffer_size;
        }
        if (-1 != thread_num) {
            m_rpc_client_options.work_thread_num = thread_num;
        }
        m_rpc_client.ResetOptions(m_rpc_client_options);

        sofa::pbrpc::RpcClientOptions new_options = m_rpc_client.GetOptions();
        LOG(INFO) << "set rpc option: ("
            << "max_inflow: " << new_options.max_throughput_in
            << " MB/s, max_outflow: " << new_options.max_throughput_out
            << " MB/s, max_pending_buffer_size: " << new_options.max_pending_buffer_size
            << " MB, work_thread_num: " << new_options.work_thread_num
            << ")";
    }

    RpcClientBase() : m_rpc_channel(NULL) {}
    virtual ~RpcClientBase() {}

protected:
    virtual void ResetClient(const std::string& server_addr) {
        std::map<std::string, sofa::pbrpc::RpcChannel*>::iterator it;
        m_mutex.Lock();
        it = m_rpc_channel_list.find(server_addr);
        if (it != m_rpc_channel_list.end()) {
            m_rpc_channel = it->second;
        } else {
            m_rpc_channel = m_rpc_channel_list[server_addr]
                = new sofa::pbrpc::RpcChannel(&m_rpc_client, server_addr,
                                              m_channel_options);
        }
        m_mutex.Unlock();
    }

protected:
    sofa::pbrpc::RpcChannel* m_rpc_channel;

    static sofa::pbrpc::RpcChannelOptions m_channel_options;
    static std::map<std::string, sofa::pbrpc::RpcChannel*> m_rpc_channel_list;
    static sofa::pbrpc::RpcClientOptions m_rpc_client_options;
    static sofa::pbrpc::RpcClient m_rpc_client;
    static Mutex m_mutex;
};

template<class ServerType>
class RpcClient : public RpcClientBase {
public:
    RpcClient(const std::string& addr) {
        ResetClient(addr);
    }
    virtual ~RpcClient() {}

    std::string GetConnectAddr() const {
        return m_server_addr;
    }

protected:
    virtual void ResetClient(const std::string& server_addr) {
        if (m_server_addr == server_addr) {
            // VLOG(5) << "address [" << server_addr << "] not be applied";
            return;
        }
        /*
        IpAddress ip_address(server_addr);
        if (!ip_address.IsValid()) {
            LOG(ERROR) << "invalid address: " << server_addr;
            return;
        }
        */
        RpcClientBase::ResetClient(server_addr);
        m_server_client.reset(new ServerType(m_rpc_channel));
        m_server_addr = server_addr;
        // VLOG(5) << "reset connected address to: " << server_addr;
    }

    template <class Request, class Response, class Callback>
    bool SendMessageWithRetry(void(ServerType::*func)(
                              google::protobuf::RpcController*, const Request*,
                              Response*, google::protobuf::Closure*),
                              const Request* request, Response* response,
                              Callback* closure, const std::string& tips,
                              int32_t rpc_timeout, ThreadPool* thread_pool = 0) {
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

    virtual bool PollAndResetServerAddr() {
        return true;
    }

    virtual bool IsRetryStatus(const StatusCode& status) {
        return false;
    }

private:
    scoped_ptr<ServerType> m_server_client;
    std::string m_server_addr;

    bool sync_call_failed;
    AutoResetEvent sync_call_event;
};

} // namespace tera

#endif // TERA_RPC_CLIENT_ASYNC_H_
