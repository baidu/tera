// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_RPC_CLIENT_ASYNC_H_
#define TERA_RPC_CLIENT_ASYNC_H_

#include <string>

#include <functional>
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
    Callback closure;
    std::string tips;
    ThreadPool* thread_pool;

    RpcCallbackParam(sofa::pbrpc::RpcController* ctrler, const Request* req,
                     Response* resp, Callback cb, const std::string& str,
                     ThreadPool* tpool)
        : rpc_controller(ctrler), request(req), response(resp),
          closure(cb), tips(str), thread_pool(tpool) {}
};

class RpcClientBase {
public:
    static void SetOption(int32_t max_inflow, int32_t max_outflow,
                          int32_t pending_buffer_size, int32_t thread_num) {
        channel_options_.create_with_init = false;
        if (-1 != max_inflow) {
            rpc_client_options_.max_throughput_in = max_inflow;
        }
        if (-1 != max_outflow) {
            rpc_client_options_.max_throughput_out = max_outflow;
        }
        if (-1 != pending_buffer_size) {
            rpc_client_options_.max_pending_buffer_size = pending_buffer_size;
        }
        if (-1 != thread_num) {
            rpc_client_options_.work_thread_num = thread_num;
        }
        rpc_client_.ResetOptions(rpc_client_options_);

        sofa::pbrpc::RpcClientOptions new_options = rpc_client_.GetOptions();
        LOG(INFO) << "set rpc option: ("
            << "max_inflow: " << new_options.max_throughput_in
            << " MB/s, max_outflow: " << new_options.max_throughput_out
            << " MB/s, max_pending_buffer_size: " << new_options.max_pending_buffer_size
            << " MB, work_thread_num: " << new_options.work_thread_num
            << ")";
    }

    RpcClientBase() : rpc_channel_(NULL) {}
    virtual ~RpcClientBase() {}

protected:
    virtual void ResetClient(const std::string& server_addr) {
        std::map<std::string, sofa::pbrpc::RpcChannel*>::iterator it;
        mutex_.Lock();
        it = rpc_channel_list_.find(server_addr);
        if (it != rpc_channel_list_.end()) {
            rpc_channel_ = it->second;
        } else {
            sofa::pbrpc::RpcChannel* c = new sofa::pbrpc::RpcChannel(&rpc_client_,
                                                                     server_addr,
                                                                     channel_options_);
            if (c->Init()) {
                rpc_channel_ = rpc_channel_list_[server_addr] = c;
            } else {
                delete c;
                rpc_channel_ = NULL;
            }
        }
        mutex_.Unlock();
    }

protected:
    sofa::pbrpc::RpcChannel* rpc_channel_;

    static sofa::pbrpc::RpcChannelOptions channel_options_;
    static std::map<std::string, sofa::pbrpc::RpcChannel*> rpc_channel_list_;
    static sofa::pbrpc::RpcClientOptions rpc_client_options_;
    static sofa::pbrpc::RpcClient rpc_client_;
    static Mutex mutex_;
};

template<class ServerType>
class RpcClient : public RpcClientBase {
public:
    RpcClient(const std::string& addr)
        : sync_call_failed(false) {
        ResetClient(addr);
    }
    virtual ~RpcClient() {}

    std::string GetConnectAddr() const {
        return server_addr_;
    }

protected:
    virtual void ResetClient(const std::string& server_addr) {
        if (server_addr_ == server_addr) {
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
        if (rpc_channel_ == NULL) {
            server_client_.reset(NULL);
        } else {
            server_client_.reset(new ServerType(rpc_channel_));
        }
        server_addr_ = server_addr;
        // VLOG(5) << "reset connected address to: " << server_addr;
    }

    template <class Request, class Response, class Callback>
    bool SendMessageWithRetry(void(ServerType::*func)(
                              google::protobuf::RpcController*, const Request*,
                              Response*, google::protobuf::Closure*),
                              const Request* request, Response* response,
                              Callback closure, const std::string& tips,
                              int32_t rpc_timeout, ThreadPool* thread_pool = 0) {
        if (NULL == server_client_.get()) {
            // sync call
            if (!closure) {
                return false;
            }

            // async call
            ThreadPool::Task callback =
                std::bind(&RpcClient::template UserCallback<Request, Response, Callback>,
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
        (server_client_.get()->*func)(rpc_controller, request, response, done);

        // sync call
        if (!closure) {
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
        Callback closure = param->closure;
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
        if (!closure) {
            client->sync_call_failed = failed;
            client->sync_call_event.Set();
            return;
        }

        // async call
        ThreadPool::Task done =
            std::bind(&RpcClient::template UserCallback<Request, Response, Callback>,
            request, response, closure, failed, error);
        thread_pool->AddTask(done);
    }

    template <class Request, class Response, class Callback>
    static void UserCallback(const Request* request, Response* response,
                             Callback closure, bool failed, int error) {
        closure((Request*)request, response, failed, error);
    }

    virtual bool PollAndResetServerAddr() {
        return true;
    }

    virtual bool IsRetryStatus(const StatusCode& status) {
        return false;
    }

private:
    scoped_ptr<ServerType> server_client_;
    std::string server_addr_;

    bool sync_call_failed;
    AutoResetEvent sync_call_event;
};

} // namespace tera

#endif // TERA_RPC_CLIENT_ASYNC_H_
