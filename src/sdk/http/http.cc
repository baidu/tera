// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#include <iostream>

#include "gflags/gflags.h"
#include "proto/http.pb.h"
#include "sdk/tera.h"
#include "sofa/pbrpc/pbrpc.h"

DECLARE_string(flagfile);

namespace tera {
namespace http {

class HttpProxyImpl : public tera::http::HttpProxy {
public:
    HttpProxyImpl(const std::string& confpath) {
        client_ = Client::NewClient(confpath, NULL);
        assert(client_ != NULL);
    }
    virtual ~HttpProxyImpl() {}

private:
    virtual void Get(google::protobuf::RpcController* controller,
                     const tera::http::GetRequest* request,
                     tera::http::GetResponse* response,
                     google::protobuf::Closure* done) {
        tera::ErrorCode err;
        Table* table = NULL;
        std::string value;
        // TODO(taocipian) cache opened table
        if ((table = client_->OpenTable(request->tablename(), &err)) == NULL) {
            response->set_reason("fail to open table " + request->tablename() + " : " + err.GetReason());
        } else if (!table->Get(request->rowkey(), request->cf(), request->qu(), &value, &err, 0)) {
            response->set_reason("fail to get record from table: " + err.GetReason());
        } else {
            response->set_value(value);
        }

        response->set_status(err.GetType() == ErrorCode::kOK);
        done->Run();
        delete table;
    }

private:
    tera::Client* client_;
};


bool InitRPCService(const std::string& ip_port, const std::string& confpath) {
    // 定义RpcServer
    sofa::pbrpc::RpcServerOptions options;
    options.work_thread_num = 8;
    sofa::pbrpc::RpcServer rpc_server(options);

    // 启动RpcServer
    if (!rpc_server.Start(ip_port)) {
        SLOG(ERROR, "start server failed");
        return false;
    }

    // 创建和注册服务
    tera::http::HttpProxy* echo_service = new HttpProxyImpl(confpath);
    if (!rpc_server.RegisterService(echo_service)) {
        SLOG(ERROR, "register service failed");
        return false;
    }

    // 等待SIGINT/SIGTERM退出信号
    rpc_server.Run();

    // 停止Server
    rpc_server.Stop();
    return true;
}

} // namespace http
} // namespace tera

int main(int argc, char** argv)
{
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    tera::http::InitRPCService("0.0.0.0:12321", FLAGS_flagfile);
    return 0;
}
