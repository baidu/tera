// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <iostream>

#include "boost/bind.hpp"
#include "common/mutex.h"
#include "common/thread_pool.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "sofa/pbrpc/pbrpc.h"

#include "proto/http.pb.h"
#include "sdk/tera.h"
#include "utils/counter.h"

DECLARE_int32(tera_http_ctrl_thread_num);
DECLARE_int32(tera_http_request_thread_num);

DECLARE_string(flagfile);
DECLARE_string(tera_http_port);

namespace tera {
namespace http {

tera::Counter read_request_counter;
tera::Counter read_response_counter;

tera::Counter write_request_counter;
tera::Counter write_response_counter;

class StatusCode {
public:
    enum code {
        kOk = 0,
        kError = 1
    };
    StatusCode() {
        code_ = kOk;
    }
    bool Ok() {
        return code_ == kOk;
    }
    void SetError(const std::string& reason) {
        code_ = kError;
        reason_ = reason;
    }
    std::string GetReason() {
        return reason_;
    }
private:
    code code_;
    std::string reason_;
};

class HttpProxyImpl : public tera::http::HttpProxy {
public:
    HttpProxyImpl(const std::string& confpath) {
        client_ = Client::NewClient(confpath, "terahttp", NULL);
        assert(client_ != NULL);

        request_pool_ = new common::ThreadPool(FLAGS_tera_http_request_thread_num);
        assert(request_pool_ != NULL);

        ctrl_pool_ = new common::ThreadPool(FLAGS_tera_http_ctrl_thread_num);
        assert(ctrl_pool_ != NULL);

        LogCounter();
    }
    virtual ~HttpProxyImpl() {}

private:
    void LogCounter();

    virtual void Get(google::protobuf::RpcController* controller,
                     const tera::http::GetRequest* request,
                     tera::http::GetResponse* response,
                     google::protobuf::Closure* done) {
        VLOG(25) << "accept RPC (Get)";
        read_request_counter.Add(1);
        common::ThreadPool::Task callback =
            boost::bind(&HttpProxyImpl::DoGet, this, controller, request, response, done);
        request_pool_->AddTask(callback);
    }
    virtual void DoGet(google::protobuf::RpcController* controller,
                       const tera::http::GetRequest* request,
                       tera::http::GetResponse* response,
                       google::protobuf::Closure* done);

    virtual void Put(google::protobuf::RpcController* controller,
                     const tera::http::PutRequest* request,
                     tera::http::PutResponse* response,
                     google::protobuf::Closure* done) {
        VLOG(25) << "accept RPC (Put)";
        write_request_counter.Add(1);
        common::ThreadPool::Task callback =
            boost::bind(&HttpProxyImpl::DoPut, this, controller, request, response, done);
        request_pool_->AddTask(callback);
    }
    virtual void DoPut(google::protobuf::RpcController* controller,
                       const tera::http::PutRequest* request,
                       tera::http::PutResponse* response,
                       google::protobuf::Closure* done);

    Table* OpenTableWithCache(const std::string& tablename, ErrorCode* err) {
        MutexLock l(&mutex_);
        std::map<std::string, tera::Table*>::iterator it = tables_.find(tablename);
        if (it == tables_.end()) {
            mutex_.Unlock();
            Table* table = client_->OpenTable(tablename, err);
            mutex_.Lock();
            if (table == NULL) {
                VLOG(20) << "[OpenTableWithCache] open table failed:" << tablename
                    << " for " << err->GetReason();
                return NULL;
            }
            VLOG(25) << "[OpenTableWithCache] open table done:" << tablename;
            tables_[tablename] = table;
            return table;
        } else {
            VLOG(25) << "[OpenTableWithCache] open table(cached):" << tablename;
            return it->second;
        }
    }

private:
    mutable Mutex mutex_;

    //       tablename          Table*
    std::map<std::string, tera::Table*> tables_;

    tera::Client* client_;
    common::ThreadPool* request_pool_;
    common::ThreadPool* ctrl_pool_;
};

struct PutContext {
      const tera::http::PutRequest* request_;
      tera::http::PutResponse* response_;
      google::protobuf::Closure* done_;
      Mutex mutex_;
      int32_t finished_count_;
};

struct GetContext {
      const tera::http::GetRequest* request_;
      tera::http::GetResponse* response_;
      google::protobuf::Closure* done_;
      Mutex mutex_;
      int32_t finished_count_;
};

struct GetContext* NewGetContext(const tera::http::GetRequest* request,
                                 tera::http::GetResponse* response,
                                 google::protobuf::Closure* done) {
    struct GetContext* c = new GetContext;
    c->request_ = request;
    c->response_ = response;
    c->done_ = done;
    c->finished_count_ = 0;
    return c;
}

struct PutContext* NewPutContext(const tera::http::PutRequest* request,
                                 tera::http::PutResponse* response,
                                 google::protobuf::Closure* done) {
    struct PutContext* c = new PutContext;
    c->request_ = request;
    c->response_ = response;
    c->done_ = done;
    c->finished_count_ = 0;
    return c;
}

void ReadCallback(tera::RowReader* reader) {
    struct GetContext* context = (struct GetContext*)reader->GetContext();
    const GetRequest* request = context->request_;
    GetResponse* response = context->response_;
    google::protobuf::Closure* done = context->done_;
    ErrorCode status = reader->GetError();
    {
        MutexLock (&context->mutex_);
        context->finished_count_++;
        HttpColumnReader* res = response->add_results();
        if (status.GetType() != tera::ErrorCode::kOK) {
            res->set_status(false);
            res->set_reason("fail to get record from table: " + status.GetReason());
        } else {
            res->set_rowkey(reader->RowName());
            res->set_columnfamily(reader->Family());
            res->set_qualifier(reader->Qualifier());
            res->set_status(true);
            res->set_value(reader->Value());
        }
        if (context->finished_count_ == request->reader_list_size()) {
            write_response_counter.Add(1);
            response->set_status(true);
            delete context;
            done->Run();
        }
        delete reader;
    }
}

void WriteCallback(tera::RowMutation* mutation) {
    struct PutContext* context = (struct PutContext*)mutation->GetContext();
    const PutRequest* request = context->request_;
    PutResponse* response = context->response_;
    google::protobuf::Closure* done = context->done_;
    ErrorCode status = mutation->GetError();
    {
        MutexLock (&context->mutex_);
        context->finished_count_++;
        HttpRowMutationResult* res = response->add_results();
        res->set_rowkey(mutation->RowKey());
        if (status.GetType() != tera::ErrorCode::kOK) {
            res->set_status(false);
            res->set_reason("fail to put record to table: " + status.GetReason());
        } else {
            res->set_status(true);
        }
        if (context->finished_count_ == request->mutation_list_size()) {
            write_response_counter.Add(1);
            response->set_status(true);
            delete context;
            done->Run();
        }
        delete mutation;
    }
}

void HttpProxyImpl::LogCounter() {
    LOG(INFO) << "[write] request: " << write_request_counter.Clear()
        << " response: " << write_response_counter.Clear();
    LOG(INFO) << "[read] request: " << read_request_counter.Clear()
        << " response: " << read_response_counter.Clear();
    common::ThreadPool::Task callback =
         boost::bind(&HttpProxyImpl::LogCounter, this);
    ctrl_pool_->DelayTask(1000, callback);
}

void HttpProxyImpl::DoGet(google::protobuf::RpcController* controller,
                          const tera::http::GetRequest* request,
                          tera::http::GetResponse* response,
                          google::protobuf::Closure* done) {
    VLOG(30) << request->ShortDebugString();
    tera::ErrorCode err;
    Table* table = NULL;

    // check arguments
    StatusCode status;
    if (!request->has_tablename()) {
        status.SetError("invalid request, expect <tablename>");
    }
    for (int i = 0; i < request->reader_list_size(); i++) {
        HttpColumnReader http_column_reader = request->reader_list(i);
        if (!http_column_reader.has_rowkey()
            || !http_column_reader.has_columnfamily()
            || !http_column_reader.has_qualifier()) {
            status.SetError("invalid request, expect <rowkey> & <columnfamily> & <qualifier>");
        }
    }

    // try open table
    if ((table = OpenTableWithCache(request->tablename(), &err)) == NULL) {
        std::string reason = "fail to open table " + request->tablename() + " : " + err.GetReason();
        status.SetError(reason);
    }

    // 如果在检查参数的过程中遇到很多错误，这里选择返回最后一个错误原因给用户，
    // 主要是为了保持错误检查代码尽量简洁易读。
    if (!status.Ok()) {
        read_response_counter.Add(1);
        response->set_reason(status.GetReason());
        response->set_status(false);
        done->Run();
        return;
    }

    struct GetContext* context = NewGetContext(request, response, done);
    for (int i = 0; i < request->reader_list_size(); i++) {
        HttpColumnReader http_column_reader = request->reader_list(i);
        std::string rowkey = http_column_reader.rowkey();
        tera::RowReader* reader = table->NewRowReader(rowkey);
        reader->AddColumn(http_column_reader.columnfamily(), http_column_reader.qualifier());
        reader->SetContext(context);
        reader->SetCallBack(ReadCallback);
        table->Get(reader);
    }
}

void HttpProxyImpl::DoPut(google::protobuf::RpcController* controller,
                          const tera::http::PutRequest* request,
                          tera::http::PutResponse* response,
                          google::protobuf::Closure* done) {
    VLOG(30) << request->ShortDebugString();
    tera::ErrorCode err;
    Table* table = NULL;

    // check arguments
    StatusCode status;
    if (!request->has_tablename()) {
        status.SetError("invalid request, expect <tablename>");
    }
    for (int i = 0; i < request->mutation_list_size(); i++) {
        HttpRowMutation http_row_mutation = request->mutation_list(i);
        if (!http_row_mutation.has_rowkey()) {
            status.SetError("invalid request, expect <rowkey>");
        }
        std::string type = http_row_mutation.type();
        if (type != "put" && type != "del-col" && type != "del-row") {
            status.SetError("invalid request, operation:put/del-col/del-row");
        }
    }

    // try open table
    if ((table = OpenTableWithCache(request->tablename(), &err)) == NULL) {
        std::string reason = "fail to open table " + request->tablename() + " : " + err.GetReason();
        status.SetError(reason);
    }

    // 如果在检查参数的过程中遇到很多错误，这里选择返回最后一个错误原因给用户，
    // 主要是为了保持错误检查代码尽量简洁易读。
    if (!status.Ok()) {
        write_response_counter.Add(1);
        response->set_reason(status.GetReason());
        response->set_status(false);
        done->Run();
        return;
    }

    struct PutContext* context = NewPutContext(request, response, done);
    for (int i = 0; i < request->mutation_list_size(); i++) {
        HttpRowMutation http_row_mutation = request->mutation_list(i);
        std::string rowkey = http_row_mutation.rowkey();
        tera::RowMutation* mutation = table->NewRowMutation(rowkey);
        for (int k = 0; k < http_row_mutation.columns_size(); k++) {
            MutationColumns col = http_row_mutation.columns(k);
            if (http_row_mutation.type() == "put") {
                mutation->Put(col.columnfamily(), col.qualifier(), col.value());
            } else if (http_row_mutation.type() == "del-col") {
                mutation->DeleteColumns(col.columnfamily(), col.qualifier());
            } else if (http_row_mutation.type() == "del-row") {
                mutation->DeleteRow();
            } else {
                abort(); // should checked at the start
            }
        }
        mutation->SetContext(context);
        mutation->SetCallBack(WriteCallback);
        table->ApplyMutation(mutation);
    }
}

bool InitRPCService(const std::string& ip_port, const std::string& confpath) {
    // 定义RpcServer
    sofa::pbrpc::RpcServerOptions options;
    options.work_thread_num = 8;
    sofa::pbrpc::RpcServer rpc_server(options);

    // 启动RpcServer
    if (!rpc_server.Start(ip_port)) {
        LOG(ERROR) << "start server failed";
        return false;
    }

    // 创建和注册服务
    tera::http::HttpProxy* http_service = new HttpProxyImpl(confpath);
    if (!rpc_server.RegisterService(http_service)) {
        LOG(ERROR) << "register service failed";
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
    tera::http::InitRPCService("0.0.0.0:" + FLAGS_tera_http_port, FLAGS_flagfile);
    return 0;
}
