// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <glog/logging.h>
#include "common/timer.h"
#include "master/master_env.h"
#include "master/procedure_executor.h"
#include "proto/tabletnode_client.h"

DEFINE_int32(procedure_executor_thread_num, 10, "procedure executor thread pool number");

namespace tera {
namespace master {


void ProcedureWrapper::RunNextStage(std::shared_ptr<ProcedureExecutor> proc_executor) {
    proc_->RunNextStage();
    scheduling_.store(false);
    if (Done()) {
        VLOG(23) << "procedure executor remove procedure: " << ProcId();   
        proc_executor->RemoveProcedure(ProcId());
    }
}

ProcedureExecutor::ProcedureExecutor() : 
    running_(false),
    proc_index_(0),
    thread_pool_(new ThreadPool(FLAGS_procedure_executor_thread_num)){
    
}

bool ProcedureExecutor::Start() {
    std::unique_lock<std::mutex> lock(mutex_);
    if (running_) {
        return false;
    }
    running_ = true;
    schedule_thread_ = std::move(std::thread(&ProcedureExecutor::ScheduleProcedures, this));
    return true;
}

void ProcedureExecutor::Stop() {
    mutex_.lock();
    if (!running_) {
        mutex_.unlock();
        return;
    }
    running_ = false;
    cv_.notify_all();
    // it may takes a long time to join threads, so unlock mutex_ manually to minimize race condition 
    mutex_.unlock();

    schedule_thread_.join();

    thread_pool_->Stop(true);

}

uint64_t ProcedureExecutor::AddProcedure(std::shared_ptr<Procedure> proc) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!running_) {
        return 0;
    }
    std::string proc_id = proc->ProcId();
    if (procedure_indexs_.find(proc_id) != procedure_indexs_.end()) {
        return 0;        
    }
    procedure_indexs_.emplace(proc_id, ++proc_index_);
    procedures_.emplace(proc_index_, 
            std::shared_ptr<ProcedureWrapper>(new ProcedureWrapper(proc)));
    cv_.notify_all();
    return proc_index_;
}

bool ProcedureExecutor::RemoveProcedure(const std::string& proc_id) {
    std::unique_lock<std::mutex> lock(mutex_);
    auto it = procedure_indexs_.find(proc_id);
    if (it == procedure_indexs_.end()) {
        return false;
    }
    procedures_.erase(it->second);
    procedure_indexs_.erase(it);
    return true;
}

void ProcedureExecutor::ScheduleProcedures() {
    while (running_){
        std::map<uint64_t, std::shared_ptr<ProcedureWrapper>> procedures;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            while (procedures_.empty() && running_) {
                cv_.wait(lock);
            }
            procedures = procedures_;
        }
        
        for (auto it = procedures.begin(); it != procedures.end(); ++it) {
            auto proc = it->second;
            const std::string proc_id = proc->ProcId();
            if (proc->TrySchedule()) {
                ThreadPool::Task task = std::bind(&ProcedureWrapper::RunNextStage, proc, shared_from_this());
                thread_pool_->AddTask(task);
            }
        }
        // sleep 10ms before start next schedule round
        usleep(10 * 1000);
    }
}


}
}
