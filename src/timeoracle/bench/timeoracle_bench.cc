#include <iostream>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include "common/mutex.h"
#include "common/thread_pool.h"
#include "common/this_thread.h"

#include "proto/timeoracle_client.h"

using namespace tera;
using namespace tera::timeoracle;

int main(int argc, char** argv) {
    ::google::ParseCommandLineFlags(&argc, &argv, true);

    std::shared_ptr<common::ThreadPool> thread_pool(new common::ThreadPool(10));
    timeoracle::TimeoracleClient::SetThreadPool(thread_pool.get());

    GetTimestampRequest request;
    GetTimestampResponse response;

    request.set_number(10);
    timeoracle::TimeoracleClient timeoracle_client("127.0.0.1:8881");

    uint64_t x = 10000000000000ULL;
    uint64_t i = 0;
    while (++i) {
        if ((i % 10) == 0) {
            request.set_number(x);
        } else {
            request.set_number(10);
        }

        if (timeoracle_client.GetTimestamp(&request, &response, nullptr)) {
            if (!response.has_status()) {
                std::cout << "success ,timestamp=" << response.start_timestamp()
                    << ",number=" << response.number() << std::endl;
                continue;
            }
        }
        std::cout << "failed,,code=" << (int)response.status() << std::endl;
        ThisThread::Sleep(200);
    }

    return 0;
}
