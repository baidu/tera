#include <iostream>
#include <string>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include "sdk/sdk_zk.h"

#include "sdk/timeoracle_client_impl.h"
#include <thread>

DEFINE_int64(client_thread_num, 10, "");

using namespace tera;
using namespace tera::timeoracle;

std::shared_ptr<common::ThreadPool> g_thread_pool;


void worker() {
    tera::sdk::ClusterFinder* cluster_finder = sdk::NewTimeoracleClusterFinder();
    if (!cluster_finder) {
        std::cerr << "Create cluster failed, use -h to see configuer items\n";
        exit(0);
    }
    tera::timeoracle::TimeoracleClientImpl client(g_thread_pool.get(), cluster_finder);

    while (true) {
        int64_t st = client.GetTimestamp(1);
        if (st <= 0) {
            std::cout << "rpc failed" << std::endl;
            ThisThread::Sleep(200);
        }
    }
}

int main(int argc, char** argv) {
    if (argc > 1 && (std::string(argv[1]) == "-h" || std::string(argv[1]) == "help")) {
        std::cout << argv[0] << " --client_thread_num=<Thread Num> \n"
                  << "    and zk/ins configures should be set in tera.flag or via command line" << std::endl;
        return 0;
    }
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    g_thread_pool.reset(new common::ThreadPool(FLAGS_client_thread_num + 1));

    std::vector<std::thread>    thread_list;
    for (int64_t i = 0; i < FLAGS_client_thread_num; ++i) {
        thread_list.push_back(std::thread(&worker));
    }

    for (auto& th : thread_list) {
        th.join();
    }

    return 0;
}
