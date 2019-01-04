// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: baorenyi@baidu.com

#ifndef TERA_BENCHMARK_TPCC_DRIVER_H
#define TERA_BENCHMARK_TPCC_DRIVER_H

#include <stdint.h>
#include <string>

#include "benchmark/tpcc/random_generator.h"
#include "benchmark/tpcc/tpccdb.h"
#include "common/counter.h"
#include "common/event.h"
#include "common/thread_pool.h"

namespace tera {
namespace tpcc {

class Driver {
 public:
  Driver(RandomGenerator* random_gen, TpccDb* db);
  ~Driver() {}
  void RunTransactions();
  void Join();

 private:
  void PrintJoinTimeoutInfo(int need_cnt, int table_enum_num);

  // for run transaction
  void RunOneTransaction();
  //
  void RunStockLevelTxn();

  void RunOrderStatusTxn();

  void RunDeliveryTxn();

  void RunPaymentTxn();

  void RunNewOrderTxn();

  // for async run txn
  void PushToInsertQueue(const ThreadPool::Task& task);

  int32_t FindWareHouse();

  int32_t FindDistrict();

  int32_t FindCustomerId();

  int32_t FindItemId();

 private:
  typedef std::vector<std::pair<Counter, Counter>> TxnStates;
  CompletedEvent event_;
  RandomGenerator* rand_gen_;
  TpccDb* db_;
  TxnStates states_;
  std::string now_datatime_;
  common::ThreadPool thread_pool_;
};

}  // namespace tpcc
}  // namespace tera

#endif /* TERA_BENCHMARK_TPCC_DATA_GENERATOR_H */
