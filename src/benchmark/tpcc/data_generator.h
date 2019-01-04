// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: baorenyi@baidu.com

#ifndef TERA_BENCHMARK_TPCC_DATA_GENERATOR_H
#define TERA_BENCHMARK_TPCC_DATA_GENERATOR_H

#include <stdint.h>
#include <string>

#include "benchmark/tpcc/random_generator.h"
#include "benchmark/tpcc/tpccdb.h"
#include "common/counter.h"
#include "common/event.h"
#include "common/thread_pool.h"

namespace tera {
namespace tpcc {

class DataGenerator {
 public:
  DataGenerator(RandomGenerator* random_gen, TpccDb* db);
  ~DataGenerator() {}
  void GenWarehouses();
  void GenItems();
  void Join();

 private:
  void PrintJoinTimeoutInfo(int need_cnt, int table_enum_num);

  // for generate data
  void GenStocks(int32_t warehouse_id);
  void GenCustomers(int32_t district_id, int32_t warehouse_id);
  void GenHistorys(int32_t district_id, int32_t warehouse_id);
  void GenOrderLines(int cnt, int32_t order_id, int32_t district_id, int32_t warehouse_id,
                     bool new_order);
  void GenOrders(int32_t district_id, int32_t warehouse_id);
  void GenDistricts(int32_t warehouse_id);

  void GenItem(int32_t item_id, bool is_original);
  void GenStock(int32_t id, int32_t warehouse_id, bool is_original);

  // for async insert
  void PushToInsertQueue(const ThreadPool::Task& task);

 private:
  typedef std::vector<std::pair<Counter, Counter>> InsertStates;
  CompletedEvent event_;
  RandomGenerator* rand_gen_;
  TpccDb* db_;
  InsertStates states_;
  std::string now_datatime_;
  common::ThreadPool thread_pool_;
};

}  // namespace tpcc
}  // namespace tera

#endif /* TERA_BENCHMARK_TPCC_DATA_GENERATOR_H */
