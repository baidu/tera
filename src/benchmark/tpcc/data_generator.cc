// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: baorenyi@baidu.com

#include "benchmark/tpcc/data_generator.h"
#include "benchmark/tpcc/tpccdb.h"
#include "common/thread_pool.h"
#include "common/timer.h"

DECLARE_int32(warehouses_count);
DECLARE_int32(tpcc_thread_pool_size);
DECLARE_int32(generate_data_wait_times);

namespace tera {
namespace tpcc {

DataGenerator::DataGenerator(RandomGenerator* rand_gen, TpccDb* db)
    : event_(),
      rand_gen_(rand_gen), 
      db_(db), 
      now_datatime_(get_curtime_str()), 
      thread_pool_(FLAGS_tpcc_thread_pool_size) {
    for (int i = 0; i < kTpccTableCnt; ++i) {
        states_.push_back(std::make_pair(Counter(), Counter()));
    }
}

void DataGenerator::PrintJoinTimeoutInfo(int need_cnt, int table_enum_num) {
    if (need_cnt > states_[table_enum_num].first.Get() + states_[table_enum_num].second.Get()) {
        LOG(ERROR) << "table:" << kTpccTables[table_enum_num] 
                   << "[need/succ/fail]:[" 
                   << need_cnt << "/" 
                   << states_[table_enum_num].first.Get() << "/"
                   << states_[table_enum_num].first.Get() << "]";
    }
}

void DataGenerator::Join() {
    event_.Trigger();
    if (!event_.TimeWait(FLAGS_generate_data_wait_times)) {
		int stock_cnt = FLAGS_warehouses_count * kItemCount;
		int districts_cnt = FLAGS_warehouses_count * kDistrictCountPerWarehouse;
		int customers_cnt = districts_cnt * kCustomerCountPerDistrict;
        PrintJoinTimeoutInfo(kItemCount, kItemTable);
        PrintJoinTimeoutInfo(stock_cnt, kStockTable);
        PrintJoinTimeoutInfo(FLAGS_warehouses_count, kWarehouseTable);
        PrintJoinTimeoutInfo(districts_cnt, kDistrictTable);
        PrintJoinTimeoutInfo(customers_cnt, kCustomerTable);
        PrintJoinTimeoutInfo(customers_cnt, kCustomerLastIndex);
        PrintJoinTimeoutInfo(customers_cnt, kHistoryTable);
    }
}

void DataGenerator::GenStocks(int32_t warehouse_id) {
    IdSet original_ids = PickUniqueIdSet(rand_gen_, kItemCount / 10, 1, kItemCount);
    event_.AddEventSources(kItemCount);
    for (int id = 1; id <= kItemCount; ++id) {
        bool is_original = original_ids.find(id) != original_ids.end();
        PushToInsertQueue(std::bind(&DataGenerator::GenStock, this, id, warehouse_id, is_original));
    }
}

void DataGenerator::GenStock(int32_t id, int32_t warehouse_id, bool is_original) {
    Stock s(id, warehouse_id, is_original, rand_gen_);
    VLOG(12) << s.ToString();
    db_->InsertStock(s) ? states_[kStockTable].first.Inc() : states_[kStockTable].second.Inc();
    event_.Complete();
}

void DataGenerator::GenCustomers(int32_t district_id, int32_t warehouse_id) {
    IdSet bad_credit_ids = PickUniqueIdSet(rand_gen_, 
            kCustomerCountPerDistrict / 10, 1, kCustomerCountPerDistrict);
    event_.AddEventSources(kCustomerCountPerDistrict);
    for (int c_id = 1; c_id <= kCustomerCountPerDistrict; ++c_id) {
        bool is_bad_credit = bad_credit_ids.find(c_id) != bad_credit_ids.end();
        Customer c(c_id, district_id, warehouse_id, now_datatime_, is_bad_credit, rand_gen_);
        VLOG(12) << c.ToString();
        db_->InsertCustomer(c) ? states_[kCustomerTable].first.Inc() : states_[kCustomerTable].second.Inc();
    }
    event_.Complete(kCustomerCountPerDistrict);
}

void DataGenerator::GenHistorys(int32_t district_id, int32_t warehouse_id) {
    event_.AddEventSources(kCustomerCountPerDistrict);
    for (int h_id = 1; h_id <= kCustomerCountPerDistrict; ++h_id) {
        History h(h_id, district_id, warehouse_id, now_datatime_, rand_gen_);
        VLOG(12) << h.ToString();
        db_->InsertHistory(h) ? states_[kHistoryTable].first.Inc() : states_[kHistoryTable].second.Inc();
    }
    event_.Complete(kCustomerCountPerDistrict);
}

void DataGenerator::GenOrderLines(int cnt, int32_t order_id, int32_t district_id, 
                                  int32_t warehouse_id, bool new_order) {
    event_.AddEventSources(cnt);
    for (int i = 1; i <= cnt; ++i) {
        OrderLine ol(order_id, district_id, warehouse_id, i, new_order, now_datatime_, rand_gen_);
        VLOG(12) << ol.ToString();
        db_->InsertOrderLine(ol) ? states_[kOrderLineTable].first.Inc() : states_[kOrderLineTable].second.Inc();
    }
    event_.Complete(cnt);
}

void DataGenerator::GenOrders(int32_t d_id, int32_t w_id) {
    std::vector<int> disorder_ids = rand_gen_->MakeDisOrderList(1, kCustomerCountPerDistrict);
    event_.AddEventSources(kCustomerCountPerDistrict);
    for (int o_id = 1; o_id <= kCustomerCountPerDistrict; ++o_id) {
        bool new_order = (kCustomerCountPerDistrict - kInitNewOrderCountPerDistrict) < o_id;
        int32_t c_id = disorder_ids[o_id];
        Order o(o_id, c_id, d_id, w_id, new_order, now_datatime_, rand_gen_);
        // insert order line and new order first
        // this use sync interface
        GenOrderLines(o.o_ol_cnt, o_id, d_id, w_id, new_order);
        if (new_order) {
            event_.AddEventSources(1);
            NewOrder no(o_id, d_id, w_id);
            VLOG(12) << no.ToString();
            db_->InsertNewOrder(no) ? states_[kNewOrderTable].first.Inc() : states_[kNewOrderTable].second.Inc();
            event_.Complete(1);
        }
        // wait orderline and neworder insert done
        VLOG(12) << o.ToString();
        db_->InsertOrder(o) ? states_[kOrderTable].first.Inc() : states_[kOrderTable].second.Inc();
    }
    event_.Complete(kCustomerCountPerDistrict);
}

void DataGenerator::GenDistricts(int32_t warehouse_id) {
    event_.AddEventSources(kDistrictCountPerWarehouse);
    for (int d_id = 1; d_id <= kDistrictCountPerWarehouse; ++d_id) {
        District d(d_id, warehouse_id, rand_gen_);
        VLOG(12) << d.ToString();
        db_->InsertDistrict(d) ? states_[kDistrictTable].first.Inc() : states_[kDistrictTable].second.Inc();
        GenCustomers(d_id, warehouse_id);
        GenHistorys(d_id, warehouse_id);

        GenOrders(d_id, warehouse_id);
    } 
    event_.Complete(kDistrictCountPerWarehouse);
}

void DataGenerator::GenWarehouses() {
    event_.AddEventSources(FLAGS_warehouses_count);
    for (int32_t w_id = 1; w_id <= FLAGS_warehouses_count; ++w_id) {
        GenStocks(w_id);
        Warehouse w(w_id, rand_gen_);
        VLOG(12) << w.ToString();
        db_->InsertWarehouse(w) ? states_[kWarehouseTable].first.Inc() : states_[kWarehouseTable].second.Inc();

        GenDistricts(w_id);
    }
    event_.Complete(FLAGS_warehouses_count);
}

void DataGenerator::GenItems() {
    IdSet original_ids = PickUniqueIdSet(rand_gen_, kItemCount / 10, 1, kItemCount);
    event_.AddEventSources(kItemCount);
    for (int i_id = 1; i_id <= kItemCount; ++i_id) {
        bool is_original = original_ids.find(i_id) != original_ids.end();
        PushToInsertQueue(std::bind(&DataGenerator::GenItem, this, i_id, is_original));
    }
}

void DataGenerator::GenItem(int32_t item_id, bool is_original) {
    Item item(item_id, is_original, rand_gen_);
    VLOG(12) << item.ToString();
    db_->InsertItem(item) ? states_[kItemTable].first.Inc() : states_[kItemTable].second.Inc();
    event_.Complete();
}

void DataGenerator::PushToInsertQueue(const ThreadPool::Task& task) {
    while(thread_pool_.PendingNum() > FLAGS_tpcc_thread_pool_size / 2) {
        usleep(100);
    }
    thread_pool_.AddTask(task);
    VLOG(12) << "thread_pool pending num = " << thread_pool_.PendingNum();
}

} // namespace tpcc
} // namespace tera
