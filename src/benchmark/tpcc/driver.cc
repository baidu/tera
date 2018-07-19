// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: baorenyi@baidu.com

#include "benchmark/tpcc/driver.h"
#include "benchmark/tpcc/tpccdb.h"
#include "common/thread_pool.h"
#include "common/timer.h"

DECLARE_int32(driver_wait_times);
DECLARE_int32(warehouses_count);
DECLARE_int32(tpcc_run_gtxn_thread_pool_size);
DECLARE_int64(transactions_count);

namespace tera {
namespace tpcc {

Driver::Driver(RandomGenerator* rand_gen, TpccDb* db)
    : event_(),
      rand_gen_(rand_gen), 
      db_(db), 
      now_datatime_(get_curtime_str()),
      thread_pool_(FLAGS_tpcc_run_gtxn_thread_pool_size) {
}

void Driver::PrintJoinTimeoutInfo(int need_cnt, int table_enum_num) {
    if (need_cnt < states_[table_enum_num].first.Get() + states_[table_enum_num].second.Get()) {
        LOG(ERROR) << "table:" << kTpccTables[table_enum_num] 
                   << "[need/succ/fail]:[" 
                   << need_cnt << "/" 
                   << states_[table_enum_num].first.Get() << "/"
                   << states_[table_enum_num].first.Get() << "]";
    }
}

void Driver::RunTransactions() {
    for (int64_t i = 0; i < FLAGS_transactions_count; ++i) {
        RunOneTransaction();
    }
}

void Driver::Join() {
    event_.Trigger();
    if (!event_.TimeWait(FLAGS_driver_wait_times)) {
        // TODO
    }
}

void Driver::RunOneTransaction() {
    int rand_num = rand_gen_->GetRandom(1, 100);
    if (rand_num <= kTpccTransactionRatios[0]) {        //  %4 do stock_level
        RunStockLevelTxn();
    } else if (rand_num <= kTpccTransactionRatios[1]) { //  %4 do order_status
        RunOrderStatusTxn();
    } else if (rand_num <= kTpccTransactionRatios[2]) { //  %4 do delivery
        RunDeliveryTxn();
    } else if (rand_num <= kTpccTransactionRatios[3]) { // %43 do payment
        RunPaymentTxn();
    } else {                                            // %45 do new_order
        RunNewOrderTxn();
    }
} 

void Driver::RunStockLevelTxn() {
    int32_t threshold = rand_gen_->GetRandom(kMinStockLevelThreshold, kMaxStockLevelThreshold); 
    StockLevelResult ret;
    db_->StockLevelTxn(FindWareHouse(), FindDistrict(), threshold, &ret);
}

void Driver::RunOrderStatusTxn() {
    int x = rand_gen_->GetRandom(1, 100);
    OrderStatusResult ret;
    if (x <= 60) {
        // 60% order_status by lastname
        std::string last_name = GenLastName(rand_gen_, kCustomerCountPerDistrict);
        db_->OrderStatusTxn(true, FindWareHouse(), FindDistrict(), 
                -1, last_name, &ret);
    } else {
        // 40% order_status by customer_id
        db_->OrderStatusTxn(false, FindWareHouse(), FindDistrict(), 
                FindCustomerId(), "", &ret);
    }
}

void Driver::RunDeliveryTxn() {
    int32_t carrier_id = rand_gen_->GetRandom(kMinCarrierId, kMaxCarrierId);
    DeliveryResult ret;;
    db_->DeliveryTxn(FindWareHouse(), carrier_id, get_curtime_str(), &ret); 
}

void Driver::RunPaymentTxn() {
    int32_t warehouse_id = FindWareHouse();
    int32_t district_id = FindDistrict();

    float h_amount = rand_gen_->MakeFloat(kRuntimeMinAmount, kRuntimeMaxAmount, 
            kRuntimeAmountDigits);

    int32_t customer_warehouse_id = -1;
    int32_t customer_district_id = -1;

    int x = rand_gen_->GetRandom(1, 100);
    
    // set customer c_w_id and c_d_id 
    if (FLAGS_warehouses_count == 1 && x <= 85) {
        // 85% payment through local warehouse (or only one warehouse)
        customer_warehouse_id = warehouse_id;
        customer_district_id = district_id;
    } else {
        // 15% payment through remote warehouse
        customer_warehouse_id = 
            rand_gen_->GetRandom(1, FLAGS_warehouses_count, warehouse_id);
        customer_district_id = FindDistrict(); 
    }

    x = rand_gen_->GetRandom(1, 100);
    PaymentResult ret;
    if (x <= 60) {
        // 60% payment by lastname
        std::string last_name = GenLastName(rand_gen_, kCustomerCountPerDistrict);
        db_->PaymentTxn(true, warehouse_id, district_id, 
                customer_warehouse_id, customer_district_id, -1,
                last_name, h_amount, &ret);
    } else {
        // 40% payment by customer_id
        db_->PaymentTxn(false, warehouse_id, district_id, 
                customer_warehouse_id, customer_district_id, FindCustomerId(),
                "", h_amount, &ret);
    }
}

void Driver::RunNewOrderTxn() {
    int32_t warehouse_id = FindWareHouse();

    // init NewOrderInfo
    NewOrderInfo info;
    // 1% of new_order transactions will be failed
    info.need_failed = rand_gen_->GetRandom(1,100) == 1 ? true : false; 
    info.o_ol_cnt = rand_gen_->GetRandom(kMinOrderLineCnt, kMaxOrderLineCnt);

    info.ol_supply_w_ids.reserve(info.o_ol_cnt);
    info.ol_i_ids.reserve(info.o_ol_cnt);
    info.ol_quantities.reserve(info.o_ol_cnt);
    info.o_all_local = 1;
    for (int32_t i = 0; i < info.o_ol_cnt; ++i) {
        // 1% of orderlines will be remote order
        bool remote = rand_gen_->GetRandom(1, 100) == 1 ? true : false;
        if (FLAGS_warehouses_count > 1 && remote) {
            info.ol_supply_w_ids.emplace_back(
                    rand_gen_->GetRandom(1, FLAGS_warehouses_count, warehouse_id));
            info.o_all_local = 0;
        } else {
            info.ol_supply_w_ids.emplace_back(warehouse_id);
        }
        info.ol_i_ids.emplace_back(FindItemId());
        info.ol_quantities.emplace_back(
                rand_gen_->GetRandom(1, kMaxOrderLineQuantity));
    }

    NewOrderResult ret;
    db_->NewOrderTxn(warehouse_id, FindDistrict(), FindCustomerId(), info, &ret);
}

void Driver::PushToInsertQueue(const ThreadPool::Task& task) {
    while(thread_pool_.PendingNum() > FLAGS_tpcc_run_gtxn_thread_pool_size / 2) {
        usleep(100);
    }
    thread_pool_.AddTask(task);
    VLOG(12) << "thread_pool pending num = " << thread_pool_.PendingNum();
}

int32_t Driver::FindWareHouse() {
    return rand_gen_->GetRandom(1, FLAGS_warehouses_count);
}

int32_t Driver::FindDistrict() {
    return rand_gen_->GetRandom(1, kDistrictCountPerWarehouse);
}

int32_t Driver::FindCustomerId() {
    return rand_gen_->NURand(1023, 1, kCustomerCountPerDistrict);
}

int32_t Driver::FindItemId() {
    return rand_gen_->NURand(8191, 1, kItemCount); 
}

} // namespace tpcc
} // namespace tera
