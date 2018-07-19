// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: baorenyi@baidu.com

#include "benchmark/tpcc/tera_tpccdb.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "sdk/client_impl.h"
#include "sdk/sdk_utils.h"

namespace tera {
namespace tpcc {

void TeraTpccDb::OrderStatusTxn(bool by_last_name,
                                int32_t warehouse_id, int32_t district_id, 
                                int32_t c_customer_id, 
                                const std::string& last_name,
                                OrderStatusResult* ret) {
    // open table
    Table* t_order_index = table_map_[kTpccTables[kOrderIndex]];
    Table* t_orderline = table_map_[kTpccTables[kOrderLineTable]];
    Table* t_order = table_map_[kTpccTables[kOrderTable]];
    // begin transaction
    std::unique_ptr<Transaction> gtxn(client_->NewGlobalTransaction()); 
    std::string customer_key = "";
    RetTuples customer_ret;
    if (!GetCustomer(ret, gtxn.get(), by_last_name, last_name, c_customer_id, 
                warehouse_id, district_id, &customer_key, &customer_ret)) {
        return;
    }

    // find newest order from order index
    ErrorCode error_code;
    std::string prefix_key = std::to_string(warehouse_id) + "_" 
        + std::to_string(district_id) + "_";
    std::string start_key = prefix_key + customer_ret["c_id"] + "_";
    ScanDescriptor scan_desc(start_key);
    scan_desc.SetEnd(start_key + "~");
    scan_desc.AddColumnFamily("cf0");
    ResultStream* scanner = t_order_index->Scan(scan_desc, &error_code);
    int32_t max_order_id = -1;
    for (scanner->LookUp(start_key); !scanner->Done(); scanner->Next()) {
        std::string row_key = scanner->RowName();
        RowReader* index_reader = t_order_index->NewRowReader(row_key);
        RetTuples index_ret;
        if (!GetValues(ret, gtxn.get(), index_reader, 
                       {"o_id"},
                       &index_ret,
                       "@order_status|order_index_reader|" + row_key)) {
            break;
        }
        if ( max_order_id < std::stoi(index_ret["o_id"])) {
            max_order_id = std::stoi(index_ret["o_id"]);
        }
    }
    delete scanner;
    if (max_order_id == -1) {
        SetTxnResult(ret, gtxn.get(), false, "not found order|" + start_key);
        return;
    }
    std::string order_key = prefix_key + std::to_string(max_order_id);
    RowReader* order_reader = t_order->NewRowReader(order_key);
    RetTuples order_ret;
    if (!GetValues(ret, gtxn.get(), order_reader, 
                {"o_ol_cnt", "o_id"},
                &order_ret,
                "@order_status|order_reader|" + order_key)) {
        return;
    }
    for (int32_t i = 1; i <= std::stoi(order_ret["o_ol_cnt"]); ++i) {
        std::string ol_key = prefix_key + order_ret["o_id"] + "_" + std::to_string(i);
        RowReader* ol_reader = t_orderline->NewRowReader(ol_key);
        RetTuples ol_ret;
        if (!GetValues(ret, gtxn.get(), ol_reader, 
                    {}, // TODO
                    &ol_ret,
                    "@order_status|ol_reader|" + ol_key)) {
            return;
        }
    }
    SetTxnResult(ret, gtxn.get());
}

} // namespace tpcc
} // namespace tera
