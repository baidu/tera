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

void TeraTpccDb::DeliveryTxn(int32_t warehouse_id, 
                             int32_t carrier_id, 
                             const std::string& delivery_datetime,
                             DeliveryResult* ret) {
    // open table
    Table* t_neworder = table_map_[kTpccTables[kNewOrderTable]];
    Table* t_order = table_map_[kTpccTables[kOrderTable]];
    Table* t_orderline = table_map_[kTpccTables[kOrderLineTable]];
    Table* t_customer = table_map_[kTpccTables[kCustomerTable]];
    // begin transaction
    Transaction* gtxn = client_->NewGlobalTransaction(); 
    for (int32_t district_id = 1; district_id <= kDistrictCountPerWarehouse; ++district_id) {
        // The row in the NEW-ORDER table with matching NO_W_ID (equals W_ID) 
        // and NO_D_ID (equals D_ID) and with the lowest NO_O_ID value is selected.
        ErrorCode error_code;
        std::string start_key = std::to_string(warehouse_id) + "_" + std::to_string(district_id) + "_";
        ScanDescriptor scan_desc(start_key);
        scan_desc.SetEnd(start_key + "~");
        scan_desc.AddColumnFamily("cf0");
        tera::ResultStream* scanner = t_neworder->Scan(scan_desc, &error_code);
        bool not_new_order = false;
        int32_t order_id = INT32_MAX;
        for (scanner->LookUp(start_key); !scanner->Done(); scanner->Next()) {
            std::string row_key = scanner->RowName();
            if (row_key.find(start_key) == std::string::npos) {
                not_new_order = true;
                break;
            }
            std::size_t found = row_key.find_last_of("_");
            int32_t found_order_id = std::stoi(row_key.substr(found + 1));
            if (order_id > found_order_id) {
                order_id = found_order_id;
            }
        }
        delete scanner;
        // If no matching row is found, then the delivery of an order 
        // for this district is skipped.
        if (not_new_order || order_id == INT32_MAX) {
            continue;
        }

        // The selected row in the NEW-ORDER table is deleted
        std::string no_primary_key = start_key + std::to_string(order_id);
        RowReader* no_reader = t_neworder->NewRowReader(no_primary_key);
        RetTuples no_ret;
        if (!GetValues(ret, gtxn, no_reader, 
                       {"no_o_id"},
                       &no_ret, 
                       "@delivery|no_reader|" + no_primary_key)) {
            return;
        }

        RowMutation* no_mu = t_neworder->NewRowMutation(no_primary_key);
        no_mu->DeleteColumns("cf0", "no_o_id", gtxn->GetStartTimestamp());
        no_mu->DeleteColumns("cf0", "no_d_id", gtxn->GetStartTimestamp());
        no_mu->DeleteColumns("cf0", "no_w_id", gtxn->GetStartTimestamp());
        gtxn->ApplyMutation(no_mu);
        delete no_mu;

        // The row in the ORDER table with matching 
        // O_W_ID (equals W_ID), O_D_ID (equals D_ID), and O_ID (equals NO_O_ID) 
        // is selected, O_C_ID, the customer number, is retrieved, 
        // and O_CARRIER_ID is updated.
        std::string order_primary_key = no_primary_key;
        RowReader* order_reader = t_order->NewRowReader(order_primary_key);
        RetTuples order_ret;
        if (!GetValues(ret, gtxn, order_reader, 
                       {"o_carrier_id", "o_ol_cnt", "o_c_id"},
                       &order_ret, 
                       "@delivery|order_reader|" + order_primary_key)) {
            return;
        }
        RowMutation* order_mu = t_order->NewRowMutation(order_primary_key);
        order_mu->Put("cf0", "o_carrier_id", std::to_string(carrier_id));
        gtxn->ApplyMutation(order_mu);
        delete order_mu;

        int32_t o_ol_cnt = std::stoi(order_ret["o_ol_cnt"]);
        // the sum of all OL_AMOUNT.
        float amount = 0.0f;
        // All rows in the ORDER-LINE table with matching 
        // OL_W_ID (= O_W_ID), OL_D_ID (= O_D_ID), and OL_O_ID (= O_ID) are selected. 
        for (int32_t ol_number = 1; ol_number <= o_ol_cnt; ++ ol_number) {
            std::string ol_key = order_primary_key + "_" + std::to_string(ol_number);
            RowReader* ol_reader = t_orderline->NewRowReader(ol_key);
            RetTuples ol_ret;
            if (!GetValues(ret, gtxn, ol_reader, 
                           {"ol_amount", "ol_delivery_d"},
                           &ol_ret, 
                           "@delivery|ol_reader|" + ol_key)) {
                return;
            }
            amount += std::stof(ol_ret["ol_amount"]);
            RowMutation* ol_mu = t_orderline->NewRowMutation(ol_key);
            // All OL_DELIVERY_D, the delivery dates, 
            // are updated to the current system time as returned by the OS 
            ol_mu->Put("cf0","ol_delivery_d",delivery_datetime);
            gtxn->ApplyMutation(ol_mu);
            delete ol_mu;
        }

        // The row in the CUSTOMER table with matching 
        // C_W_ID (= W_ID), C_D_ID (= D_ID), and C_ID (= O_C_ID) is selected 
        std::string customer_key = start_key + order_ret["o_c_id"];
        RowReader* customer_reader = t_customer->NewRowReader(customer_key);
        RetTuples customer_ret;
        if (!GetValues(ret, gtxn, customer_reader,
                       {"c_balance", "c_delivery_cnt"}, 
                       &customer_ret,
                       "@delivery|customer_reader" + customer_key)) {
            return;
        }
        // and C_BALANCE + sum(OL_AMOUNT) previously retrieved. C_DELIVERY_CNT + 1.
        RowMutation* customer_mu = t_customer->NewRowMutation(customer_key);
        customer_mu->Put("cf0", "c_balance", 
                std::to_string(std::stof(customer_ret["c_balance"]) + amount));
        customer_mu->Put("cf0", "c_delivery_cnt", 
                std::to_string(std::stoi(customer_ret["c_delivery_cnt"]) + 1));
        gtxn->ApplyMutation(customer_mu);
        delete customer_mu;
    }
    gtxn->Commit();
    SetTxnResult(ret, gtxn, gtxn->GetError().GetType() == ErrorCode::kOK);
}

} // namespace tpcc
} // namespace tera
