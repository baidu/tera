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

void TeraTpccDb::PaymentTxn(bool by_last_name, int32_t warehouse_id, int32_t district_id,
                            int32_t customer_warehouse_id, int32_t customer_district_id,
                            int32_t c_customer_id, const std::string& last_name, int32_t h_amount,
                            PaymentResult* ret) {
  // open table
  Table* t_warehouse = table_map_[kTpccTables[kWarehouseTable]];
  Table* t_district = table_map_[kTpccTables[kDistrictTable]];
  Table* t_customer = table_map_[kTpccTables[kCustomerTable]];
  Table* t_history = table_map_[kTpccTables[kHistoryTable]];
  Table* t_history_index = table_map_[kTpccTables[kHistoryIndex]];

  // begin transaction
  Transaction* gtxn = client_->NewGlobalTransaction();

  // read customer
  std::string customer_key = "";
  RetTuples customer_ret;
  if (!GetCustomer(ret, gtxn, by_last_name, last_name, c_customer_id, customer_warehouse_id,
                   customer_district_id, &customer_key, &customer_ret)) {
    return;
  }

  // read warehouse
  std::string warehouse_key = std::to_string(warehouse_id);
  RowReader* warehouse_reader = t_warehouse->NewRowReader(warehouse_key);
  RetTuples warehouse_ret;
  if (!GetValues(ret, gtxn, warehouse_reader,
                 {"w_ytd", "w_name", "w_street_1", "w_street_2", "w_city", "w_state", "w_zip"},
                 &warehouse_ret, "@payment|warehouse_reader|" + warehouse_key)) {
    return;
  }

  // update warehouse
  RowMutation* warehouse_mu = t_warehouse->NewRowMutation(warehouse_key);
  // add amount of this payment to the ytd balance of current warehouse.
  float w_ytd = std::stof(warehouse_ret["w_ytd"]) + h_amount;
  warehouse_mu->Put("cf0", "w_ytd", std::to_string(w_ytd));
  gtxn->ApplyMutation(warehouse_mu);
  delete warehouse_mu;

  // read district
  std::string district_id_str = std::to_string(district_id);
  std::string district_key = warehouse_key + "_" + district_id_str;
  RowReader* district_reader = t_district->NewRowReader(district_key);
  RetTuples district_ret;
  if (!GetValues(ret, gtxn, district_reader,
                 {"d_ytd", "d_name", "d_street_1", "d_street_2", "d_city", "d_state", "d_zip"},
                 &district_ret, "@payment|district_reader|" + district_key)) {
    return;
  }

  // update district
  RowMutation* district_mu = t_district->NewRowMutation(district_key);
  // add amount of this payment to the ytd balance of current district.
  float d_ytd = std::stof(district_ret["d_ytd"]) + h_amount;
  district_mu->Put("cf0", "d_ytd", std::to_string(d_ytd));
  gtxn->ApplyMutation(district_mu);
  delete district_mu;

  // update customer
  // [Revision 5.11 - Page 34] see Clause 2.5.2.2
  // C_BALANCE is decreased by H_AMOUNT.
  // C_YTD_PAYMENT is increased by H_AMOUNT.
  // C_PAYMENT_CNT is incremented by 1.
  RowMutation* customer_mu = t_customer->NewRowMutation(customer_key);
  std::string c_balance_str = std::to_string(std::stof(customer_ret["c_balance"]) - h_amount);
  customer_mu->Put("cf0", "c_balance", c_balance_str);
  customer_mu->Put("cf0", "c_ytd_payment",
                   std::to_string(std::stof(customer_ret["c_ytd_payment"]) + h_amount));
  customer_mu->Put("cf0", "c_payment_cnt",
                   std::to_string(std::stof(customer_ret["c_payment_cnt"]) + h_amount));

  if (customer_ret["c_credit"] == "BC") {
    std::string data_info = customer_key + "_" + district_key + "_" + std::to_string(h_amount);
    customer_ret["c_data"].insert(0, data_info);
    if (customer_ret["c_data"].size() > kCustomerDataUpperLen) {
      customer_ret["c_data"].substr(0, kCustomerDataUpperLen);
    }
    customer_mu->Put("cf0", "c_data", customer_ret["c_data"]);
  }
  gtxn->ApplyMutation(customer_mu);
  delete customer_mu;

  // read history_index (find newest history)
  std::string history_data = warehouse_ret["w_name"] + "    " + district_ret["d_name"];
  RowReader* hindex_reader = t_history_index->NewRowReader("count");
  RetTuples hindex_ret;
  if (!GetValues(ret, gtxn, hindex_reader, {"count"}, &hindex_ret,
                 "@payment|hindex_reader|count")) {
    return;
  }
  int cnt = std::stoi(hindex_ret["count"]);

  // update history_index
  RowMutation* hindex_mu = t_history_index->NewRowMutation("count");
  hindex_mu->Put("cf0", "count", std::to_string(++cnt));
  gtxn->ApplyMutation(hindex_mu);
  delete hindex_mu;

  // update history use now newest count as the primary key(row_key) of history
  // default t_history don't have priamry key in tpcc
  std::string history_key = std::to_string(cnt);
  RowMutation* mu = t_history->NewRowMutation(history_key);
  mu->Put("cf0", "h_c_id", customer_ret["c_id"]);
  mu->Put("cf0", "h_c_d_id", customer_ret["c_d_id"]);
  mu->Put("cf0", "h_c_w_id", customer_ret["c_w_id"]);
  mu->Put("cf0", "h_d_id", district_id_str);
  mu->Put("cf0", "h_w_id", warehouse_key);
  mu->Put("cf0", "h_amount", std::to_string(h_amount));
  // The payment date (H_DATE) in generated within the SUT
  // by using the current system date and time
  std::string datetime = get_curtime_str();
  mu->Put("cf0", "h_date", datetime);
  mu->Put("cf0", "h_data", history_data);
  gtxn->ApplyMutation(mu);
  delete mu;

  gtxn->Commit();
  RetTuples single_line;
  RetTuples other_ret = {{"w_id", warehouse_key},
                         {"d_id", district_id_str},
                         {"h_amount", std::to_string(h_amount)},
                         {"h_date", datetime},
                         {"c_balance", c_balance_str},
                         {"c_data", customer_ret["c_data"].substr(0, 200)}};
  SetPaymentSingleLineRet(warehouse_ret, district_ret, customer_ret, other_ret, &single_line);

  SetTxnResult(ret, gtxn);
}

void TeraTpccDb::SetPaymentSingleLineRet(const RetTuples& warehouse_ret,
                                         const RetTuples& district_ret,
                                         const RetTuples& customer_ret, const RetTuples& other_ret,
                                         RetTuples* payment_ret) {
  // The following fields are displayed:
  // W_ID, D_ID, C_ID, C_D_ID, C_W_ID,
  // W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP,
  // D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP,
  // C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE,
  // C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE,
  // the first 200 characters of C_DATA (only if C_CREDIT = "BC"),
  // H_AMOUNT, and H_DATE.
  payment_ret->insert(other_ret.begin(), other_ret.end());
  for (auto t : warehouse_ret) {
    if (t.first != "w_ytd" && t.first != "w_name") {
      payment_ret->insert(t);
    }
  }
  for (auto t : district_ret) {
    if (t.first != "d_ytd" && t.first != "w_name") {
      payment_ret->insert(t);
    }
  }
  std::unordered_set<std::string> c_names = {"c_id",     "c_d_id",   "c_w_id",       "c_first",
                                             "c_middle", "c_last",   "c_street_1",   "c_street_2",
                                             "c_city",   "c_state",  "c_zip",        "c_phone",
                                             "c_since",  "c_credit", "c_credit_lim", "c_discount"};
  for (auto t : customer_ret) {
    if (c_names.find(t.first) != c_names.end()) {
      payment_ret->insert(t);
    }
  }
}

}  // namespace tpcc
}  // namespace tera
