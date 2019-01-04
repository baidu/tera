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

void TeraTpccDb::NewOrderTxn(int32_t warehouse_id, int32_t district_id, int32_t customer_id,
                             const NewOrderInfo& info, NewOrderResult* ret) {
  // open table
  Table* t_warehouse = table_map_[kTpccTables[kWarehouseTable]];
  Table* t_district = table_map_[kTpccTables[kDistrictTable]];
  Table* t_customer = table_map_[kTpccTables[kCustomerTable]];
  Table* t_order = table_map_[kTpccTables[kOrderTable]];
  Table* t_order_index = table_map_[kTpccTables[kOrderIndex]];
  Table* t_neworder = table_map_[kTpccTables[kNewOrderTable]];
  Table* t_orderline = table_map_[kTpccTables[kOrderLineTable]];
  Table* t_item = table_map_[kTpccTables[kItemTable]];
  Table* t_stock = table_map_[kTpccTables[kStockTable]];
  // begin transaction
  std::unique_ptr<Transaction> gtxn(client_->NewGlobalTransaction());
  std::string datetime = get_curtime_str();
  std::string warehouse_key = std::to_string(warehouse_id);
  std::string district_key = warehouse_key + "_" + std::to_string(district_id);
  std::string customer_key = district_key + "_" + std::to_string(customer_id);

  RowReader* warehouse_reader = t_warehouse->NewRowReader(warehouse_key);
  RetTuples warehouse_ret;
  if (!GetValues(ret, gtxn.get(), warehouse_reader, {"w_tax"}, &warehouse_ret,
                 "@new_order|warehouse_reader|" + warehouse_key)) {
    return;
  }

  RowReader* district_reader = t_district->NewRowReader(district_key);
  RetTuples district_ret;
  if (!GetValues(ret, gtxn.get(), district_reader, {"d_next_o_id", "d_tax"}, &district_ret,
                 "@new_order|district_reader|" + district_key)) {
    return;
  }
  std::string d_next_o_id_str = std::to_string(std::stoi(district_ret["d_next_o_id"]) + 1);

  RowReader* customer_reader = t_customer->NewRowReader(customer_key);
  RetTuples customer_ret;
  if (!GetValues(ret, gtxn.get(), customer_reader, {"c_discount", "c_credit", "c_last"},
                 &customer_ret, "@new_order|customer_reader|" + customer_key)) {
    return;
  }

  RowMutation* district_mu = t_district->NewRowMutation(district_key);
  district_mu->Put("cf0", "d_next_o_id", d_next_o_id_str);
  gtxn->ApplyMutation(district_mu);
  delete district_mu;

  std::string order_key = district_key + "_" + d_next_o_id_str;
  RowMutation* order_mu = t_order->NewRowMutation(order_key);
  std::string order_index_key = customer_key + "_" + d_next_o_id_str;
  RowMutation* order_index_mu = t_order_index->NewRowMutation(order_index_key);
  order_index_mu->Put("cf0", "o_id", d_next_o_id_str);
  order_index_mu->Put("cf0", "o_c_id", std::to_string(customer_id));
  order_index_mu->Put("cf0", "o_d_id", std::to_string(district_id));
  order_index_mu->Put("cf0", "o_w_id", warehouse_key);
  order_mu->Put("cf0", "o_id", d_next_o_id_str);
  order_mu->Put("cf0", "o_c_id", std::to_string(customer_id));
  order_mu->Put("cf0", "o_d_id", std::to_string(district_id));
  order_mu->Put("cf0", "o_w_id", warehouse_key);
  order_mu->Put("cf0", "o_carrier_id", std::to_string(0));
  order_mu->Put("cf0", "o_ol_cnt", std::to_string(info.o_ol_cnt));
  order_mu->Put("cf0", "o_all_local", std::to_string(info.o_all_local));
  order_mu->Put("cf0", "o_entry_d", datetime);
  gtxn->ApplyMutation(order_mu);
  gtxn->ApplyMutation(order_index_mu);
  delete order_mu;
  delete order_index_mu;

  RowMutation* no_mu = t_neworder->NewRowMutation(order_key);
  no_mu->Put("cf0", "no_o_id", d_next_o_id_str);
  no_mu->Put("cf0", "no_d_id", std::to_string(district_id));
  no_mu->Put("cf0", "no_w_id", warehouse_key);
  gtxn->ApplyMutation(no_mu);
  delete no_mu;

  std::string ol_dist_info_key;
  if (district_id == kDistrictCountPerWarehouse) {
    ol_dist_info_key = "s_dist_10";
  } else {
    ol_dist_info_key = "s_dist_0" + std::to_string(district_id);
  }

  float ol_amount_sum = 0;
  for (int32_t i = 0; i < info.o_ol_cnt; ++i) {
    int32_t i_id = info.ol_i_ids[i];
    std::string item_key = std::to_string(i_id);
    RowReader* item_reader = t_item->NewRowReader(item_key);
    RetTuples item_ret;
    if (!GetValues(ret, gtxn.get(), item_reader, {"i_price", "i_name", "i_data"}, &item_ret,
                   "@new_order|item_reader|" + item_key)) {
      return;
    }

    std::string ol_supply_w_id_str = std::to_string(info.ol_supply_w_ids[i]);
    std::string stock_key = ol_supply_w_id_str + "_" + item_key;
    RowReader* stock_reader = t_item->NewRowReader(stock_key);
    RetTuples stock_ret;
    if (!GetValues(ret, gtxn.get(), stock_reader, {"s_quantity", "s_ytd", "s_order_cnt",
                                                   "s_remote_cnt", "s_data", ol_dist_info_key},
                   &stock_ret, "@new_order|stock_reader|" + stock_key)) {
      return;
    }

    int32_t ol_quantity = info.ol_quantities[i];
    float ol_amount = std::stof(item_ret["i_price"]) * ol_quantity;
    ol_amount_sum += ol_amount;
    std::string ol_number_str = std::to_string(i + 1);
    std::string ol_key = order_key + "_" + ol_number_str;
    RowMutation* ol_mu = t_orderline->NewRowMutation(ol_key);
    ol_mu->Put("cf0", "ol_o_id", d_next_o_id_str);
    ol_mu->Put("cf0", "ol_d_id", std::to_string(district_id));
    ol_mu->Put("cf0", "ol_w_id", warehouse_key);
    ol_mu->Put("cf0", "ol_number", ol_number_str);
    ol_mu->Put("cf0", "ol_i_id", item_key);
    ol_mu->Put("cf0", "ol_supply_w_id", ol_supply_w_id_str);
    ol_mu->Put("cf0", "ol_delivery_d", "");
    ol_mu->Put("cf0", "ol_quantity", std::to_string(ol_quantity));
    ol_mu->Put("cf0", "ol_amount", std::to_string(ol_amount));
    ol_mu->Put("cf0", "ol_dist_info", stock_ret[ol_dist_info_key]);
    gtxn->ApplyMutation(ol_mu);
    delete ol_mu;
    // update stock
    int32_t s_quantity = std::stoi(stock_ret["s_quantity"]);
    if (s_quantity > ol_quantity + 10) {
      s_quantity -= ol_quantity;
    } else {
      s_quantity = (s_quantity - ol_quantity) + 91;
    }
    float s_ytd = std::stof(stock_ret["s_quantity"]) + ol_quantity;
    int32_t s_order_cnt = std::stoi(stock_ret["s_order_cnt"]) + 1;
    int32_t s_remote_cnt = std::stoi(stock_ret["s_remote_cnt"]);
    if (info.ol_supply_w_ids[i] != warehouse_id) {
      ++s_remote_cnt;
    }
    RowMutation* stock_mu = t_stock->NewRowMutation(stock_key);
    stock_mu->Put("cf0", "s_quantity", std::to_string(s_quantity));
    stock_mu->Put("cf0", "s_ytd", std::to_string(s_ytd));
    stock_mu->Put("cf0", "s_order_cnt", std::to_string(s_order_cnt));
    stock_mu->Put("cf0", "s_remote_cnt", std::to_string(s_remote_cnt));
    gtxn->ApplyMutation(stock_mu);
    delete stock_mu;

    // set result
    RetTuples line;
    line["ol_supply_w_id"] = ol_supply_w_id_str;
    line["ol_i_id"] = item_key;
    line["i_name"] = item_ret["i_name"];
    line["ol_quantity"] = std::to_string(ol_quantity);
    line["s_quantity"] = std::to_string(s_quantity);
    line["i_price"] = item_ret["i_price"];
    line["ol_amount"] = std::to_string(ol_amount);
    std::string i_data = item_ret["i_data"];
    std::string s_data = item_ret["s_data"];
    if (i_data.find("ORIGINAL") != std::string::npos &&
        s_data.find("ORIGINAL") != std::string::npos) {
      line["brand_generic"] = "B";
    } else {
      line["brand_generic"] = "G";
    }
    ret->AddLine(line);
  }
  if (!info.need_failed) {
    RetTuples single_line;
    single_line["o_id"] = d_next_o_id_str;
    single_line["o_ol_cnt"] = std::to_string(info.o_ol_cnt);
    single_line["c_last"] = customer_ret["c_last"];
    single_line["c_credit"] = customer_ret["c_credit"];
    single_line["c_discount"] = customer_ret["c_discount"];
    single_line["w_tax"] = warehouse_ret["w_tax"];
    single_line["d_tax"] = district_ret["d_tax"];
    single_line["o_entry_d"] = datetime;
    float c_discount = std::stof(customer_ret["c_discount"]);
    float w_tax = std::stof(warehouse_ret["w_tax"]);
    float d_tax = std::stof(district_ret["d_tax"]);
    float total_amount = ol_amount_sum * (1 - c_discount) * (1 + w_tax + d_tax);
    single_line["total_amount"] = std::to_string(total_amount);
    ret->SetSingleLine(single_line);
    gtxn->Commit();
    SetTxnResult(ret, gtxn.get());
  } else {
    // set commit failed
    SetTxnResult(ret, gtxn.get(), false, "@new_order|rowback simulation");
  }
}

}  // namespace tpcc
}  // namespace tera
