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

void TeraTpccDb::StockLevelTxn(int32_t warehouse_id, int32_t district_id, int32_t threshold,
                               StockLevelResult* ret) {
  // open table
  Table* t_district = table_map_[kTpccTables[kDistrictTable]];
  Table* t_order = table_map_[kTpccTables[kOrderTable]];
  Table* t_orderline = table_map_[kTpccTables[kOrderLineTable]];
  Table* t_stock = table_map_[kTpccTables[kStockTable]];
  // begin transaction
  std::unique_ptr<Transaction> gtxn(client_->NewGlobalTransaction());
  std::string district_primary_key =
      std::to_string(warehouse_id) + "_" + std::to_string(district_id);
  RowReader* district_reader = t_district->NewRowReader(district_primary_key);
  RetTuples district_ret;
  if (!GetValues(ret, gtxn.get(), district_reader, {"d_next_o_id"}, &district_ret,
                 "@stock_level|district_reader|" + district_primary_key)) {
    return;
  }
  int32_t order_id = std::stoi(district_ret["d_next_o_id"]);

  int32_t cnt = 0;
  for (int32_t ol_o_id = order_id - 20; ol_o_id <= order_id; ++ol_o_id) {
    std::string order_primary_key = std::to_string(warehouse_id) + "_" +
                                    std::to_string(district_id) + "_" + std::to_string(ol_o_id);
    RowReader* order_reader = t_order->NewRowReader(order_primary_key);
    RetTuples order_ret;
    if (!GetValues(ret, gtxn.get(), order_reader, {"o_ol_cnt"}, &order_ret,
                   "@stock_level|order_reader|" + order_primary_key)) {
      return;
    }
    int32_t o_ol_cnt = std::stoi(order_ret["o_ol_cnt"]);
    for (int32_t ol_number = 1; ol_number <= o_ol_cnt; ++ol_number) {
      std::string ol_primary_key = order_primary_key + "_" + std::to_string(ol_number);
      RowReader* ol_reader = t_orderline->NewRowReader(ol_primary_key);
      RetTuples ol_ret;
      ol_reader->AddColumn("cf0", "ol_i_id");
      if (!GetValues(ret, gtxn.get(), ol_reader, {"ol_i_id"}, &ol_ret,
                     "@stock_level|ol_reader|" + ol_primary_key)) {
        return;
      }
      int32_t ol_i_id = std::stoi(ol_ret["ol_i_id"]);
      std::string stock_key = std::to_string(warehouse_id) + "_" + std::to_string(ol_i_id);
      RowReader* stock_reader = t_stock->NewRowReader(stock_key);
      RetTuples stock_ret;
      if (!GetValues(ret, gtxn.get(), stock_reader, {"s_quantity"}, &stock_ret,
                     "@stock_level|stock_reader|" + stock_key)) {
        return;
      }
      int32_t s_quantity = std::stoi(stock_ret["s_quantity"]);
      if (s_quantity < threshold) {
        ++cnt;
      }
    }
  }
  // only read not need commit
  ret->SetLowStock(cnt);
  SetTxnResult(ret, gtxn.get());
}

}  // namespace tpcc
}  // namespace tera
