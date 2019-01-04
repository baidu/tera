// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: baorenyi@baidu.com

#ifndef TERA_BENCHMARK_TPCC_TPCC_TYPES_H
#define TERA_BENCHMARK_TPCC_TPCC_TYPES_H

#include <string>

#include <gflags/gflags.h>
#include <glog/logging.h>

namespace tera {
namespace tpcc {

const int kTpccTableCnt = 12;

// t_customer_last_index is the index of t_customer
//
const char* const kTpccTables[] = {"t_item", "t_warehouse", "t_district", "t_customer", "t_history",
                                   "t_stock", "t_order", "t_orderline", "t_neworder",
                                   "t_customer_last_index", "t_order_index", "t_history_index"};

// StockLevel  4%   4
// OrderStatus 4%   8
// Delivery    4%  12
// Payment    43%  55
// NewOrder   45% 100
const int kTpccTransactionRatios[] = {4, 8, 12, 55, 100};

// http://www.man7.org/linux/man-pages/man3/initstate.3.html
// Current "optimal" values for the size of the state array n
// are 8, 32, 64, 128, and 256 bytes;
const int kRandomStateSize = 64;

// YTD
const float kInitYTD = 300000.00f;

// tax
const float kTaxMax = 0.20f;
const float kTaxMin = 0.10f;
const int kTaxDigits = 2;

// address
const int kStreetLowerLen = 10;
const int kStreetUpperLen = 20;
const int kCityLowerLen = 10;
const int kCityUpperLen = 20;
const int kStateLen = 2;
const int kZipLen = 9;

// warehourse
const int kMaxWarehouseId = 100;
const int kWareHouseNameLowerLen = 6;
const int kWareHouseNameUpperLen = 10;

// stock
const int kMaxQuantity = 100;
const int kMinQuantity = 10;
const int kDistLen = 24;
const int kStockDataLowerLen = 26;
const int kStockDataUpperLen = 50;
const int kMinStockLevelThreshold = 10;
const int kMaxStockLevelThreshold = 20;

// item
const int kItemCount = 100000;
const int kItemMaxIm = 10000;
const int kItemMinIm = 1;
const float kItemMaxPrice = 100.00;
const float kItemMinPrice = 1.00;
const int kItemPriceDigits = 2;
const int kItemMaxNameLen = 24;
const int kItemMinNameLen = 14;
const int kItemMaxDataLen = 50;
const int kItemMinDataLen = 26;

// district
const int kDistrictCountPerWarehouse = 10;
const int kDistrictNameLowerLen = 6;
const int kDistrictNameUpperLen = 10;

// customer
const int kCustomerCountPerDistrict = 3000;
const float kInitCreditLimit = 5000.00;
const float kMaxDisCount = 0.0;
const float kMinDisCount = 0.5;
const int kDisCountDigits = 2;
const float kInitBalance = -10.00;
const float kInitYTDPayment = 10.00;
const int kInitPaymentCnt = 1;
const int kInitDeliveryCnt = 0;
const int kFirstLowerLen = 6;
const int kFirstUpperLen = 10;
const int kMiddleLen = 2;
const int kLastLen = 16;
const int kPhoneLen = 16;
const int kCreditLen = 2;
const int kCustomerDataUpperLen = 500;
const int kCustomerDataLowerLen = 300;

// order
const int kInitOrdersPerDistrict = 3000;
const int kInitAllLocal = 1;
const int kMaxCarrierId = 10;
const int kMinCarrierId = 1;
const int kMaxOrderLineCnt = 15;
const int kMinOrderLineCnt = 5;

// new order
const int kInitNewOrderCountPerDistrict = 900;

// order line
const int kMaxItemId = 100000;
const int kMinItemId = 1;
const int kInitQuantity = 5;
const int kMaxOrderLineQuantity = 10;
const float kOrderLineMinAmount = 0.01f;
const float kOrderLineMaxAmount = 9999.99f;
const int kOrderLineAmountDigits = 2;

// history
const float kInitHistoryAmount = 10.00f;
const int kHistoryDataLowerLen = 12;
const int kHistoryDataUpperLen = 24;

// runtime h_amount
const float kRuntimeMaxAmount = 5000.00f;
const float kRuntimeMinAmount = 1.00f;
const int kRuntimeAmountDigits = 2;

}  // namespace tpcc
}  // namepsace tera

#endif /* TERA_BENCHMARK_TPCC_TPCC_TYPES_H */
