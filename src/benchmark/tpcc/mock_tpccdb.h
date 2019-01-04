// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: baorenyi@baidu.com

#ifndef TERA_BENCHMARK_TPCC_MOCK_TPCCDB_H
#define TERA_BENCHMARK_TPCC_MOCK_TPCCDB_H

#include "benchmark/tpcc/tpccdb.h"

namespace tera {
namespace tpcc {

class TpccDb;
class TxnResult;

class MockTpccDb : public TpccDb {
 public:
  MockTpccDb();
  virtual ~MockTpccDb() {}

  virtual bool CreateTables() { return true; }
  virtual bool CleanTables() { return true; }

  // init db
  virtual bool InsertItem(const Item& i) { return flag_; }

  virtual bool InsertWarehouse(const Warehouse& w) { return flag_; }

  virtual bool InsertDistrict(const District& d) { return flag_; }

  virtual bool InsertCustomer(const Customer& c) { return flag_; }

  virtual bool InsertHistory(const History& h) { return flag_; }

  virtual bool InsertStock(const Stock& s) { return flag_; }

  virtual bool InsertOrder(const Order& o) { return flag_; }

  virtual bool InsertOrderLine(const OrderLine& ol) { return flag_; }

  virtual bool InsertNewOrder(const NewOrder& no) { return flag_; }

  virtual void StockLevelTxn(int32_t warehouse_id, int32_t district_id, int32_t threshold,
                             StockLevelResult* ret) {}

  virtual void DeliveryTxn(int32_t warehouse_id, int32_t carrier_id,
                           const std::string& delivery_datetime, DeliveryResult* ret) {}

  virtual void OrderStatusTxn(bool by_last_name, int32_t warehouse_id, int32_t district_id,
                              int32_t c_customer_id, const std::string& last_name,
                              OrderStatusResult* ret) {}

  virtual void PaymentTxn(bool by_last_name, int32_t warehouse_id, int32_t district_id,
                          int32_t c_warehouse_id, int32_t c_district_id, int32_t c_customer_id,
                          const std::string& last_name, int32_t h_amount, PaymentResult* ret) {}

  virtual void NewOrderTxn(int32_t warehouse_id, int32_t district_id, int32_t customer_id,
                           const NewOrderInfo& info, NewOrderResult* ret) {}

 private:
  bool flag_;
};

}  // namespace tpcc
}  // namespace tera

#endif /* TERA_BENCHMARK_TPCC_MOCK_TPCCDB_H */
