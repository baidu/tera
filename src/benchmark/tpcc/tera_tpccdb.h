// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: baorenyi@baidu.com

#ifndef TERA_BENCHMARK_TPCC_TERA_TPCCDB_H
#define TERA_BENCHMARK_TPCC_TERA_TPCCDB_H

#include "tera.h"
#include "benchmark/tpcc/tpccdb.h"

namespace tera {
namespace tpcc {

class TpccDb;
class TxnResult;

class TeraTpccDb : public TpccDb {
 public:
  TeraTpccDb();
  virtual ~TeraTpccDb();

  virtual bool CreateTables();
  virtual bool CleanTables();

  // init db
  virtual bool InsertItem(const Item& i);

  virtual bool InsertWarehouse(const Warehouse& w);

  virtual bool InsertDistrict(const District& d);

  virtual bool InsertCustomer(const Customer& c);

  virtual bool InsertHistory(const History& h);

  virtual bool InsertStock(const Stock& s);

  virtual bool InsertOrder(const Order& o);

  virtual bool InsertOrderLine(const OrderLine& ol);

  virtual bool InsertNewOrder(const NewOrder& no);

  virtual void StockLevelTxn(int32_t warehouse_id, int32_t district_id, int32_t threshold,
                             StockLevelResult* ret);

  virtual void DeliveryTxn(int32_t warehouse_id, int32_t carrier_id,
                           const std::string& delivery_datetime, DeliveryResult* ret);

  virtual void OrderStatusTxn(bool by_last_name, int32_t warehouse_id, int32_t district_id,
                              int32_t c_customer_id, const std::string& last_name,
                              OrderStatusResult* ret);

  virtual void PaymentTxn(bool by_last_name, int32_t warehouse_id, int32_t district_id,
                          int32_t c_warehouse_id, int32_t c_district_id, int32_t c_customer_id,
                          const std::string& last_name, int32_t h_amount, PaymentResult* ret);

  virtual void NewOrderTxn(int32_t warehouse_id, int32_t district_id, int32_t customer_id,
                           const NewOrderInfo& info, NewOrderResult* ret);

 private:
  void SetTxnResult(TxnResult* ret, Transaction* gtxn, bool state = true,
                    const std::string& msg = "");

  bool GetValues(TxnResult* ret, Transaction* gtxn, RowReader* reader,
                 std::initializer_list<std::string> qu_names_initlist, RetTuples* ret_tuples,
                 const std::string& if_error_msg);

  bool GetCustomer(TxnResult* ret, Transaction* gtxn, bool by_last_name,
                   const std::string& last_name, int32_t customer_id, int32_t warehouse_id,
                   int32_t district_id, std::string* customer_key, RetTuples* customer_ret);

 private:
  void SetPaymentSingleLineRet(const RetTuples& warehouse_ret, const RetTuples& district_ret,
                               const RetTuples& customer_ret, const RetTuples& other_ret,
                               RetTuples* payment_ret);

 private:
  Client* client_;
  std::unordered_map<std::string, Table*> table_map_;
};

}  // namespace tpcc
}  // namespace tera

#endif /* TERA_BENCHMARK_TPCC_TERA_TPCCDB_H */
