// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: baorenyi@baidu.com

#ifndef TERA_BENCHMARK_TPCC_TPCCDB_H
#define TERA_BENCHMARK_TPCC_TPCCDB_H

#include <iostream>
#include <stdint.h>
#include <string>
#include <unordered_set>
#include <unordered_map>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "benchmark/tpcc/random_generator.h"
#include "benchmark/tpcc/tpcc_types.h"

namespace tera {
namespace tpcc {

typedef std::unordered_set<int> IdSet;
typedef std::unordered_map<std::string, std::string> ForeignKeyMap;
typedef std::unordered_map<std::string, std::string> RetTuples;

inline float GenTax(RandomGenerator* rand_gen) {
  return rand_gen->MakeFloat(kTaxMax, kTaxMin, kTaxDigits);
}

inline std::string GenZip(RandomGenerator* rand_gen) {
  return rand_gen->MakeNString(kZipLen, kZipLen);
}

inline std::string GenData(RandomGenerator* rand_gen, int lower_len, int upper_len,
                           bool is_original) {
  std::string ret = rand_gen->MakeAString(lower_len, upper_len);
  if (is_original) {
    int pos = rand_gen->GetRandom(0, ret.size() - 8);
    ret = ret.replace(pos, 8, "ORIGINAL");
  }
  return ret;
}

inline std::string GenLastName(RandomGenerator* rand_gen, int id) {
  if (id > 999) {
    id = rand_gen->NURand(255, 0, std::min(999, id - 1));
  }
  std::vector<std::string> labels = {"BAR", "OUGHT", "ABLE",  "PRI",   "PRES",
                                     "ESE", "ANTI",  "CALLY", "ATION", "EING"};
  return labels[id / 100] + labels[(id / 10) % 10] + labels[id % 10];
}

inline IdSet PickUniqueIdSet(RandomGenerator* rand_gen, size_t cnt, int lower_id, int upper_id) {
  IdSet ids;
  while (ids.size() < cnt) {
    int tmp_id = rand_gen->GetRandom(lower_id, upper_id);
    if (ids.find(tmp_id) == ids.end()) {
      ids.insert(tmp_id);
    }
  }
  return ids;
}

struct Item {
  int32_t i_id;
  int32_t i_im_id;
  float i_price;
  std::string i_name;
  std::string i_data;

  Item(int32_t id, bool is_original, RandomGenerator* rand_gen) : i_id(id) {
    i_im_id = rand_gen->GetRandom(kItemMinIm, kItemMaxIm);
    i_price = rand_gen->MakeFloat(kItemMinPrice, kItemMaxPrice, kItemPriceDigits);
    i_name = rand_gen->MakeAString(kItemMinNameLen, kItemMaxNameLen);
    i_data = GenData(rand_gen, kItemMinDataLen, kItemMaxDataLen, is_original);
  }

  std::string PrimaryKey() const { return std::to_string(i_id); }
  std::string ToString() const;
};

struct Warehouse {
  int32_t w_id;
  float w_tax;
  float w_ytd;
  std::string w_name;
  std::string w_street_1;
  std::string w_street_2;
  std::string w_city;
  std::string w_state;
  std::string w_zip;
  Warehouse(int32_t id, RandomGenerator* rand_gen) : w_id(id) {
    w_tax = GenTax(rand_gen);
    w_ytd = kInitYTD;
    w_name = rand_gen->MakeAString(kWareHouseNameLowerLen, kWareHouseNameUpperLen);
    w_street_1 = rand_gen->MakeAString(kStreetLowerLen, kStreetUpperLen);
    w_street_2 = rand_gen->MakeAString(kStreetLowerLen, kStreetUpperLen);
    w_city = rand_gen->MakeAString(kCityLowerLen, kCityUpperLen);
    w_state = rand_gen->MakeAString(kStateLen, kStateLen);
    w_zip = GenZip(rand_gen);
  }
  std::string PrimaryKey() const { return std::to_string(w_id); }
  std::string ToString() const;
};

struct District {
  int32_t d_id;
  int32_t d_w_id;
  float d_tax;
  float d_ytd;
  int32_t d_next_o_id;
  std::string d_name;
  std::string d_street_1;
  std::string d_street_2;
  std::string d_city;
  std::string d_state;
  std::string d_zip;

  District(int32_t id, int32_t w_id, RandomGenerator* rand_gen);
  std::string PrimaryKey() const;
  std::string ForeignKey() const;
  std::string ToString() const;
};

struct Stock {
  int32_t s_i_id;
  int32_t s_w_id;
  int32_t s_quantity;
  int32_t s_ytd;
  int32_t s_order_cnt;
  int32_t s_remote_cnt;
  std::vector<std::string> s_dist;
  std::string s_data;

  Stock(int32_t id, int32_t w_id, bool is_original, RandomGenerator* rand_gen);
  std::string PrimaryKey() const;
  std::string ForeignKey() const;
  std::string ToString() const;
};

struct Customer {
  int32_t c_id;
  int32_t c_d_id;
  int32_t c_w_id;
  float c_credit_lim;
  float c_discount;
  float c_balance;
  float c_ytd_payment;
  int32_t c_payment_cnt;
  int32_t c_delivery_cnt;
  std::string c_first;
  std::string c_middle;
  std::string c_last;
  std::string c_street_1;
  std::string c_street_2;
  std::string c_city;
  std::string c_state;
  std::string c_zip;
  std::string c_phone;
  std::string c_since;
  std::string c_credit;
  std::string c_data;
  Customer(int32_t id, int32_t d_id, int32_t w_id, const std::string& datetime, bool bad_credit,
           RandomGenerator* rand_gen);
  std::string PrimaryKey() const;
  std::string ForeignKey() const;
  std::string ToString() const;
};

struct Order {
  int32_t o_id;
  int32_t o_c_id;
  int32_t o_d_id;
  int32_t o_w_id;
  int32_t o_carrier_id;
  int32_t o_ol_cnt;

  // If the order includes only home order-lines,
  // then O_ALL_LOCAL is set to 1, otherwise O_ALL_LOCAL is set to 0.
  int32_t o_all_local;
  std::string o_entry_d;

  Order(int32_t id, int32_t c_id, int32_t d_id, int32_t w_id, bool new_order,
        const std::string& datetime, RandomGenerator* rand_gen);
  std::string PrimaryKey() const;
  std::string ForeignKey() const;
  std::string ToString() const;
};

// An order-line is said to be 'home' if it is supplied by the home warehouse
// (i.e., when OL_SUPPLY_W_ID equals O_W_ID).
//
// An order-line is said to be remote when it is supplied by a remote warehouse
// (i.e., when OL_SUPPLY_W_ID does not equal O_W_ID).
//
struct OrderLine {
  int32_t ol_o_id;
  int32_t ol_d_id;
  int32_t ol_w_id;
  int32_t ol_number;
  int32_t ol_i_id;
  int32_t ol_supply_w_id;
  int32_t ol_quantity;
  float ol_amount;
  std::string ol_delivery_d;
  std::string ol_dist_info;

  OrderLine(int32_t o_id, int32_t d_id, int32_t w_id, int32_t number, bool new_order,
            const std::string& datetime, RandomGenerator* rand_gen);
  std::string PrimaryKey() const;
  ForeignKeyMap ForeignKeys() const;
  std::string ToString() const;
};

struct NewOrder {
  int32_t no_o_id;
  int32_t no_d_id;
  int32_t no_w_id;

  NewOrder(int32_t o_id, int32_t d_id, int32_t w_id);
  std::string PrimaryKey() const;
  std::string ForeignKey() const;
  std::string ToString() const;
};

struct History {
  int32_t h_c_id;
  int32_t h_c_d_id;
  int32_t h_c_w_id;
  int32_t h_d_id;
  int32_t h_w_id;
  float h_amount;
  std::string h_date;
  std::string h_data;

  History(int32_t c_id, int32_t d_id, int32_t w_id, const std::string& datetime,
          RandomGenerator* rand_gen)
      : h_c_id(c_id),
        h_c_d_id(d_id),
        h_c_w_id(w_id),
        h_d_id(d_id),
        h_w_id(w_id),
        h_amount(kInitHistoryAmount),
        h_date(datetime) {
    h_data = rand_gen->MakeAString(kHistoryDataLowerLen, kHistoryDataUpperLen);
  }
  std::string PrimaryKey() const { return std::to_string(h_c_id); }
  std::string ToString() const;
};

struct NewOrderInfo {
  bool need_failed;
  int32_t o_all_local;
  int32_t o_ol_cnt;
  std::vector<int32_t> ol_supply_w_ids;
  std::vector<int32_t> ol_i_ids;
  std::vector<int32_t> ol_quantities;
};

enum TpccTables {
  kItemTable = 0,
  kWarehouseTable = 1,
  kDistrictTable = 2,
  kCustomerTable = 3,
  kHistoryTable = 4,
  kStockTable = 5,
  kOrderTable = 6,
  kOrderLineTable = 7,
  kNewOrderTable = 8,

  // the index of table
  kCustomerLastIndex = 9,
  kOrderIndex = 10,
  kHistoryIndex = 11
};

/// ------------------------- transaction result ---------------------------///

class TxnResult {
 public:
  void SetState(bool status);
  bool State() const;
  void SetReason(const std::string& reason);
  const std::string& Reason() const;

 private:
  bool status_;
  std::string reason_;
};

class StockLevelResult : public TxnResult {
 public:
  void SetLowStock(int low_stock);
  int LowStock() const;

 private:
  int low_stock_;
};

class PaymentResult : public TxnResult {
 public:
  void SetSingleLine(const RetTuples& single_line);

 private:
  RetTuples single_line_;
};

class NewOrderResult : public TxnResult {
 public:
  void AddLine(const RetTuples& line);
  void SetSingleLine(const RetTuples& single_line);

 private:
  std::vector<RetTuples> lines_;
  RetTuples single_line_;
};

class OrderStatusResult : public TxnResult {};

class DeliveryResult : public TxnResult {};

class TpccDb {
 public:
  TpccDb() {}
  virtual ~TpccDb() {}

  // init db
  virtual bool CreateTables() = 0;
  virtual bool CleanTables() = 0;

  // for insert table
  virtual bool InsertItem(const Item& i) = 0;

  virtual bool InsertWarehouse(const Warehouse& w) = 0;

  virtual bool InsertDistrict(const District& d) = 0;

  virtual bool InsertCustomer(const Customer& c) = 0;

  virtual bool InsertHistory(const History& h) = 0;

  virtual bool InsertStock(const Stock& s) = 0;

  virtual bool InsertOrder(const Order& o) = 0;

  virtual bool InsertOrderLine(const OrderLine& ol) = 0;

  virtual bool InsertNewOrder(const NewOrder& no) = 0;

  //  for transaction

  //  The Stock-Level Transaction [Revision 5.11 - Page 44]
  //
  //  (warehouse_id, district_id)
  //      is the primarykey of t_district
  //      Each terminal must use a unique value of (W_ID, D_ID) that is constant
  //      over the whole measurement, i.e., D_IDs cannot be re-used within a
  //      warehouse
  //
  //  threshold
  //      The threshold of minimum quantity in stock (threshold) is selected
  //      at random within [10 .. 20].
  //
  virtual void StockLevelTxn(int32_t warehouse_id, int32_t district_id, int32_t threshold,
                             StockLevelResult* ret) = 0;

  //  The Delivery Transaction [Revision 5.11 - Page 40]
  //
  //  warehouse_id
  //      For any given terminal, the home warehouse number (W_ID) is constant
  //      over the whole measurement interval
  //
  //  carrier_id
  //      The carrier number (O_CARRIER_ID) is randomly selected within [1 ..
  //      10].
  //
  //  delivery_datetime
  //      The delivery date (OL_DELIVERY_D) is generated within the
  //      SUT by using the current system date and time.
  //
  virtual void DeliveryTxn(int32_t warehouse_id, int32_t carrier_id,
                           const std::string& delivery_datetime, DeliveryResult* ret) = 0;

  //  The Order-Status Transaction [Revision 5.11 - Page 37]
  //
  //  warehouse_id
  //      For any given terminal, the home warehouse number (W_ID) is constant
  //      over the whole measurement interval
  //
  //  district_id
  //      The district number (D_ID) is randomly selected within [1 .. 10]
  //      from the home warehouse (D_W_ID = W_ID).
  //
  //  c_warehouse_id, c_district_id, last_name
  //      customer is randomly selected
  //      60% of the time by last name (C_W_ID, C_D_ID, C_LAST)
  //      from the selected district (C_D_ID = D_ID)
  //      and the home warehouse number (C_W_ID = W_ID).
  //
  //  c_warehouse_id, c_district_id, customer_id
  //      40% of the time by number (C_W_ID, C_D_ID, C_ID)
  //      from the selected district (C_D_ID = D_ID)
  //      and the home warehouse number (C_W_ID = W_ID).
  //
  virtual void OrderStatusTxn(bool by_last_name, int32_t warehouse_id, int32_t district_id,
                              int32_t c_customer_id, const std::string& last_name,
                              OrderStatusResult* ret) = 0;

  //  The Payment Transaction [Revision 5.11 - Page 33]
  //
  //  warehouse_id
  //      For any given terminal, the home warehouse number (W_ID) is constant
  //      over the whole measurement interval
  //
  //  district_id
  //      The district number (D_ID) is randomly selected within [1 .. 10]
  //      from the home warehouse (D_W_ID = W_ID).
  //
  //  c_warehouse_id, c_district_id, last_name
  //      The customer is randomly selected
  //      1) 60% of the time by last name (C_W_ID , C_D_ID, C_LAST)
  //  c_warehouse_id, c_district_id, customer_id
  //      The customer is randomly selected
  //      2) 40% of the time by number (C_W_ID , C_D_ID , C_ID).
  //
  //  h_amount
  //      The payment amount (H_AMOUNT) is randomly selected within
  //      [1.00 .. 5,000.00].
  //
  virtual void PaymentTxn(bool by_last_name, int32_t warehouse_id, int32_t district_id,
                          int32_t c_warehouse_id, int32_t c_district_id, int32_t c_customer_id,
                          const std::string& last_name, int32_t h_amount, PaymentResult* ret) = 0;

  //  The New-Order Transaction [Revision 5.11 - Page 28]
  //  warehouse_id
  //      For any given terminal, the home warehouse number (W_ID) is constant
  //      over the whole measurement interval
  //
  //  district_id
  //      The district number (D_ID) is randomly selected within [1 .. 10]
  //      from the home warehouse (D_W_ID = W_ID).
  //
  //  customer_id
  //      The non-uniform random customer number (C_ID) is selected using
  //      the NURand(1023,1,3000) function from the selected district
  //      number (C_D_ID = D_ID) and the home warehouse number (C_W_ID = W_ID).
  //
  virtual void NewOrderTxn(int32_t warehouse_id, int32_t district_id, int32_t customer_id,
                           const NewOrderInfo& info, NewOrderResult* ret) = 0;

  static TpccDb* NewTpccDb(const std::string& db_type);
};

}  // namespace tpcc
}  // namespace tera

#endif /* TERA_BENCHMARK_TPCC_TPCCDB_H */
