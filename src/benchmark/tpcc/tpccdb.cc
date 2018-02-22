// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: baorenyi@baidu.com

#include <string>

#include "benchmark/tpcc/mock_tpccdb.h"
#include "benchmark/tpcc/tera_tpccdb.h"
#include "benchmark/tpcc/tpccdb.h"

namespace tera {
namespace tpcc {

class TeraTpccDb;
class MockTpccDb;

/// ------------------------- [begin item table] -------------------------- ///
std::string Item::ToString() const {
    std::stringstream ss;
    ss << "i_id = " << i_id
       << ",i_im_id = " << i_im_id
       << ",i_price = " << i_price
       << ",i_name = " << i_name
       << ",i_data = " << i_data;
    return ss.str();
}

/// ------------------------- [begin warehouse table] --------------------- ///
std::string Warehouse::ToString() const {
    std::stringstream ss;
    ss << "w_id = " << w_id
       << ",w_tax = " << w_tax
       << ",w_ytd = " << w_ytd
       << ",w_name = " << w_name
       << ",w_street_1 = " << w_street_1
       << ",w_street_2 = " << w_street_2
       << ",w_city = " << w_city
       << ",w_state = " << w_state
       << ",w_zip = " << w_zip;
    return ss.str(); 
}

/// ------------------------- [begin district table] ---------------------- ///

District::District(int32_t id, int32_t w_id, RandomGenerator* rand_gen) 
    : d_id(id), d_w_id(w_id), d_ytd(kInitYTD), d_next_o_id(kCustomerCountPerDistrict + 1) {
    d_tax = GenTax(rand_gen);
    d_name = rand_gen->MakeAString(kDistrictNameLowerLen, kDistrictNameUpperLen);
    d_street_1 = rand_gen->MakeAString(kStreetLowerLen, kStreetUpperLen);
    d_street_2 = rand_gen->MakeAString(kStreetLowerLen, kStreetUpperLen); 
    d_city = rand_gen->MakeAString(kCityLowerLen, kCityUpperLen);
    d_state = rand_gen->MakeAString(kStateLen,kStateLen);
    d_zip = GenZip(rand_gen);
}

std::string District::PrimaryKey() const { 
    return std::to_string(d_w_id) + "_" 
           + std::to_string(d_id);
}

std::string District::ForeignKey() const {
    return std::to_string(d_w_id);
}

std::string District::ToString() const {
    std::stringstream ss;
    ss << "d_id  = " << d_id
       << ",d_w_id = " << d_w_id
       << ",d_tax = " << d_tax
       << ",d_ytd = " << d_ytd
       << ",d_next_o_id = " << d_next_o_id
       << ",d_name = " << d_name
       << ",d_street_1 = " << d_street_1
       << ",d_street_2 = " << d_street_2
       << ",d_city = " << d_city
       << ",d_state = " << d_state
       << ",d_zip = " << d_zip;
    return ss.str(); 
}

/// ------------------------- [begin stock table] ------------------------- ///

Stock::Stock(int32_t id, int32_t w_id, bool is_original, RandomGenerator* rand_gen) 
    : s_i_id (id), s_w_id(w_id) {
    s_quantity = rand_gen->GetRandom(kMinQuantity, kMaxQuantity);
    s_ytd = 0;
    s_order_cnt = 0;
    s_remote_cnt = 0;
    for (int i = 0; i < kDistrictCountPerWarehouse; ++i) {
        s_dist.push_back(rand_gen->MakeAString(kDistLen, kDistLen));
    }
    s_data = GenData(rand_gen, kStockDataLowerLen, kStockDataUpperLen, is_original);
}

std::string Stock::PrimaryKey() const { 
    return std::to_string(s_w_id) + "_" + std::to_string(s_i_id); 
}

std::string Stock::ForeignKey() const { 
    return std::to_string(s_i_id); 
}

std::string Stock::ToString() const {
    std::stringstream ss;
    ss << "s_w_id = " << s_w_id
       << ",s_quantity = " << s_quantity
       << ",s_ytd = " << s_ytd
       << ",s_order_cnt = " << s_order_cnt
       << ",s_remote_cnt = " << s_remote_cnt
       << ",s_data = " << s_data
       << ",s_dist = [";
    for (auto d : s_dist) {
        ss << d << ",";
    }
    ss << "]";
    return ss.str(); 
}

/// ------------------------- [begin order table] ------------------------- ///

Order::Order(int32_t id, int32_t c_id, int32_t d_id, int32_t w_id,
             bool new_order, const std::string& datetime, 
             RandomGenerator* rand_gen) 
    : o_id(id), o_c_id(c_id), o_d_id(d_id), o_w_id(w_id), 
      o_carrier_id(0), o_all_local(kInitAllLocal), 
      o_entry_d(datetime) {

    if (!new_order) {
        o_carrier_id = rand_gen->GetRandom(kMinCarrierId, kMaxCarrierId);
    }
    o_ol_cnt = rand_gen->GetRandom(kMinOrderLineCnt, kMaxOrderLineCnt);
}

std::string Order::PrimaryKey() const { 
    return std::to_string(o_w_id) + "_" 
           + std::to_string(o_d_id) + "_" 
           + std::to_string(o_id);
}

std::string Order::ForeignKey() const {
    return std::to_string(o_w_id) + "_"
           + std::to_string(o_d_id) + "_"
           + std::to_string(o_c_id);
}

std::string Order::ToString() const {
    std::stringstream ss;
    ss << "o_id = " << o_id
       << ",o_c_id = " << o_c_id
       << ",o_d_id = " << o_d_id
       << ",o_w_id = " << o_w_id
       << ",o_carrier_id = " << o_carrier_id
       << ",o_ol_cnt = " << o_ol_cnt
       << ",o_all_local = " << o_all_local
       << ",o_entry_d = " << o_entry_d;
    return ss.str(); 
}

/// ------------------------- [begin neworder table] ---------------------- ///


NewOrder::NewOrder(int32_t o_id, int32_t d_id, int32_t w_id) 
    : no_o_id(o_id), no_d_id(d_id), no_w_id(w_id) {
}

std::string NewOrder::ToString() const {
    std::stringstream ss;
    ss << "no_o_id = " << no_o_id
       << ",no_d_id = " << no_d_id
       << ",no_w_id = " << no_w_id;
    return ss.str(); 
}

std::string NewOrder::PrimaryKey() const {
    return std::to_string(no_w_id) 
        + "_" + std::to_string(no_d_id) 
        + "_" + std::to_string(no_o_id);
}

std::string NewOrder::ForeignKey() const {
    return std::to_string(no_w_id) 
        + "_" + std::to_string(no_d_id) 
        + "_" + std::to_string(no_o_id);
}

/// ------------------------- [begin orderline table] --------------------- ///

OrderLine::OrderLine(int32_t o_id, int32_t d_id, int32_t w_id, int32_t number, 
                     bool new_order, const std::string& datetime, 
                     RandomGenerator* rand_gen) 
    : ol_o_id(o_id), ol_d_id(d_id), ol_w_id(w_id), ol_number(number),
      ol_supply_w_id(w_id), ol_quantity(kInitQuantity),
      ol_amount(0.00f), ol_delivery_d(datetime) {

    ol_i_id = rand_gen->GetRandom(kMinItemId, kMaxItemId);
    if (new_order) {
        ol_amount = rand_gen->MakeFloat(kOrderLineMinAmount, 
                                        kOrderLineMaxAmount, 
                                        kOrderLineAmountDigits);
        ol_delivery_d = "";
    }
    ol_dist_info = rand_gen->MakeAString(kDistLen, kDistLen);
}

std::string OrderLine::PrimaryKey() const {
    return std::to_string(ol_w_id) + "_"
           + std::to_string(ol_d_id) + "_"
           + std::to_string(ol_o_id) + "_"
           + std::to_string(ol_number);
}

ForeignKeyMap OrderLine::ForeignKeys() const {
    ForeignKeyMap foreign_keys;
    std::string order_index = std::to_string(ol_w_id) + "_" 
                              + std::to_string(ol_d_id) + "_"
                              + std::to_string(ol_o_id);
    std::string item_index = std::to_string(ol_supply_w_id) + "_"
                             + std::to_string(ol_i_id);
    foreign_keys["order_index"] = order_index;
    foreign_keys["item_index"] = item_index;
    return foreign_keys;
}

std::string OrderLine::ToString() const {
    std::stringstream ss;
    ss << "ol_o_id = " << ol_o_id
       << ",ol_d_id = " << ol_d_id
       << ",ol_w_id = " << ol_w_id
       << ",ol_number = " << ol_number
       << ",ol_i_id = " << ol_i_id
       << ",ol_supply_w_id = " << ol_supply_w_id
       << ",ol_quantity = " << ol_quantity
       << ",ol_amount = " << ol_amount
       << ",ol_delivery_d = " << ol_delivery_d
       << ",ol_dist_info = " << ol_dist_info;
    return ss.str();
}

/// ------------------------- [begin customer table] ---------------------- ///

Customer::Customer(int32_t id, int32_t d_id, int32_t w_id, const std::string& datetime,
         bool bad_credit, RandomGenerator* rand_gen)
    : c_id(id), 
      c_d_id(d_id),
      c_w_id(w_id),
      c_credit_lim(kInitCreditLimit),
      c_balance(kInitBalance),
      c_ytd_payment(kInitYTDPayment),
      c_payment_cnt(kInitPaymentCnt),
      c_delivery_cnt(kInitDeliveryCnt),
      c_middle("OE"),
      c_since(datetime) {
    c_discount = rand_gen->MakeFloat(kMinDisCount, kMaxDisCount, kDisCountDigits);
    c_first = rand_gen->MakeAString(kFirstLowerLen, kFirstUpperLen);
    c_last = GenLastName(rand_gen, (id <= 1000 ? id : kCustomerCountPerDistrict));
    c_street_1 = rand_gen->MakeAString(kStreetLowerLen, kStreetUpperLen);
    c_street_2 = rand_gen->MakeAString(kStreetLowerLen, kStreetUpperLen); 
    c_city = rand_gen->MakeAString(kCityLowerLen, kCityUpperLen);
    c_state = rand_gen->MakeAString(kStateLen,kStateLen);
    c_zip = GenZip(rand_gen);
    c_phone = rand_gen->MakeNString(kPhoneLen,kPhoneLen);
    c_credit = bad_credit ? "BC" : "GC";
    c_data = GenData(rand_gen, kCustomerDataLowerLen, kCustomerDataUpperLen, false);
}

std::string Customer::PrimaryKey() const { 
    return std::to_string(c_w_id) + "_" + std::to_string(c_d_id)
        + "_" + std::to_string(c_id);
}

std::string Customer::ForeignKey() const { 
    return std::to_string(c_w_id) + "_" + std::to_string(c_d_id);
}

std::string Customer::ToString() const {
    std::stringstream ss;
    ss << "c_id = " << c_id
       << ",c_d_id = " << c_d_id
       << ",c_w_id = " << c_w_id
       << ",c_credit_lim = " << c_credit_lim
       << ",c_discount = " << c_discount
       << ",c_balance = " << c_balance
       << ",c_ytd_payment = " << c_ytd_payment
       << ",c_payment_cnt = " << c_payment_cnt
       << ",c_delivery_cnt = " << c_delivery_cnt
       << ",c_name = [" << c_first << "," << c_middle << "," << c_last << "]"
       << ",c_street_1 = " << c_street_1
       << ",c_street_2 = " << c_street_2
       << ",c_city = " << c_city
       << ",c_state = " << c_state
       << ",c_zip = " << c_zip
       << ",c_phone = " << c_phone
       << ",c_since = " << c_since
       << ",c_credit = " << c_credit
       << ",c_data = " << c_data;
    return ss.str(); 
}

/// ------------------------- [begin history table] ----------------------- ///
std::string History::ToString() const {
    std::stringstream ss;
    ss << "h_c_id = " << h_c_id
       << ",h_c_d_id = " << h_c_d_id
       << ",h_c_w_id = " << h_c_w_id
       << ",h_d_id = " << h_d_id
       << ",h_w_id = " << h_w_id
       << ",h_amount = " << h_amount
       << ",h_date = " << h_date
       << ",h_data = " << h_data;
    return ss.str(); 
}

/// ------------------------- [end tables] -------------------------------- ///

bool TxnResult::State() const {
    return status_;
}

void TxnResult::SetState(bool status) {
    status_ = status;
}

void TxnResult::SetReason(const std::string& reason) {
    reason_ = reason;
}

void StockLevelResult::SetLowStock(int low_stock) {
    low_stock_ = low_stock;
}

int StockLevelResult::LowStock() const {
    return low_stock_;
}

void PaymentResult::SetSingleLine(const RetTuples& single_line) {
    single_line_ = single_line;
}

void NewOrderResult::AddLine(const RetTuples& line) {
    lines_.push_back(line);
} 

void NewOrderResult::SetSingleLine(const RetTuples& single_line) {
    single_line_ = single_line;
}

TpccDb* TpccDb::NewTpccDb(const std::string& db_type) {
    if (db_type == "tera") {
        return new TeraTpccDb();
    } else {
        LOG(ERROR) << "not support db:" << db_type;
    }
    return NULL;
}

} // namespace tpcc
} // namespace tera

