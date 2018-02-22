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

DECLARE_string(tera_client_flagfile);
DECLARE_string(tera_table_schema_dir);

namespace tera {
namespace tpcc {

TeraTpccDb::TeraTpccDb() : client_(NULL) {
    ErrorCode error_code;
    client_ = Client::NewClient(FLAGS_tera_client_flagfile, "tera_tpcc", &error_code);
    if (client_ == NULL) {
        LOG(ERROR) << "new client failed. err:" << error_code.ToString();
        _Exit(EXIT_FAILURE);
    }
}

TeraTpccDb::~TeraTpccDb() {
    delete client_;
}

bool TeraTpccDb::CreateTables() {
    ErrorCode err;
    for (auto table : kTpccTables) {
        std::string schema_file = FLAGS_tera_table_schema_dir + table;
        TableDescriptor* desc = new TableDescriptor();
        if (ParseTableSchemaFile(schema_file, desc, &err)) {
            if (client_->CreateTable(*desc, &err) && err.GetType() == ErrorCode::kOK) {
                LOG(INFO) << "create table " << table << " ok";
                Table* table_ptr = client_->OpenTable(table, &err);
                if (table_ptr == NULL) {
                    LOG(ERROR) << "open table " << table << " failed";
                    delete desc;
                    return false;
                } else {
                    table_map_[table] = table_ptr;
                    LOG(INFO) << "open table " << table << " ok";
                }
            } else {
                LOG(ERROR) << "create table " << table << " failed";
                delete desc;
                return false;
            }
        } else {
            LOG(ERROR) << "load schema failed, schema_file:" << schema_file << "err:" << err.ToString();
            delete desc;
            return false;
        }
        delete desc;
    }
    return true;
}

bool TeraTpccDb::CleanTables() {
    ErrorCode err;
    for (auto table : kTpccTables) {
        if (!client_->DisableTable(table, &err)) {
            LOG(ERROR) << "fail to disable table : " << table << " err: " <<err.ToString();
        } else {
            // make sure clean tables 
            TableMeta table_meta;
            TabletMetaList tablet_list;
            tera::ClientImpl* client_impl = static_cast<tera::ClientImpl*>(client_);
            if (!client_impl->ShowTablesInfo(table, &table_meta, &tablet_list, &err)) {
                LOG(ERROR) << "table not exist: " << table;
                continue;
            }
            uint64_t tablet_num = tablet_list.meta_size();
            VLOG(11) << tablet_num;
            int wait_times = 0;
            while (true) {
                if (!client_impl->ShowTablesInfo(table, &table_meta, &tablet_list, &err)) {
                    LOG(ERROR) << "table not exist: " << table;
                    break;
                }
                uint64_t tablet_cnt = 0;
                for (int32_t i = 0; i < tablet_list.meta_size(); ++i) {
                    const TabletMeta& tablet = tablet_list.meta(i);
                    if (tablet.status() == kTabletDisable || tablet.status() == kTableOffLine) {
                        tablet_cnt++;
                    }
                }
                if (tablet_cnt == tablet_num) {
                    break;
                }
                if (wait_times < 20) {
                    sleep(1);
                } else {
                    LOG(ERROR) << "disable  table : " << table << " failed, try " << wait_times << " time(s)";
                    break;
                }
            }
        }
        if (!client_->DeleteTable(table, &err)) {
            LOG(ERROR) << "drop table: " << table << " failed. " << err.ToString();
        } else {
            LOG(INFO) << "drop table: "<< table << " done.";
        }
    }
    return true;
}

// init db 
bool TeraTpccDb::InsertItem(const Item& i) {
    std::string tablename = "t_item";
    if ( table_map_.find(tablename) == table_map_.end()) {
        return false;
    }
    Table* table = table_map_[tablename];
    Transaction* gtxn = client_->NewGlobalTransaction();
    RowMutation* mu = table->NewRowMutation(i.PrimaryKey());
    mu->Put("cf0", "i_id", std::to_string(i.i_id));
    mu->Put("cf0", "i_im_id", std::to_string(i.i_im_id));
    mu->Put("cf0", "i_price", std::to_string(i.i_price));
    mu->Put("cf0", "i_name", i.i_name);
    mu->Put("cf0", "i_data", i.i_data);
    gtxn->ApplyMutation(mu);
    gtxn->Commit();
    delete mu;
    if (gtxn->GetError().GetType() != ErrorCode::kOK) {
        LOG(ERROR) << "insert table:" << tablename << " failed. err:" 
                   << gtxn->GetError().ToString();
        delete gtxn;
        return false;
    }
    delete gtxn;
    return true;
}

bool TeraTpccDb::InsertWarehouse(const Warehouse& w) {
    std::string tablename = "t_warehouse";
    if ( table_map_.find(tablename) == table_map_.end()) {
        return false;
    }
    Table* table = table_map_[tablename];
    Transaction* gtxn = client_->NewGlobalTransaction();
    RowMutation* mu = table->NewRowMutation(w.PrimaryKey());
    mu->Put("cf0", "w_id", std::to_string(w.w_id));
    mu->Put("cf0", "w_tax", std::to_string(w.w_tax));
    mu->Put("cf0", "w_ytd", std::to_string(w.w_ytd));
    mu->Put("cf0", "w_name", w.w_name);
    mu->Put("cf0", "w_street_1", w.w_street_1);
    mu->Put("cf0", "w_street_2", w.w_street_2);
    mu->Put("cf0", "w_city", w.w_city);
    mu->Put("cf0", "w_state", w.w_state);
    mu->Put("cf0", "w_zip", w.w_zip);
    gtxn->ApplyMutation(mu);
    gtxn->Commit();
    delete mu;
    if (gtxn->GetError().GetType() != ErrorCode::kOK) {
        LOG(ERROR) << "insert table:" << tablename << " failed. err:" 
                   << gtxn->GetError().ToString();
        delete gtxn;
        return false;
    }
    delete gtxn;
    return true;
}

bool TeraTpccDb::InsertDistrict(const District& d) {
    std::string tablename = "t_district";
    if ( table_map_.find(tablename) == table_map_.end()) {
        return false;
    }
    Table* table = table_map_[tablename];
    Transaction* gtxn = client_->NewGlobalTransaction();
    RowMutation* mu = table->NewRowMutation(d.PrimaryKey());
    mu->Put("cf0", "d_id", std::to_string(d.d_id));
    mu->Put("cf0", "d_w_id", std::to_string(d.d_w_id));
    mu->Put("cf0", "d_tax", std::to_string(d.d_tax));
    mu->Put("cf0", "d_ytd", std::to_string(d.d_ytd));
    mu->Put("cf0", "d_next_o_id", std::to_string(d.d_next_o_id));
    mu->Put("cf0", "d_name", d.d_name);
    mu->Put("cf0", "d_street_1", d.d_street_1);
    mu->Put("cf0", "d_street_2", d.d_street_2);
    mu->Put("cf0", "d_city", d.d_city);
    mu->Put("cf0", "d_state", d.d_state);
    mu->Put("cf0", "d_zip", d.d_zip);
    gtxn->ApplyMutation(mu);
    gtxn->Commit();
    delete mu;
    if (gtxn->GetError().GetType() != ErrorCode::kOK) {
        LOG(ERROR) << "insert table:" << tablename << " failed. err:" 
                   << gtxn->GetError().ToString();
        delete gtxn;
        return false;
    }
    delete gtxn;
    return true;
}

bool TeraTpccDb::InsertCustomer(const Customer& c) {
    std::string tablename = "t_customer";
    std::string c_last_index_name = "t_customer_last_index";
    if ( table_map_.find(tablename) == table_map_.end()
            || table_map_.find(c_last_index_name) == table_map_.end()) {
        return false;
    }
    Table* table = table_map_[tablename];
    Table* t_index = table_map_[tablename];
    Transaction* gtxn = client_->NewGlobalTransaction();
    std::string key = std::to_string(c.c_w_id) + "_" + std::to_string(c.c_d_id)
        + "_" + c.c_last + "_" + std::to_string(c.c_id);
    RowMutation* index_mu = t_index->NewRowMutation(key);
    index_mu->Put("cf0", "c_id", std::to_string(c.c_id));
    index_mu->Put("cf0", "c_d_id", std::to_string(c.c_d_id));
    index_mu->Put("cf0", "c_w_id", std::to_string(c.c_w_id));
    index_mu->Put("cf0", "c_last", c.c_last);
    gtxn->ApplyMutation(index_mu);
    delete index_mu;

    RowMutation* mu = table->NewRowMutation(c.PrimaryKey());
    mu->Put("cf0", "c_id", std::to_string(c.c_id));
    mu->Put("cf0", "c_d_id", std::to_string(c.c_d_id));
    mu->Put("cf0", "c_w_id", std::to_string(c.c_w_id));
    mu->Put("cf0", "c_credit_lim", std::to_string(c.c_credit_lim));
    mu->Put("cf0", "c_discount", std::to_string(c.c_discount));
    mu->Put("cf0", "c_balance", std::to_string(c.c_balance));
    mu->Put("cf0", "c_ytd_payment", std::to_string(c.c_ytd_payment));
    mu->Put("cf0", "c_payment_cnt", std::to_string(c.c_payment_cnt));
    mu->Put("cf0", "c_delivery_cnt", std::to_string(c.c_delivery_cnt));
    mu->Put("cf0", "c_first", c.c_first);
    mu->Put("cf0", "c_middle", c.c_middle);
    mu->Put("cf0", "c_last", c.c_last);
    mu->Put("cf0", "c_street_1", c.c_street_1);
    mu->Put("cf0", "c_street_2", c.c_street_2);
    mu->Put("cf0", "c_city", c.c_city);
    mu->Put("cf0", "c_state", c.c_state);
    mu->Put("cf0", "c_zip", c.c_zip);
    mu->Put("cf0", "c_phone", c.c_phone);
    mu->Put("cf0", "c_since", c.c_since);
    mu->Put("cf0", "c_credit", c.c_credit);
    mu->Put("cf0", "c_data", c.c_data);
    gtxn->ApplyMutation(mu);
    gtxn->Commit();
    delete mu;
    if (gtxn->GetError().GetType() != ErrorCode::kOK) {
        LOG(ERROR) << "insert table:" << tablename << " failed. err:" 
                   << gtxn->GetError().ToString();
        delete gtxn;
        return false;
    }
    delete gtxn;
    return true;
}

bool TeraTpccDb::InsertHistory(const History& h) {
    std::string tablename = "t_history";
    std::string history_index_name = "t_history_index";

    if (table_map_.find(tablename) == table_map_.end() || 
            table_map_.find(history_index_name) == table_map_.end()) {
        return false;
    }
    Table* table = table_map_[tablename];
    Table* t_history_index = table_map_[history_index_name];
    Transaction* gtxn = client_->NewGlobalTransaction();
    
    RowReader* hindex_reader = t_history_index->NewRowReader("count");
    RetTuples hindex_ret;
    int cnt = -1;
    TxnResult ret;
    if (hindex_reader->GetError().GetType() != ErrorCode::kNotFound
            && !GetValues(&ret, gtxn, hindex_reader, 
                   {"count"},
                   &hindex_ret,
                   "@insert_history|hindex_reader|count")) {
        return false;
    } else if (hindex_reader->GetError().GetType() == ErrorCode::kNotFound) {
        cnt = 0;
    } else {
        cnt = std::stoi(hindex_ret["count"]);
    }
    
    RowMutation* hindex_mu = t_history_index->NewRowMutation("count");
    hindex_mu->Put("cf0", "count", std::to_string(++cnt));
    gtxn->ApplyMutation(hindex_mu);
    delete hindex_mu;

    RowMutation* mu = table->NewRowMutation(std::to_string(cnt));
    mu->Put("cf0", "h_c_id", std::to_string(h.h_c_id));
    mu->Put("cf0", "h_c_d_id", std::to_string(h.h_c_d_id));
    mu->Put("cf0", "h_c_w_id", std::to_string(h.h_c_w_id));
    mu->Put("cf0", "h_d_id", std::to_string(h.h_d_id));
    mu->Put("cf0", "h_w_id", std::to_string(h.h_w_id));
    mu->Put("cf0", "h_amount", std::to_string(h.h_amount));
    mu->Put("cf0", "h_date", h.h_date);
    mu->Put("cf0", "h_data", h.h_data);
    gtxn->ApplyMutation(mu);
    gtxn->Commit();
    delete mu;
    if (gtxn->GetError().GetType() != ErrorCode::kOK) {
        LOG(ERROR) << "insert table:" << tablename << " failed. err:" 
                   << gtxn->GetError().ToString();
        delete gtxn;
        return false;
    }
    delete gtxn;
    return true;
}

bool TeraTpccDb::InsertStock(const Stock& s) {
    std::string tablename = "t_stock";
    if ( table_map_.find(tablename) == table_map_.end()) {
        return false;
    }
    Table* table = table_map_[tablename];
    Transaction* gtxn = client_->NewGlobalTransaction();
    RowMutation* mu = table->NewRowMutation(s.PrimaryKey());

    mu->Put("cf0", "s_i_id", std::to_string(s.s_i_id));
    mu->Put("cf0", "s_w_id", std::to_string(s.s_w_id));
    mu->Put("cf0", "s_quantity", std::to_string(s.s_quantity));
    mu->Put("cf0", "s_ytd", std::to_string(s.s_ytd));
    mu->Put("cf0", "s_order_cnt", std::to_string(s.s_order_cnt));
    mu->Put("cf0", "s_remote_cnt", std::to_string(s.s_remote_cnt));
    int i = 0;
    for (auto dist : s.s_dist) {
        mu->Put("cf0", "s_dist_" + std::to_string(++i), dist);
    }
    mu->Put("cf0", "s_data", s.s_data);

    gtxn->ApplyMutation(mu);
    gtxn->Commit();
    delete mu;
    if (gtxn->GetError().GetType() != ErrorCode::kOK) {
        LOG(ERROR) << "insert table:" << tablename << " failed. err:" 
                   << gtxn->GetError().ToString();
        delete gtxn;
        return false;
    }
    delete gtxn;
    return true;
}

bool TeraTpccDb::InsertOrder(const Order& o) {
    std::string tablename = "t_order";
    std::string indexname = "t_order_index";
    if ( table_map_.find(tablename) == table_map_.end() || 
            table_map_.find(indexname) == table_map_.end()) {
        return false;
    }
    Table* table = table_map_[tablename];
    Table* index = table_map_[indexname];

    Transaction* gtxn = client_->NewGlobalTransaction();
    RowMutation* mu = table->NewRowMutation(o.PrimaryKey());
    std::string index_key = o.ForeignKey() + "_" + std::to_string(o.o_id);
    RowMutation* index_mu = index->NewRowMutation(index_key);
    index_mu->Put("cf0", "o_id", std::to_string(o.o_id));
    index_mu->Put("cf0", "o_c_id", std::to_string(o.o_c_id));
    index_mu->Put("cf0", "o_d_id", std::to_string(o.o_d_id));
    index_mu->Put("cf0", "o_w_id", std::to_string(o.o_w_id));
    mu->Put("cf0", "o_id", std::to_string(o.o_id));
    mu->Put("cf0", "o_c_id", std::to_string(o.o_c_id));
    mu->Put("cf0", "o_d_id", std::to_string(o.o_d_id));
    mu->Put("cf0", "o_w_id", std::to_string(o.o_w_id));
    mu->Put("cf0", "o_carrier_id", std::to_string(o.o_carrier_id));
    mu->Put("cf0", "o_ol_cnt", std::to_string(o.o_ol_cnt));
    mu->Put("cf0", "o_all_local", std::to_string(o.o_all_local));
    mu->Put("cf0", "o_entry_d", o.o_entry_d);
    gtxn->ApplyMutation(mu);
    gtxn->ApplyMutation(index_mu);
    delete mu;
    delete index_mu;
    gtxn->Commit();
    if (gtxn->GetError().GetType() != ErrorCode::kOK) {
        LOG(ERROR) << "insert table:" << tablename << " failed. err:" 
                   << gtxn->GetError().ToString();
        delete gtxn;
        return false;
    }
    delete gtxn;
    return true;
}

bool TeraTpccDb::InsertOrderLine(const OrderLine& ol) {
     std::string tablename = "t_orderline";
    if ( table_map_.find(tablename) == table_map_.end()) {
        return false;
    }
    Table* table = table_map_[tablename];
    Transaction* gtxn = client_->NewGlobalTransaction();
    RowMutation* mu = table->NewRowMutation(ol.PrimaryKey());
    mu->Put("cf0", "ol_o_id", std::to_string(ol.ol_o_id));
    mu->Put("cf0", "ol_d_id", std::to_string(ol.ol_d_id));
    mu->Put("cf0", "ol_w_id", std::to_string(ol.ol_w_id));
    mu->Put("cf0", "ol_number", std::to_string(ol.ol_number));
    mu->Put("cf0", "ol_i_id", std::to_string(ol.ol_i_id));
    mu->Put("cf0", "ol_supply_w_id", std::to_string(ol.ol_supply_w_id));
    mu->Put("cf0", "ol_quantity", std::to_string(ol.ol_quantity));
    mu->Put("cf0", "ol_amount", std::to_string(ol.ol_amount));
    mu->Put("cf0", "ol_delivery_d", ol.ol_delivery_d);
    mu->Put("cf0", "ol_dist_info", ol.ol_dist_info);
    gtxn->ApplyMutation(mu);
    gtxn->Commit();
    delete mu;
    if (gtxn->GetError().GetType() != ErrorCode::kOK) {
        LOG(ERROR) << "insert table:" << tablename << " failed. err:" 
                   << gtxn->GetError().ToString();
        delete gtxn;
        return false;
    }
    delete gtxn;
    return true;
}

bool TeraTpccDb::InsertNewOrder(const NewOrder& no) {
    std::string tablename = "t_neworder";
    if ( table_map_.find(tablename) == table_map_.end()) {
        return false;
    }
    Table* table = table_map_[tablename];
    Transaction* gtxn = client_->NewGlobalTransaction();
    RowMutation* mu = table->NewRowMutation(no.PrimaryKey());
    mu->Put("cf0", "no_o_id", std::to_string(no.no_o_id));
    mu->Put("cf0", "no_d_id", std::to_string(no.no_d_id));
    mu->Put("cf0", "no_w_id", std::to_string(no.no_w_id));
    gtxn->ApplyMutation(mu);
    gtxn->Commit();
    delete mu;
    if (gtxn->GetError().GetType() != ErrorCode::kOK) {
        LOG(ERROR) << "insert table:" << tablename << " failed. err:" 
                   << gtxn->GetError().ToString();
        delete gtxn;
        return false;
    }
    delete gtxn;
    return true;
}

void TeraTpccDb::SetTxnResult(TxnResult* ret, Transaction* gtxn, bool state, 
                              const std::string& msg) {
    ret->SetState(state);
    if (msg != "") {
        ret->SetReason(gtxn->GetError().GetReason() + " msg:" + msg);
    } else {
        ret->SetReason(gtxn->GetError().GetReason());
    }
}

bool TeraTpccDb::GetValues(TxnResult* ret, Transaction* gtxn, RowReader* reader,
                           std::initializer_list<std::string> qu_names_initlist,
                           RetTuples* ret_tuples,
                           const std::string& if_error_msg) {
    std::vector<std::string> qu_names(qu_names_initlist);
    for (auto& qu_name : qu_names) {
        reader->AddColumn("cf0", qu_name);
    }
    gtxn->Get(reader);
    if (gtxn->GetError().GetType() != ErrorCode::kOK) {
        SetTxnResult(ret, gtxn, false, if_error_msg);
        delete reader;
        return false;
    } else {
        RowReader::TRow row;
        reader->ToMap(&row);
        for (auto qu_name : qu_names) {
            if (row["cf0"].find(qu_name) != row["cf0"].end()) {
                 for (auto k : row["cf0"][qu_name]) {
                     ret_tuples->insert({{qu_name, k.second}});
                     break;
                 }
            }
        }
        delete reader;
    }
    return true;
}

bool TeraTpccDb::GetCustomer(TxnResult* ret, Transaction* gtxn, bool by_last_name, 
                             const std::string& last_name, int32_t customer_id,
                             int32_t warehouse_id, int32_t district_id,
                             std::string* customer_key, RetTuples* customer_ret) {
    // open table
    Table* t_customer_last_index = table_map_[kTpccTables[kCustomerLastIndex]];
    Table* t_customer = table_map_[kTpccTables[kCustomerTable]];
    *customer_key = std::to_string(warehouse_id) + "_" + std::to_string(district_id) + "_";

    if (by_last_name) {
        ErrorCode error_code;
        std::string start_key = *customer_key + last_name + "_";
        ScanDescriptor scan_desc(start_key);
        scan_desc.SetEnd(start_key + "~");
        scan_desc.AddColumnFamily("cf0");
        ResultStream* scanner = t_customer_last_index->Scan(scan_desc, &error_code);
        std::vector<std::string> keys;
        for (scanner->LookUp(start_key); !scanner->Done(); scanner->Next()) {
            std::string row_key = scanner->RowName();
            if (row_key.find(start_key) == std::string::npos) {
                break;
            }

            RowReader* index_reader = t_customer_last_index->NewRowReader(row_key);
            RetTuples index_ret;
            if (!GetValues(ret, gtxn, index_reader, 
                           {"c_id"},
                           &index_ret, 
                           "@get_customer|index_reader|" + row_key)) {
                delete scanner;
                return false;
            }
            keys.push_back(index_ret["c_id"]);
        }
        delete scanner;
        size_t pos = keys.size();
        pos = pos % 2 == 0 ? (pos / 2 - 1) : (pos / 2);
        *customer_key += keys.at(pos);
    } else {
        *customer_key += std::to_string(customer_id); 
    }
    RowReader* customer_reader = t_customer->NewRowReader(*customer_key);
    if (!GetValues(ret, gtxn, customer_reader,
                   {"c_id", "c_d_id", "c_w_id", "c_first", "c_middle", "c_last",
                    "c_balance", "c_ytd_payment", "c_payment_cnt", "c_credit", 
                    "c_data", "c_street_1", "c_street_2", "c_city", "c_state",
                    "c_zip", "c_phone", "c_since", "c_credit_lim", "c_discount"},
                   customer_ret,
                   "@get_customer|customer_reader" + *customer_key)) {
        return false;
    }
    return true;
}

} // namespace tpcc
} // namespace tera
