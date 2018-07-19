#include <atomic>
#include <iostream>
#include <memory>
#include <thread>

#include <assert.h>
#include <unistd.h>

#include "tera.h"

std::string read_result = "";
std::atomic<bool> all_gtxn_thread_done(false);
std::atomic<int> finish_cnt(0);
    
struct RowReaderContext {
    tera::Transaction* gtxn;
    tera::Table* t1;
    tera::Table* t2;
};

tera::Table* InitTable(tera::Client* client, const std::string& tablename) {
    tera::ErrorCode error_code;
    if (!client->IsTableExist(tablename, &error_code)) {
        tera::TableDescriptor schema(tablename);
        schema.EnableTxn(); // 参与全局事务的表schema 都需要设置 txn=true
        schema.AddLocalityGroup("lg0");
        tera::ColumnFamilyDescriptor* cfd1 = schema.AddColumnFamily("cf1");
        cfd1->EnableGlobalTransaction();
        tera::ColumnFamilyDescriptor* cfd2 = schema.AddColumnFamily("cf2");
        cfd2->DisableGlobalTransaction();
        client->CreateTable(schema, &error_code);
        assert(error_code.GetType() == tera::ErrorCode::kOK);
    }
    
    tera::Table* table = client->OpenTable(tablename, &error_code);
    assert(table && error_code.GetType() == tera::ErrorCode::kOK);
    return table;
}

void TxnCallBack(tera::Transaction* txn) {
    if (txn->GetError().GetType() != tera::ErrorCode::kOK) {
        std::cout << "txn failed, start_ts= " << txn->GetStartTimestamp() 
                  << ", reason= " << txn->GetError().ToString()
                  << std::endl;
    } else {
        std::cout << "gtxn success" << std::endl;
    }
    delete txn;
    all_gtxn_thread_done.store(true);
}

void ReadRowCallBack(tera::RowReader* row_reader) {
    RowReaderContext* ctx = (RowReaderContext*)row_reader->GetContext();
    while (!row_reader->Done()) {
        printf("Row: %s\%s\%ld\%s\n",
                row_reader->RowName().c_str(), row_reader->ColumnName().c_str(),
                row_reader->Timestamp(), row_reader->Value().c_str());
        row_reader->Next();
        read_result += row_reader->Value();
    }
    delete row_reader;
    ++finish_cnt;
    // mutations begin at all reader callback done
    if (finish_cnt.load() == 2) {
        // write to other columns
        tera::Transaction* g_txn = ctx->gtxn;
        tera::RowMutation* m1 = ctx->t1->NewRowMutation("r1");
        tera::RowMutation* m2 = ctx->t2->NewRowMutation("r1");
        m1->Put( "cf1", "q1", read_result);
        m2->Put( "cf1", "q1", read_result);

        // ApplyMutation only modifying local memory and do not need asynchronous
        // we also support asynchronous interface for RowMutation，as you like
        g_txn->ApplyMutation(m1);
        g_txn->ApplyMutation(m2);
        g_txn->SetCommitCallback(TxnCallBack);
        delete m1;
        delete m2;
        // need not check ApplyMutation, Transaction will be check before commit.
        g_txn->Commit();
    }
}

void DoTxn(tera::Client* client, tera::Table* t1, tera::Table* t2) {
    
    // begin global transaction
    tera::Transaction* g_txn = client->NewGlobalTransaction();
    if (g_txn == NULL) {
        return;
    }

    // read from different tables 
    tera::RowReader* r1 = t1->NewRowReader("r1");
    tera::RowReader* r2 = t2->NewRowReader("r1");
    r1->AddColumn("cf1", "q2");
    r2->AddColumn("cf1", "q2");
    r1->SetCallBack(ReadRowCallBack);
    r2->SetCallBack(ReadRowCallBack);
    RowReaderContext ctx;
    ctx.gtxn = g_txn;
    ctx.t1 = t1;
    ctx.t2 = t2;
    r1->SetContext(&ctx);
    r2->SetContext(&ctx);
    // read from t1:r1:cf1:q2 and check 
    g_txn->Get(r1);
    // read from t2:r1:cf1:q2 and check 
    g_txn->Get(r2);
}

int main(int argc, char *argv[]) {

    tera::ErrorCode error_code;
    
    tera::Client* client = tera::Client::NewClient("../conf/tera.flag", "global_txn_sample_async", &error_code);
    if (client == NULL) {
        return -1;
    }

    // create or open tables
    // before global transaction should be 
    // (1) OpenTable which you will r/w
    // (2) check OpenTable success
    tera::Table* t1 = InitTable(client, "t1");
    tera::Table* t2 = InitTable(client, "t2");

    // the global transaction may add to threadpool, which implements by yourself.
    //
    // In this example, 
    //
    // first, read two cell values from different tables,
    // next, get all values concat at reader callback, 
    // last, put concat result into different tables.
    DoTxn(client, t1, t2);

    // global transaction thead always finished before callback
    // wait for callback thread done at main thread
    // if your know the program can't exit before callback done, it's not necessary.
    while (!all_gtxn_thread_done.load()) {
        usleep(100);
    }
    return 0;
}
