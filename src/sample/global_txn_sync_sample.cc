#include <memory>
#include <iostream>

#include <assert.h>
#include "tera.h"

int main(int argc, char* argv[]) {
  tera::ErrorCode error_code;

  tera::Client* client =
      tera::Client::NewClient("../conf/tera.flag", "global_txn_sample", &error_code);
  assert(client);
  // create or open tables
  tera::Table* t1 = nullptr;
  tera::Table* t2 = nullptr;
  if (!client->IsTableExist("t1", &error_code)) {
    tera::TableDescriptor schema("t1");
    schema.EnableTxn();  // 参与全局事务的表schema 都需要设置 txn=true
    schema.AddLocalityGroup("lg0");
    tera::ColumnFamilyDescriptor* cfd1 = schema.AddColumnFamily("cf1");
    cfd1->EnableGlobalTransaction();
    tera::ColumnFamilyDescriptor* cfd2 = schema.AddColumnFamily("cf2");
    cfd2->EnableGlobalTransaction();
    client->CreateTable(schema, &error_code);
    assert(error_code.GetType() == tera::ErrorCode::kOK);
  }

  if (!client->IsTableExist("t2", &error_code)) {
    tera::TableDescriptor schema("t2");
    schema.EnableTxn();  // 参与全局事务的表schema 都需要设置 txn=true
    schema.AddLocalityGroup("lg0");
    tera::ColumnFamilyDescriptor* cfd1 = schema.AddColumnFamily("cf1");
    cfd1->EnableGlobalTransaction();
    tera::ColumnFamilyDescriptor* cfd2 = schema.AddColumnFamily("cf2");
    cfd2->EnableGlobalTransaction();
    client->CreateTable(schema, &error_code);
    assert(error_code.GetType() == tera::ErrorCode::kOK);
  }
  // before global transaction should be
  // (1) OpenTable which you will r/w
  // (2) check OpenTable success
  t1 = client->OpenTable("t1", &error_code);
  assert(t1 && error_code.GetType() == tera::ErrorCode::kOK);

  t2 = client->OpenTable("t2", &error_code);
  assert(t2 && error_code.GetType() == tera::ErrorCode::kOK);

  // begin global transaction
  tera::Transaction* g_txn = client->NewGlobalTransaction();
  if (g_txn == NULL) {
    return -1;
  }
  if (error_code.GetType() != tera::ErrorCode::kOK) {
    std::cout << error_code.ToString() << std::endl;
    return -1;
  }
  // read from different tables
  std::unique_ptr<tera::RowReader> r1(t1->NewRowReader("r1"));
  std::unique_ptr<tera::RowReader> r2(t2->NewRowReader("r1"));
  r1->AddColumn("cf1", "q2");
  r2->AddColumn("cf1", "q2");
  // read from t1:r1:cf1:q2 and check
  g_txn->Get(r1.get());
  if (g_txn->GetError().GetType() != tera::ErrorCode::kOK) {
    std::cout << g_txn->GetError().ToString() << std::endl;
    return -1;
  }
  std::string r1_v = "";
  while (!r1->Done()) {
    std::cout << r1->Value() << std::endl;
    r1_v = r1->Value();
    r1->Next();
  }

  // read from t2:r1:cf1:q2 and check
  g_txn->Get(r2.get());
  if (g_txn->GetError().GetType() != tera::ErrorCode::kOK) {
    std::cout << g_txn->GetError().ToString() << std::endl;
    return -1;
  }
  std::string r2_v = "";
  while (!r2->Done()) {
    std::cout << r2->Value() << std::endl;
    r2_v = r2->Value();
    r2->Next();
  }

  // write to other columns
  std::unique_ptr<tera::RowMutation> m1(t1->NewRowMutation("r1"));
  std::unique_ptr<tera::RowMutation> m2(t2->NewRowMutation("r1"));
  m1->Put("cf1", "q1", r2_v);
  m2->Put("cf1", "q1", r1_v);

  g_txn->ApplyMutation(m1.get());
  g_txn->ApplyMutation(m2.get());
  // need not check ApplyMutation, Transaction will be check before commit.
  g_txn->Commit();
  if (g_txn->GetError().GetType() != tera::ErrorCode::kOK) {
    std::cout << g_txn->GetError().ToString() << std::endl;
  } else {
    std::cout << "commit success" << std::endl;
  }

  delete g_txn;
  // end global transaction
  return 0;
}
