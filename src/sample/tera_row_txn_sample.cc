#include <assert.h>
#include <stdio.h>

#include "tera.h"

int main() {
  tera::ErrorCode error_code;

  // Get a client instance
  tera::Client* client = tera::Client::NewClient("./tera.flag", "txn_sample", &error_code);
  assert(client);

  // Create table
  tera::TableDescriptor schema("employee");
  schema.EnableTxn();
  schema.AddLocalityGroup("lg0");
  schema.AddColumnFamily("title");
  schema.AddColumnFamily("salary");
  client->CreateTable(schema, &error_code);
  assert(error_code.GetType() == tera::ErrorCode::kOK);

  // Open table
  tera::Table* table = client->OpenTable("employee", &error_code);
  assert(table);

  // init a row
  tera::RowMutation* init = table->NewRowMutation("Amy");
  init->Put("title", "", "junior");
  init->Put("salary", "", "100");
  table->ApplyMutation(init);
  assert(init->GetError().GetType() == tera::ErrorCode::kOK);
  delete init;

  // txn read the row
  tera::Transaction* txn = table->StartRowTransaction("Amy");
  tera::RowReader* reader = table->NewRowReader("Amy");
  reader->AddColumnFamily("title");
  txn->Get(reader);
  assert(reader->GetError().GetType() == tera::ErrorCode::kOK);

  // get title
  std::string title;
  while (!reader->Done()) {
    if (reader->Family() == "title") {
      title = reader->Value();
      break;
    }
    reader->Next();
  }
  delete reader;

  // txn write the row
  tera::RowMutation* mutation = table->NewRowMutation("Amy");
  if (title == "junior") {
    mutation->Put("title", "", "senior");
    mutation->Put("salary", "", "200");
  } else if (title == "senior") {
    mutation->Put("title", "", "director");
    mutation->Put("salary", "", "300");
  }
  txn->ApplyMutation(mutation);
  assert(mutation->GetError().GetType() == tera::ErrorCode::kOK);
  delete mutation;

  // txn commit
  table->CommitRowTransaction(txn);
  printf("Transaction commit result %s\n", txn->GetError().ToString().c_str());
  delete txn;

  // Close
  delete table;
  delete client;
  return 0;
}
