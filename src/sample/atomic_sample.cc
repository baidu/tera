#include <iostream>
#include <assert.h>
#include "tera.h"

int main() {
    tera::ErrorCode error_code;

    // Get a client instance
    tera::Client* client = tera::Client::NewClient("./tera.flag", "atomic_sample", &error_code);
    assert(client);

    // Create table
    tera::TableDescriptor schema("atomic_sample");
    schema.AddLocalityGroup("lg0");
    schema.AddColumnFamily("cnt");
    schema.AddColumnFamily("sum");
    client->CreateTable(schema, &error_code);
    // CreateTable status check is intentionally ignored to support repeated call.

    // Open table
    tera::Table* table = client->OpenTable("atomic_sample", &error_code);
    assert(table);

    // init a row
    tera::RowMutation* init = table->NewRowMutation("key1");
    init->Put("cnt", "", tera::CounterCoding::EncodeCounter(10));
    init->Put("sum", "", tera::CounterCoding::EncodeCounter(100));
    table->ApplyMutation(init);
    assert(init->GetError().GetType() == tera::ErrorCode::kOK);
    delete init;

    // add into the row
    tera::RowMutation* mutation = table->NewRowMutation("key1");
    mutation->Add("cnt", "", 1);
    mutation->Add("sum", "", 20);
    table->ApplyMutation(mutation);
    assert(mutation->GetError().GetType() == tera::ErrorCode::kOK);
    delete mutation;

    // read the row
    tera::RowReader* reader = table->NewRowReader("key1");
    table->Get(reader);
    assert(reader->GetError().GetType() == tera::ErrorCode::kOK);

    int64_t counter = 0;
    while (!reader->Done()) {
        if (reader->Family() == "cnt") {
            tera::CounterCoding::DecodeCounter(reader->Value(), &counter);
            assert(counter == 11);
        } else if (reader->Family() == "sum") {
            tera::CounterCoding::DecodeCounter(reader->Value(), &counter);
            assert(counter == 120);
        }
        std::cout << reader->Family() << ": " << counter << std::endl;
        reader->Next();
    }
    delete reader;

    // Close
    delete table;
    delete client;
    return 0;
}
