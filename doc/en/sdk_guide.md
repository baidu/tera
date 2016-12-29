# Tera SDK Guide

>**Directory**
>  1. [Interfaces](#interfaces)
>   * tera::Client, tera::Table
>   * tera::RowMutation, tera::RowReader, tera::ScanDescriptor/tera::ResultStream
>   * tera::TableDescriptor, tera::LocalityGroupDescriptor, tera::ColumnFamilyDescriptor
>  2. [Samples](#samples)
>   * [Table management](#management-samples)
>     * Table description
>     * Table control
>   * [Data manipulation](#manipulation-samples)
>     * Read
>     * Write
>     * Scan
>  3. [Java SDK](sdk_guide_java.md)
>  4. [Python SDK](sdk_guide_python.md)

<a name="interfaces"></a>
## 1. Interfaces

### 1.1 tera::Client --- *used to communicate with a single Tera cluster*

tera::Client is the entry point of all subsequent access to Tera.

Functions include:

1. Table definition: create, update, etc.
2. Table control: drop, enable, disable, add snapshot, delete snapshot, etc
3. Table access: open, close, table schema acquirement, etc.
4. User management: useradd, userdel, chpassword, group management, etc.
5. Cluster state monitor: table state monitor, node state monitor, etc.

Suggested usage:

* One client for one cluster. Create more than one client only if you need to communicate with more than one cluster.

### 1.2 tera::Table --- *used to communicate with a single Tera table*

tera::Table is the fundamental class for all manipulation APIs of table.
Obtain an instance of tera::Table from *tera::Client::OpenTable()*, and *delete* it when no longer needed.

### 1.3 tera::RowReader --- *used to perform random read operations*

A RowReader describes a Get operation for the specified row and returns filtered cells of table.
* Obtain an instance of RowReader from *tera::Table::NewRowReader()*, apply it to server by *tera::Table::Get()*, and *delete* it if it is no longer needed.
* Set a callback in it to activate asynchronous read.
* Check if the read is successful by *RowReader::GetError()*.
* Users may set request filter conditions, such as set of columns, maximum versions, time range, etc.
* There are two ways to access the returned data of RowReader. One way is to use it as an iterator and the other way is to use it as a nested *std::map*.

### 1.4 tera::RowMutation --- *used to perform write operations (insert, update and delete)*

A RowMutation keeps a series of operations named "mutation".
* Obtain an instance of RowMutation from *tera::Table::NewRowMutation()*, commit it to server by *tera::Table::Put()* atomically, and *delete* it if it is no longer needed.
* Set a callback in it to activate asynchronous write.
* Check if the write operation is successful by *RowMutation::GetError()*.
* RowMutation supports atomic operations such as counter, string-append, put-if-absent, etc.

### 1.5 tera::ScanDescriptor/tera::ResultStream --- *used to perform sequential read operations*

A ScanDescriptor describes a sequential read demand, which may include range of row key, set of columns, time range, maximum and minimum versions, user defined filters, etc.
A ResultStream is a heap-allocated iterator over the contents of the desired cells of table.
* Obtain an instance of ScanDescriptor from *new tera::ScanDescriptor()*, apply it to server meanwhile obtain an instance of ResultStream by *tera::Table::Scan()*.
* If ScanDescriptor or ResultStream are no longer needed, *delete* them.
* Call *ResultStream::Done()* to check whether all the desired cells are returned; call *ResultStream::Next()* to make the iterator point to the next cell.

### 1.6 tera::TableDescriptor/tera::LocalityGroupDescriptor/tera::ColumnFamilyDescriptor --- *used to define a table*

A TableDescriptor, a LocalityGroupDescriptor and a ColumnFamilyDescriptor describe the schema of a table, a locality group and a column family, respectively.
* They are used by *tera::CLient::CreateTable()* and *tera::CLient::UpdateTable()*.
* It's not a nessesary to use these interfaces. Users are encouraged to describe the schema of a table by teracli.

### 1.7 tera::ErrorCode --- *used to get error*

<a name="samples"></a>
## 2. Samples
```
tera::ErrorCode error_code;

// Obtain an instance of tera::Client
tera::Client* client = tera::Client::NewClient("./tera.flag", &error_code);
if (client == NULL) {
    /* exception handler */
}
```

<a name="management-samples"></a>
### 2.1 Table management samples

#### Table description
```
// Create a TableDescripor
// Set table name to "hello"
tera::TableDescriptor table_desc("hello");

// Add a locality group "lg_1" to the table
tera::LocalityGroupDescriptor* lg_desc = table_desc.AddLocalityGroup("lg_1");
// Set the attributes of the locality group
// Set the storage to "Flash", block size to 8KB
lg_desc->SetStore(tera::kInFlash);
lg_desc->SetBlockSize(8);

// Add a column family "cf_11" to the table and set its LG to "lg_1"
tera::ColumnFamilyDescriptor* cf_t = table_desc.AddColumnFamily("cf_11", "lg_1");
// Set the attributes of the column family
// Set the max version to 5, TTL to 10000 seconds
cf_t->SetMaxVersions(5);
cf_t->SetTimeToLive(10000);
```

#### Table control
```
// Create table
client->CreateTable(table_desc, &error_code);

// Disable and enable table
client->DisableTable("hello", &error_code);
client->EnableTable("hello", &error_code);

// Drop table
client->DisableTable("hello", &error_code);
client->DeleteTable("hello", &error_code);

// Get the schema of table
tera::TableDescriptor* hello_desc = client->GetTableDescriptor("hello", &error_code);
/* ... */
delete hello_desc;       // Remember to delete the TableDescriptor

// Get the status of table
if (client->IsTableExist("hello", &error_code)) {
    /* ... */
}
if (client->IsTableEnable("hello", &error_code)) {
    /* ... */
}
if (client->IsTableEmpty("hello", &error_code)) {
    /* ... */
}

// Get all tables of cluster
std::vector<tera::TableInfo> table_list;
client->List(&table_list, &error_code);

// Get the locations of tablets
tera::TableInfo table_info = {NULL, ""};
std::vector<tera::TabletInfo> tablet_list;
client->List("hello", &table_info, &tablet_list, &error_code);
```

<a name="manipulation-samples"></a>
### 2.2 Data manipulation samples

#### Open table
```
// Open a table
tera::Table* table =  client->OpenTable("hello", &error_code);
```

#### Read
```
// Perform a simple synchronous read operation
std::string value;
table->Get("rowkey1", "family1", "qualifier1", &value, &error_code);     // Get the latest value of column "rowkey1:qualifier1" of row "rowkey1"

// Create a read operation
tera::RowReader* reader = table->NewRowReader("rowkey2");      // Create a RowReader for row "rowkey2"
reader->AddColumn("family21", "qualifier21");                  // Get all columns from column family "family21" with qualifier "qualifier21"
reader->AddColumnFamily("family22");                           // Get all columns from column family "family22"

// Perform the read operation
reader->SetCallBack(BatchGetCallBack);                         // Set asynchronous mode
table->Get(reader);                                            // Commit the read operation to server
while (!table->IsGetFinished());                               // Wait background operations to finish
while (!reader->Done()) {
    /* process the returned cell */
    reader->Next();
}

// Cleanup
delete reader;                                                 // remember to delete the RowReader

// Batch read operations
std::vector<tera::RowReader*> readers;
tera::RowReader* reader = table->NewRowReader("rowkey2");
readers.push_back(reader);

// Perform batch read operations
table->Get(readers);

// Cleanup
for (size_t i = 0; i < readers.size(); ++i) {
    delete readers[i];
}
```

#### Write
```
// Perform a simple synchronous write operation
table->Put("rowkey1", "family1", "qualifier1", "value11", &error_code);    // Put value "value11" to column "family1:qualifier1" of row "rowkey1"

// Create a write operation
tera::RowMutation* mutation = table->NewRowMutation("rowkey2");     // Create a write operation for row "rowkey2"
mutation->Put("family21", "qualifier21", "value21");                // Put value "value21" to column "family21:qualifier21"
mutation->Put("family22", "qualifier22", "value22");                // Put value "value22" to column "family22:qualifier22"
mutation->DeleteFamily("family11");                                 // Delete all columns of column family "family11"
mutation->DeleteColumns("family22", "qualifier22");                 // Delete all columns of column family "family22" with qualifier "qualifier22"

// Perform write operation
mutation->SetCallBack(CallBack);                         // Set asynchronous mode
table->ApplyMutation(mutation);                          // Commit the write operation to server
while (!table->IsPutFinished());                         // Wait background operations to finish

// Cleanup
delete mutation;                                         // remember to delete the RowMutation

// Batch write operaions
std::vector<tera::RowMutation*> mutations;
tera::RowMutation* mutation = table->NewRowMutation("rowkey2");
mutations.push_back(mutation);

// Perform batch write operations
table->ApplyMutation(mutations);

// Cleanup
for (size_t i = 0; i < mutations.size(); ++i) {
    delete mutations[i];
}
```

#### Scan
```
// Create a scan operation
tera::ScanDescriptor desc("rowkey1");               // Create a scan operation starting at "rowkey1" (including)
desc.SetEnd("rowkey2");                             // Set the scan operation to stop at "rowkey2" (not including)
desc.AddColumnFamily("family21");                   // Get all columns from column family "family21"
desc.AddColumn("family22", "qualifier22");          // Get the column from column family "family22" with qualifier "qualifier22"

// Perform the scan operation
tera::ResultStream* result_stream = table->Scan(desc, &error_code);
while (!result_stream->Done()) {
    /* process the returned cell */
    result_stream->Next();
}

// cleanup
delete result_stream;                               // remember to delete the ResultStream
```

#### Close table
```
delete table;
delete client;
```
