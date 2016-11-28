# Tera Python SDK Guide

## How to Use

1. Get [TeraSdk.py](../../src/sdk/python/TeraSdk.py).
1. Build Tera. See [Build](../../Build).
1. Add the path of libtera_c.so to the environment variable **LD_LIBRARY_PATH**.
1. Write your own application code. See [sample](../../src/sdk/python/sample.py).

## API Doc

See to [TeraSdk.py](../../src/sdk/python/TeraSdk.py) to get detailed explainations of APIs from the code comments.

## Support Features

1. Synchronous Read
1. Synchronous and Asynchronous Write
1. Scan

## Implementations

1. Tera Python SDK uses **ctypes** library to communicate with **libtera_c.so**.
From V2.3, Python standard libraries bring ctypes library.
1. PythonVM and libtera_c.so have to be binary compatible.

## API Examples

Python SDK implements the data manipulation operations.

Table management operations should be  achieved by [teracli](../teracli.md)

### Client & Table

One Client object is used to communicate to one Tera cluster.
```
try:
    client = Client("./tera.flag", "pysdk_log_prefix")
    '''
    table "oops" has been created by admin
    '''
    table = client.OpenTable("oops")
except TeraSdkException as e:
    print(e.reason)
```

### Write

Synchronous write
```
try:
    table.Put("row_key", "column_family", "qualifier", "value")
except TeraSdkException as e:
    print(e.reason)
```

### Read

Synchronous read
```
try:
    print(table.Get("row_key", "column_family", "qualifier", 0))
except TeraSdkException as e:
    print(e.reason)
```
