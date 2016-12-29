# Tera HTTP SDK Guide

## How to Build

```
make terahttp
```

## How to Run

```
./terahttp --flagfile=/path/to/flag
```

Tera HTTP Proxy needs a flagfile to execude just as a normal tera cilent process.

## Use HTTP Proxy to Access Tera

The HTTP request should be formatted as JSON. Refer to [Sofa-pbrpc's HTTP Support](https://github.com/baidu/sofa-pbrpc/wiki/%E9%AB%98%E7%BA%A7%E4%BD%BF%E7%94%A8)

### Sample of Curl

```
curl -d '

{
    "tablename": "oops",                        // table name
    "mutation_list": [                          // support batch write
        {                                       // set the first row mutation
            "rowkey": "row35",                  // set rowkey of mutation
            "type": "put",                      // set type of mutation, support put, del-col and del-row
            "columns": [                        // list of columns
                {
                    "columnfamily": "cf0",      // set column family of first column
                    "qualifier": "qu0",         // set qualifier of first column
                    "value": "value35"          // set value of first column
                },
                {                               // set the second column
                    "columnfamily": "cf0",
                    "qualifier": "qu1",
                    "value": "value35.2"
                }
            ]

        },
        {                                       // set the second row mutation
            "rowkey": "row36",
            "type": "put",
            "columns": [
                {
                    "columnfamily": "cf0",
                    "qualifier": "qu0",
                    "value": "value36"

                },
                {
                    "columnfamily": "cf0",
                    "qualifier": "qu1",
                    "value": "value36.2"
                }
            ]
        }
    ]
}

' http://localhost:12321/tera.http.HttpProxy.Get
```

### Sample of Python

`http_sample.py` demonstrates how to use **grequest** to achieve asynchronous read and write operations through POST method.
