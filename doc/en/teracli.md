Teracli Manual
---

## Create Table

    ./teracli  create        <table-schema>  [<tablet-delimiter-file>]
    ./teracli  createbyfile  <schema-file>   [<tablet-delimiter-file>]

- Table-schema is a string used for describing the table schema. See[PropTree](https://github.com/BaiduPS/tera/blob/master/doc/prop_tree.md) for more details.

- Table name specification: the first character should be a letter (case insensitive); valid character includes alphanumeric character([a-z] and [A-Z]), digits([0-9]), underscores(`_`), dashes(`-`); table name's valid length should be [1, 512].

- Tera supports pre-sharding while creating table, key range describes in tablet-delimiter-file, while each range ends with '\n'

- User also can use `createbyfile` with a schema-file to create a table with complicated schema.

### Table Schema Mode

Table's schema includes table name, locality groups properties and column families descriptions, a typical schema describes as follow:

    # Tablet splitsize 4096M, mergesize 512M
    # There are three locality groups, configure as mem, flash and disk
    
    table_hello <splitsize=4096, mergesize=512> {
        lg_index <storage=flash, blocksize=4> {
            update_flag <maxversions=1>
        },
        lg_props <storage=flash, blocksize=32> {
            level,
            weight
        },
        lg_raw <storage=disk, blocksize=128> {
            data <maxversions=10>
        }
    }

If schema does not include locality group information, tera will create default lg in disk type, just like:

    table_hello {cf0, cf1, cf2}

### KV Schema Mode
Tera supports high performance kv table, its schema can only include table name or add some optional locality groups's properties.
	
	# kv mode
    kv_hello  
                                                  
    # kv mode with lg configuration
    kv_hello <storage=flash, splitsize=2048, mergesize=128> 

### Attributes and meanings

Span | Properties | Meanings | Legal value | Unit | Default value | Others
---  | ---    | ---  | ---      | ---  | ---    | ---
table | splitsize | If tablet's size has grown to exceed this value, the tablet is splited in two| >=0, when splitsize = 0, disable automatic split | MB | 512 |
table | mergesize | If tablet's size has reduced to this value, the adjacent  two tablet is merged in one | >=0, when mergesize = 0, disable automatic merge | MB | 0 | splitsize is at least 5 times larger than mergesize
lg    | storage   | storage type | "disk" / "flash" / "memory" | - | "disk" |
lg    | blocksize | Leveldb's block size       | >0 | KB | 4 |
lg    | use_memtable_on_leveldb | whether enable memory compact strategy| "true" / "false" | - | false |
lg    | sst_size  | sst's size in level0 | >0 | MB | 8 |
cf    | maxversions | max number of version keeps in tera  | >0 | - | 1 |
cf    | ttl | time-to-live | >=0, ttl=0 means never timeout | second | 0 | if ttl conflicts with minversions, minversions has the last word

<!--
table | rawkey | rawkey的拼装模式 | "binary" / "kv"/ "ttlkv" | - | key的长度必须小于64KB |
lg    | compress  | 压缩算法 | "snappy" / "none" | - | "snappy" |
lg    | memtable_ldb_write_buffer_size | 内存compact开启后，写buffer的大小 | >0 | MB | 1 | 一般不用暴露给用户
lg    | memtable_ldb_block_size |  内存compact开启后，压缩块的大小 | >0 | KB | 4 | 一般不用暴露给用户
cf    | diskquota   | 存储限额  | >0 | MB | 0 | 暂未使用
cf    | minversions | 保存的最小版本数 | >0 | - | 1 |
-->

## Update Schema

    ./teracli update <tableschema>

Update table syntax is the same as create. Update operation can apply to one or more properties at one time.

### Update table schema

1. When update locality group's properties, user need to disable table
2. Table and column family's properties support online update.

#### Usage example

Update table properties:

    ./teracli update "oops<mergesize=512>"
    ./teracli update "oops<splitsize=1024,mergesize=128>"

Update locality groups properties:

    ./teracli update "oops{lg0<sst_size=9>}"

    # update table properties, while update lg property
    ./teracli update "oops<splitsize=512>{lg0<sst_size=9>}"


Update column family properties:

    ./teracli update "oops{lg0{cf0<ttl=999>}}"

    #update table and lg property, while update cf property
    ./teracli update "oops<splitsize=512>{lg0<sst_size=9>{cf0<ttl=999>}}"

Online add/delete column family:

    # add cf1 to lg0, and set ttl = 123
    # op means operation, op=add need put in front of other cf's properties
    ./teracli update "oops{lg0{cf1<op=add,ttl=123>}}"

    # delete cf1 from lg0
    ./teracli update "oops{lg0{cf1<op=del>}}"

### Update kv schema

    # When update kv schema, user need to disable table
    ./teracli update "kvtable<splitsize=1024>"


## Disable Table
Set table into disable state; disable read and write service:

    ./teracli disable <tablename>

## Enable Table
Set a table into enable state; recovery read and write service:

    ./teracli enable <tablename>

## Drop Table
Delete a disable table, this operation could not rollback:

    ./teracli drop <tablename>

## Show Table
Show single table in formation:

    ./teracli show  <tablename>
    ./teracli showx <tablename> // for more details

Show all tables' information:

    ./teracli show
    ./teracli showx // for more details

## Show Schema
Show table schema, including properties of column family or locality group, etc.

	./teracli showschema <tablename>
	./teracli showschemax <tablename> // more details

## Show Tablet Server

Show single tablet server information as belove:

    ./teracli showts "example.company.com:7770"
    ./teracli showtsx "example.company.com:7770"  // more details

Show all tablet servers's information:

    ./teracli showts
    ./teracli showtsx // more details



## Put Operation
Use put to insert a key into table:
	
    ./teracli put <tablename> <rowkey> [<columnfamily:qualifier>] <value>
	
	# In kv mode, columnfamily:qualifier should not be set.
	./teracli put <tablename> <rowkey> <value>

For example:

    ./teracli put mytable rowkey cf0:qu0 value

## Put-ttl Operation

`put-ttl` is the same as `put`, except that key will be invalid after ttl:

    ./teracli put-ttl <tablename> <rowkey> [<columnfamily:qualifier>] <value> <ttl(second)>

For example:
	
	# key will be invalid after 20 seconds
    ./teracli put-ttl mytable rowkey cf0:qu0 value 20

## Putif Operation

`putif` is the same as `put`, except that putif will fail if key already in table; it works like std::map::insert():

    ./teracli putif <tablename> <rowkey> [<columnfamily:qualifier>] <value>

## Get Operation

Use get to read value from table:

    ./teracli get <tablename> <rowkey> [<columnfamily:qualifier>]

For example:

    ./teracli get mytable rowkey cf0:qu0

## Scan Operation
Use scan to get a sorted key ranged data from table:
	
	# all the key belonging to [start, end), will be scaned out
	./teracli scan <tablename> <startkey> <endkey>
	
	# scan all the versions of qualifier
	./teracli scanallv <tablename> <startkey> <endkey>
	

## Delete Operation
Delete a qualifier:
	
	./teracli delete <tablename> <rowkey> [<columnfamily:qualifier>]
	
	# delete the newest version of the qualifier
	./teracli delete1v <tablename> <rowkey> [<columnfamily:qualifier>]

## Support Int64

Tera support setting qualifier's type into int64, use putint64 for initializtion:

	# set cell of cf0:qu0 to be 67
    ./teracli putint64 mytable row1 cf0:qu0 67 
    
 Use getint64 to read counter from cell: 

    ./teracli getint64 mytable row1 cf0:qu0

 Use addint64 to increase value of cell; the value can be positive or negative:
 
    # add -3 into cell
    ./teracli addint64 mytable row1 cf0:qu0 -3

## Atomic Counter 

Tera supports distributed atomic counter, use put_counter to set a counter:

    ./teracli put_counter <tablename> <rowkey> [<columnfamily:qualifier>] <integer(int64_t)>

For example:

    ./teracli put_counter mytable rowkey cf0:qu0 3
    

Use get_counter to read counter from cell:

    ./teracli get_counter <tablename> <rowkey> [<columnfamily:qualifier>]

For example:

    ./teracli get_counter mytable rowkey cf0:qu0
    
Use add to increase counter's value:

    ./teracli add <tablename> <rowkey> <columnfamily:qualifier> delta


For example, set counter=3 and add 2 on it, then counter will be 5:

    ./teracli add mytable rowkey cf0:qu0 2
    
*Note*：User should apply `put_counter`, `get_counter`, or `add` operations to __atomic counter__, while use `putint64`, `getint64`, `addint64` to operate __int64 counter__. Both of these two counters cannot be confused.    

## Append Operation
Append operation's syntax describe as follow, it works like std::string::append(): 

	./teracli append <tablename> <rowkey> [<columnfamily:qualifier>] <value>

For example:

    # before append, rowkey:cf0:qu0 = 'hello'
    ./teracli put    mytalbe rowkey cf0:qu0 hello
    
    # exec append
    ./teracli append mytable rowkey cf0:qu0 world
    
    # after append, rowkey:cf0:qu0 = 'helloworld'
    ./teracli get mytable rowkey cf0:qu0

