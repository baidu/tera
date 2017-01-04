#!/usr/bin/env python
# encoding: utf-8

"""
sample of using Tera Python SDK
"""

from TeraSdk import Client, RowMutation, RowReader, TeraSdkException
from TeraSdk import MUTATION_CALLBACK, READER_CALLBACK, Status
import time


def mutation_callback(raw_mu):
    mu = RowMutation(raw_mu)
    status = mu.GetStatus()
    if status.GetReasonNumber() != Status.OK:
        print(status.GetReasonString())
    print "callback of rowkey:", mu.RowKey()
    mu.Destroy()
'''
用户需要确保回调执行时，write_callback仍然有效（例如没有因为过作用域被gc掉）
'''
write_callback = MUTATION_CALLBACK(mutation_callback)


def reader_callback(raw_reader):
    reader = RowReader(raw_reader)
    status = reader.GetStatus()
    if status.GetReasonNumber() != Status.OK:
        print(status.GetReasonString())
    while not reader.Done():
        row = reader.RowKey()
        column = reader.Family() + ":" + reader.Qualifier()
        timestamp = str(reader.Timestamp())
        val = reader.Value()
        print row + ":" + column + ":" + timestamp + ":" + val
        reader.Next()
    reader.Destroy()
'''
用户需要确保回调执行时，read_callback仍然有效（例如没有因为过作用域被gc掉）
'''
read_callback = READER_CALLBACK(reader_callback)


def main():
    """
    REQUIRES: tera.flag in current work directory; table `oops' was created
    """
    try:
        client = Client("./tera.flag", "pysdk")
    except TeraSdkException as e:
        print(e.reason)
        return
    try:
        table = client.OpenTable("oops")
    except TeraSdkException as e:
        print(e.reason)
        return

    # sync put
    sync_put(table)

    # sync get
    sync_get(table)

    # sync put batch
    sync_put_batch(table)

    # sync get batch
    sync_get_batch(table)

    # scan (stream)
    scan(table)

    # async put
    async_put(table)

    # async get
    async_get(table)

    table.Close()
    client.Close()
    print("main() done\n")


def sync_put(table):
    print("\nsync put")
    try:
        table.Put("sync", "cf0", "qu0", "value")
    except TeraSdkException as e:
        print(e.reason)


def sync_get(table):
    print("\nsync get")
    try:
        print(table.Get("sync", "cf0", "qu0", 0))
    except TeraSdkException as e:
        print(e.reason)
        if "not found" in e.reason:
            pass
        else:
            return


def sync_put_batch(table):
    print("\nsync put batch")
    mutation_list = list()
    for i in range(1, 1001):
        if i == 3:
            # valid rowkey should < 64K, otherwise an invalid argument
            # mock error for batch put
            mutation = table.NewRowMutation("k" * 64 * 1024 + "k")
        else:
            mutation = table.NewRowMutation(str(i))
        mutation.Put("cf0", "qu0", "value" + str(i))
        mutation_list.append(mutation)
    table.BatchPut(mutation_list)
    for m in mutation_list:
        status = m.GetStatus()
        if status.GetReasonNumber() != Status.OK:
            print("put/write failed for key<" + m.RowKey()
                  + "> due to:" + status.GetReasonString())
        m.Destroy()


def sync_get_batch(table):
    print("\nsync get batch")
    s1 = long(time.time() * 1000)
    reader_list = list()
    for i in range(1, 1001):
        reader = table.NewRowReader(str(i))
        # read column cf0:qu0
        reader.AddColumn("cf0", "qu0")
        reader_list.append(reader)
    table.BatchGet(reader_list)
    for r in reader_list:
        status = r.GetStatus()
        if status.GetReasonNumber() != Status.OK:
            print("get/read failed for key<" + r.RowKey()
                  + "> due to:" + status.GetReasonString())
        else:
            # print(r.Value())
            pass
        r.Destroy()
    e1 = long(time.time() * 1000)
    print(str(e1 - s1) + " ms")


def async_put(table):
    print("\nasync put")
    rowkey_list = ["async"]
    for key in rowkey_list:
        mu = table.NewRowMutation(key)
        mu.Put("cf0", "qu0", "value")
        mu.SetCallback(write_callback)
        table.ApplyMutation(mu)
    while not table.IsPutFinished():
        time.sleep(0.01)


def async_get(table):
    print("\nasync get")
    rowkey_list = ["async", "async_not_found"]
    for key in rowkey_list:
        reader = table.NewRowReader(key)
        reader.SetCallback(read_callback)
        table.ApplyReader(reader)
    while not table.IsGetFinished():
        time.sleep(0.01)


def put_get_int64(table, rowkey, cf, qu, value):
    try:
        table.PutInt64(rowkey, cf, qu, value)
        print("i got:" + str(table.GetInt64(rowkey, cf, qu, 0)))
    except TeraSdkException as e:
        print(e.reason)


def scan_with_filter(table):
    from TeraSdk import ScanDescriptor
    scan_desc = ScanDescriptor("")
    scan_desc.SetBufferSize(1024 * 1024)  # 1MB
    if not scan_desc.SetFilter("SELECT * WHERE int64 cf0 >= 0"):
        print("invalid filter")
        return
    try:
        stream = table.Scan(scan_desc)
    except TeraSdkException as e:
        print(e.reason)
        return

    while not stream.Done():
        row = stream.RowName()
        column = stream.ColumnName()
        timestamp = str(stream.Timestamp())
        val = stream.ValueInt64()
        print row + ":" + column + ":" + timestamp + ":" + str(val)
        stream.Next()


def scan(table):
    print("\nscan")
    from TeraSdk import ScanDescriptor
    scan_desc = ScanDescriptor("")
    scan_desc.SetBufferSize(1024 * 1024)  # 1MB
    try:
        stream = table.Scan(scan_desc)
    except TeraSdkException as e:
        print(e.reason)
        return

    while not stream.Done():
        row = stream.RowName()
        column = stream.ColumnName()
        timestamp = str(stream.Timestamp())
        val = stream.Value()
        print row + ":" + column + ":" + timestamp + ":" + val
        stream.Next()
    scan_desc.Destroy()
    stream.Destroy()


if __name__ == '__main__':
    main()
