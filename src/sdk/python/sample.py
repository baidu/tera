#!/usr/bin/env python

"""
sample of using Tera Python SDK
"""

from TeraSdk import Client, RowMutation, MUTATION_CALLBACK, TeraSdkException
import time


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
        table = client.OpenTable("oops2")
    except TeraSdkException as e:
        print(e.reason)
        return

    # sync put
    try:
        table.Put("row_sync", "cf0", "qu_sync", "value_sync")
    except TeraSdkException as e:
        print(e.reason)
        return

    # sync get
    try:
        print(table.Get("row_sync", "cf0", "qu_sync", 0))
    except TeraSdkException as e:
        print(e.reason)
        if "not found" in e.reason:
            pass
        else:
            return

    # scan (stream)
    scan(table)

    # async put
    mu = table.NewRowMutation("row_async")
    mu.Put("cf0", "qu_async", "value_async")
    mycallback = MUTATION_CALLBACK(my_mu_callback)
    mu.SetCallback(mycallback)

    table.ApplyMutation(mu)  # async

    while not table.IsPutFinished():
        time.sleep(0.01)

    print("main() done\n")


def my_mu_callback(raw_mu):
    mu = RowMutation(raw_mu)
    print "callback of rowkey:", mu.RowKey()


def scan(table):
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


if __name__ == '__main__':
    main()
