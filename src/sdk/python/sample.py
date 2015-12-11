#!/usr/bin/env python

"""
sample of using Tera Python SDK
"""

from TeraSdk import Client, RowMutation, MUTATION_CALLBACK
import time


def main():
    """
    REQUIRES: tera.flag in current work directory; table `oops' was created
    """
    client = Client("./tera.flag", "pysdk")
    table = client.OpenTable("oops")

    # sync put
    table.Put("row_sync", "cf0", "qu_sync", "value_sync")

    # sync get
    print(table.Get("row_sync", "cf0", "qu_sync", 0))

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
    stream = table.Scan(scan_desc)
    while not stream.Done():
        row = stream.RowName()
        column = stream.ColumnName()
        timestamp = str(stream.Timestamp())
        val = stream.Value()
        print row + ":" + column + ":" + timestamp + ":" + val
        stream.Next()


if __name__ == '__main__':
    main()
