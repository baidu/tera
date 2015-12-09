#!/usr/bin/env python

from ctypes import CFUNCTYPE
from ctypes import POINTER
from ctypes import byref
from ctypes import c_bool
from ctypes import c_char_p
from ctypes import c_ubyte
from ctypes import c_uint64
from ctypes import c_void_p
from ctypes import cdll
from ctypes import string_at

lib = cdll.LoadLibrary('./libtera_c.so')

lib.tera_client_open.argtypes = [c_char_p, c_char_p, POINTER(c_char_p)]
lib.tera_client_open.restype = c_void_p

lib.tera_table_open.argtypes = [c_void_p, c_char_p, POINTER(c_char_p)]
lib.tera_table_open.restyep = c_void_p

lib.tera_table_get.argtypes = [c_void_p, c_char_p, c_uint64,
                               c_char_p, c_char_p, c_uint64,
                               POINTER(POINTER(c_ubyte)), POINTER(c_uint64),
                               POINTER(c_char_p), c_uint64]
lib.tera_table_get.restype = c_bool

lib.tera_table_put.argtypes = [c_void_p, c_char_p, c_uint64, c_char_p,
                               c_char_p, c_uint64, c_char_p, c_uint64,
                               POINTER(c_char_p)]
lib.tera_table_put.restype = c_bool

lib.tera_table_delete.argtypes = [c_void_p, c_char_p, c_uint64,
                                  c_char_p, c_char_p, c_uint64]
lib.tera_table_delete.restype = None

NULL = 0


class Client(object):
    def __init__(self, conf_path, log_prefix):
        err = c_char_p()
        self.client = lib.tera_client_open(conf_path, log_prefix, byref(err))
        if self.client == NULL:
            raise TeraSdkException("open client failed:" + err.value)

    def OpenTable(self, name):
        err = c_char_p()
        table = Table()
        table.table = lib.tera_table_open(self.client, name, byref(err))
        if table.table == NULL:
            raise TeraSdkException("open table failed:" + err.value)
        return table

MUTATION_CALLBACK = CFUNCTYPE(None, c_void_p)

lib.tera_row_mutation_put.argtypes = [c_void_p, c_char_p,
                                      c_char_p, c_uint64,
                                      c_char_p, c_uint64]
lib.tera_row_mutation_put.restype = None


lib.tera_row_mutation.argtypes = [c_void_p, c_char_p, c_uint64]
lib.tera_row_mutation.restype = c_void_p

lib.tera_row_mutation_set_callback.argtypes = [c_void_p, MUTATION_CALLBACK]
lib.tera_row_mutation_set_callback.restype = None

lib.tera_row_mutation_delete_column.argtypes = [c_void_p, c_char_p,
                                                c_char_p, c_uint64]
lib.tera_row_mutation_delete_column.restype = None


class RowMutation(object):
    def __init__(self, mutation):
        self.mutation = mutation

    def Put(self, cf, qu, value):
        lib.tera_row_mutation_put(self.mutation, cf,
                                  qu, c_uint64(len(qu)),
                                  value, c_uint64(len(value)))

    def DeleteColumn(self, cf, qu):
        lib.tera_row_mutation_delete_column(self.mutation, cf,
                                            qu, c_uint64(len(qu)))

    def SetCallback(self, callback):
        lib.tera_row_mutation_set_callback(self.mutation, callback)

    def RowKey(self):
        value = POINTER(c_ubyte)()
        vallen = c_uint64()
        lib.tera_row_mutation_rowkey(self.mutation,
                                     byref(value), byref(vallen))
        return string_at(value, long(vallen.value))

lib.tera_table_apply_mutation.argtypes = [c_void_p, c_void_p]
lib.tera_table_apply_mutation.restype = None

lib.tera_table_is_put_finished.argtypes = [c_void_p]
lib.tera_table_is_put_finished.restype = c_bool


class Table(object):
    def __init__(self):
        pass

    def NewRowMutation(self, rowkey):
        return RowMutation(lib.tera_row_mutation(self.table, rowkey,
                                                 c_uint64(len(rowkey))))

    def ApplyMutation(self, mutation):
        return lib.tera_table_apply_mutation(self.table, mutation.mutation)

    def IsPutFinished(self):
        return lib.tera_table_is_put_finished(self.table)

    def Get(self, rowkey, cf, qu, snapshot):
        err = c_char_p()
        value = POINTER(c_ubyte)()
        vallen = c_uint64()
        result = lib.tera_table_get(
            self.table, rowkey, c_uint64(len(rowkey)), cf,
            qu, c_uint64(len(qu)), byref(value), byref(vallen), byref(err),
            c_uint64(snapshot)
        )
        if not result:
            raise TeraSdkException("get record failed:" + err.value)
        return string_at(value, long(vallen.value))

    def Put(self, rowkey, cf, qu, value):
        err = c_char_p()
        result = lib.tera_table_put(
            self.table, rowkey, c_uint64(len(rowkey)), cf,
            qu, c_uint64(len(qu)), value, c_uint64(len(value)), byref(err)
        )
        if not result:
            raise TeraSdkException("put record failed:" + err.value)

    def Delete(self, rowkey, cf, qu):
        lib.tera_table_delete(
            self.table, rowkey, c_uint64(len(rowkey)),
            cf, qu, c_uint64(len(qu))
        )


class TeraSdkException(Exception):
    def __init__(self, reason):
        self.reason = reason

    def __str__(self):
        return self.reason
