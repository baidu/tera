#!/usr/bin/env python


from ctypes import POINTER
from ctypes import byref
from ctypes import c_bool
from ctypes import c_char_p
from ctypes import c_ubyte
from ctypes import c_uint64
from ctypes import c_int64
from ctypes import c_int32
from ctypes import c_void_p
from ctypes import cdll
from ctypes import string_at


# TODO(taocipian) __init__.py
def init_function_prototype():
    ######################
    # scan result stream #
    ######################
    lib.tera_result_stream_done.argtypes = [c_void_p,
                                            POINTER(c_char_p)]
    lib.tera_result_stream_done.restype = c_bool

    lib.tera_result_stream_look_up.argtypes = [c_void_p,
                                               POINTER(c_ubyte),
                                               c_uint64]
    lib.tera_result_stream_look_up.restype = c_bool

    lib.tera_result_stream_timestamp.argtypes = [c_void_p]
    lib.tera_result_stream_timestamp.restype = c_int64

    lib.tera_result_stream_column_name.argtypes = [c_void_p,
                                                   POINTER(POINTER(c_ubyte)),
                                                   POINTER(c_uint64)]
    lib.tera_result_stream_column_name.restype = None

    lib.tera_result_stream_family.argtypes = [c_void_p,
                                              POINTER(POINTER(c_ubyte)),
                                              POINTER(c_uint64)]
    lib.tera_result_stream_family.restype = None

    lib.tera_result_stream_next.argtypes = [c_void_p]
    lib.tera_result_stream_next.restype = None

    lib.tera_result_stream_qualifier.argtypes = [c_void_p,
                                                 POINTER(POINTER(c_ubyte)),
                                                 POINTER(c_uint64)]
    lib.tera_result_stream_qualifier.restype = None

    lib.tera_result_stream_row_name.argtypes = [c_void_p,
                                                POINTER(POINTER(c_ubyte)),
                                                POINTER(c_uint64)]
    lib.tera_result_stream_row_name.restype = None

    lib.tera_result_stream_value.argtypes = [c_void_p,
                                             POINTER(POINTER(c_ubyte)),
                                             POINTER(c_uint64)]
    lib.tera_result_stream_value.restype = None

    ###################
    # scan descriptor #
    ###################
    lib.tera_scan_descriptor.argtypes = [c_char_p, c_uint64]
    lib.tera_scan_descriptor.restype = c_void_p

    lib.tera_scan_descriptor_add_column.argtypes = [c_void_p, c_char_p,
                                                    c_void_p, c_uint64]
    lib.tera_scan_descriptor_add_column.restype = None

    lib.tera_scan_descriptor_add_column_family.argtypes = [c_void_p, c_char_p]
    lib.tera_scan_descriptor_add_column_family.restype = None

    lib.tera_scan_descriptor_is_async.argtypes = [c_void_p]
    lib.tera_scan_descriptor_is_async.restype = c_bool

    lib.tera_scan_descriptor_set_buffer_size.argtypes = [c_void_p, c_int64]
    lib.tera_scan_descriptor_set_buffer_size.restype = None

    lib.tera_scan_descriptor_set_end.argtypes = [c_void_p, c_void_p, c_uint64]
    lib.tera_scan_descriptor_set_end.restype = None

    lib.tera_scan_descriptor_set_filter_string.argtypes = [c_void_p, c_char_p]
    lib.tera_scan_descriptor_set_filter_string.restype = None

    lib.tera_scan_descriptor_set_pack_interval.argtypes = [c_char_p, c_int64]
    lib.tera_scan_descriptor_set_pack_interval.restype = None

    lib.tera_scan_descriptor_set_is_async.argtypes = [c_void_p, c_bool]
    lib.tera_scan_descriptor_set_is_async.restype = None

    lib.tera_scan_descriptor_set_max_versions.argtypes = [c_void_p, c_int32]
    lib.tera_scan_descriptor_set_max_versions.restype = None

    lib.tera_scan_descriptor_set_snapshot.argtypes = [c_void_p, c_uint64]
    lib.tera_scan_descriptor_set_snapshot.restype = None

    lib.tera_scan_descriptor_set_time_range.argtypes = [c_void_p,
                                                        c_int64, c_int64]
    lib.tera_scan_descriptor_set_time_range.restype = None

    ##########
    # client #
    ##########
    lib.tera_client_open.argtypes = [c_char_p, c_char_p, POINTER(c_char_p)]
    lib.tera_client_open.restype = c_void_p

    lib.tera_table_open.argtypes = [c_void_p, c_char_p, POINTER(c_char_p)]
    lib.tera_table_open.restyep = c_void_p

    #########
    # table #
    #########
    lib.tera_table_get.argtypes = [
        c_void_p, c_char_p, c_uint64,
        c_char_p, c_char_p, c_uint64,
        POINTER(POINTER(c_ubyte)), POINTER(c_uint64),
        POINTER(c_char_p), c_uint64]
    lib.tera_table_get.restype = c_bool

    lib.tera_table_put.argtypes = [
        c_void_p, c_char_p, c_uint64, c_char_p,
        c_char_p, c_uint64, c_char_p, c_uint64,
        POINTER(c_char_p)]
    lib.tera_table_put.restype = c_bool

    lib.tera_table_delete.argtypes = [
        c_void_p, c_char_p, c_uint64,
        c_char_p, c_char_p, c_uint64]
    lib.tera_table_delete.restype = None


class ScanDescriptor(object):
    def __init__(self, start_key):
        self.desc = lib.tera_scan_descriptor(start_key,
                                             c_uint64(len(start_key)))

    def SetEnd(self, end_key):
        lib.tera_scan_descriptor_set_end(self.desc, end_key,
                                         c_uint64(len(end_key)))

    def SetBufferSize(self, buffer_size):
        lib.tera_scan_descriptor_set_buffer_size(self.desc, buffer_size)

    def SetIsAsync(self, is_async):
        lib.tera_scan_descriptor_set_isasync(self.desc, is_async)

    def SetPackInterval(self, interval):
        lib.tera_scan_descriptor_set_pack_interval(self.desc, interval)

    def AddColumn(self, cf, qu):
        lib.tera_scan_descriptor_add_column(self.desc, cf,
                                            qu, c_uint64(len(qu)))

    def AddColumnFamily(self, cf):
        lib.tera_scan_descriptor_add_column_family(self.desc, cf)

    def IsAsync(self):
        return lib.tera_scan_descriptor_is_async(self.desc)

    def SetFilterString(self, filter_string):
        lib.tera_scan_descriptor_set_filter_string(self.desc, filter_string)

    def SetMaxVersions(self, versions):
        lib.tera_scan_descriptor_set_max_versions(self.desc, versions)

    def SetSnapshot(self, sid):
        lib.tera_scan_descriptor_set_snapshot(self.desc, sid)

    def SetTimeRange(self, start, end):
        lib.tera_scan_descriptor_set_time_range(self.desc, start, end)


class ResultStream(object):
    def __init__(self, stream):
        self.stream = stream

    def Done(self):
        err = c_char_p()
        return lib.tera_result_stream_done(self.stream, byref(err))

    def Next(self):
        lib.tera_result_stream_next(self.stream)

    def Value(self):
        value = POINTER(c_ubyte)()
        vallen = c_uint64()
        lib.tera_result_stream_value(self.stream, byref(value), byref(vallen))
        return string_at(value, long(vallen.value))

    def LookUp(self, string):
        return lib.tera_result_stream_look_up(self.stream,
                                              string, c_uint64(string))

    def Timestamp(self):
        return lib.tera_result_stream_timestamp(self.stream)

    def Qualifier(self):
        value = POINTER(c_ubyte)()
        vallen = c_uint64()
        lib.tera_result_stream_qualifier(self.stream,
                                         byref(value), byref(vallen))
        return string_at(value, long(vallen.value))

    def ColumnName(self):
        value = POINTER(c_ubyte)()
        vallen = c_uint64()
        lib.tera_result_stream_column_name(self.stream,
                                           byref(value), byref(vallen))
        return string_at(value, long(vallen.value))

    def Family(self):
        value = POINTER(c_ubyte)()
        vallen = c_uint64()
        lib.tera_result_stream_family(self.stream, byref(value), byref(vallen))
        return string_at(value, long(vallen.value))

    def RowName(self):
        value = POINTER(c_ubyte)()
        vallen = c_uint64()
        lib.tera_result_stream_row_name(self.stream,
                                        byref(value), byref(vallen))
        return string_at(value, long(vallen.value))


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


class Table(object):
    def __init__(self):
        pass

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

    def Scan(self, scan_descriptor):
        err = c_char_p()
        stream = lib.tera_table_scan(
            self.table,
            scan_descriptor.desc,
            byref(err)
        )
        if stream == NULL:
            raise TeraSdkException("scan failed:" + err.value)
        return ResultStream(stream)


class TeraSdkException(Exception):
    def __init__(self, reason):
        self.reason = reason

    def __str__(self):
        return self.reason


lib = cdll.LoadLibrary('./libtera_c.so')
NULL = 0

init_function_prototype()
