# -*- coding: utf-8 -*-

"""
Tera Python SDK. It needs a libtera_c.so

TODO(taocipian) __init__.py
"""

from ctypes import CFUNCTYPE, POINTER
from ctypes import byref, cdll, string_at
from ctypes import c_bool, c_char_p, c_void_p
from ctypes import c_int32, c_int64, c_ubyte, c_uint64


class ScanDescriptor(object):
    """ scan操作描述符
        scan出[start_key, end_key)范围内的所有数据，每个cell默认返回最新的1个版本
    """

    def __init__(self, start_key):
        """
        Args:
            start_key(string): scan操作的起始位置，scan结果包含start_key
        """
        self.desc = lib.tera_scan_descriptor(start_key,
                                             c_uint64(len(start_key)))

    def SetEnd(self, end_key):
        """
        不调用此函数时，end_key被置为“无穷大”

        Args:
            end_key(string): scan操作的终止位置，scan结果不包含end_key
        """
        lib.tera_scan_descriptor_set_end(self.desc, end_key,
                                         c_uint64(len(end_key)))

    def SetMaxVersions(self, versions):
        """
        不调用此函数时，默认每个cell只scan出最新版本

        Args:
            versions(long): scan时某个cell最多被选出多少个版本
        """
        lib.tera_scan_descriptor_set_max_versions(self.desc, versions)

    def SetBufferSize(self, buffer_size):
        lib.tera_scan_descriptor_set_buffer_size(self.desc, buffer_size)

    def SetIsAsync(self, is_async):
        lib.tera_scan_descriptor_set_is_async(self.desc, is_async)

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

    def SetSnapshot(self, sid):
        lib.tera_scan_descriptor_set_snapshot(self.desc, sid)

    def SetTimeRange(self, start, end):
        lib.tera_scan_descriptor_set_time_range(self.desc, start, end)

    def SetFilter(self, filter_str):
        return lib.tera_scan_descriptor_set_filter(self.desc, filter_str)


class ResultStream(object):
    """ scan操作返回的输出流
    """

    def __init__(self, stream):
        self.stream = stream

    def Done(self):
        """ 此stream是否已经读完

        Returns:
            (bool) 如果已经读完，则返回 true， 否则返回 false.
        """
        err = c_char_p()
        return lib.tera_result_stream_done(self.stream, byref(err))

    def Next(self):
        """ 迭代到下一个cell
        """
        lib.tera_result_stream_next(self.stream)

    def RowName(self):
        """
        Returns:
            (string) 当前cell对应的Rowkey
        """
        value = POINTER(c_ubyte)()
        vallen = c_uint64()
        lib.tera_result_stream_row_name(self.stream,
                                        byref(value), byref(vallen))
        return copy_string_to_user(value, long(vallen.value))

    def Family(self):
        """
        Returns:
            (string) 当前cell对应的ColumnFamily
        """
        value = POINTER(c_ubyte)()
        vallen = c_uint64()
        lib.tera_result_stream_family(self.stream, byref(value), byref(vallen))
        return copy_string_to_user(value, long(vallen.value))

    def Qualifier(self):
        """
        Returns:
            (string) 当前cell对应的Qulifier
        """
        value = POINTER(c_ubyte)()
        vallen = c_uint64()
        lib.tera_result_stream_qualifier(self.stream,
                                         byref(value), byref(vallen))
        return copy_string_to_user(value, long(vallen.value))

    def ColumnName(self):
        """
        Returns:
            (string) 当前cell对应的 ColumnName(即 ColumnFamily:Qulifier)
        """
        value = POINTER(c_ubyte)()
        vallen = c_uint64()
        lib.tera_result_stream_column_name(self.stream,
                                           byref(value), byref(vallen))
        return copy_string_to_user(value, long(vallen.value))

    def Value(self):
        """
        Returns:
            (string) 当前cell对应的value
        """
        value = POINTER(c_ubyte)()
        vallen = c_uint64()
        lib.tera_result_stream_value(self.stream, byref(value), byref(vallen))
        return copy_string_to_user(value, long(vallen.value))

    def ValueInt64(self):
        return lib.tera_result_stream_value_int64(self.stream)

    def Timestamp(self):
        """
        Returns:
            (long) 当前cell对应的时间戳，Unix time
        """
        return lib.tera_result_stream_timestamp(self.stream)


class Client(object):
    """ 通过Client对象访问一个tera集群
    使用建议：一个集群对应一个Client即可，如需访问多个Client，需要创建多个
    """

    def __init__(self, conf_path, log_prefix):
        """
        Raises:
            TeraSdkException: 创建一个Client对象失败
        """
        err = c_char_p()
        self.client = lib.tera_client_open(conf_path, log_prefix, byref(err))
        if self.client is None:
            raise TeraSdkException("open client failed:" + str(err.value))

    def OpenTable(self, name):
        """ 打开名为<name>的表

        Args:
            name(string): 表名
        Returns:
            (Table) 打开的Table指针
        Raises:
            TeraSdkException: 打开table时出错
        """
        err = c_char_p()
        table_ptr = lib.tera_table_open(self.client, name, byref(err))
        if table_ptr is None:
            raise TeraSdkException("open table failed:" + err.value)
        return Table(table_ptr)


MUTATION_CALLBACK = CFUNCTYPE(None, c_void_p)


class RowMutation(object):
    """ 对某一行的变更

        在Table.ApplyMutation()调用之前，
        RowMutation的所有操作(如Put/DeleteColumn)都不会立即生效
    """

    def __init__(self, mutation):
        self.mutation = mutation

    def Put(self, cf, qu, value):
        """ 写入（修改）这一行上
            ColumnFamily为<cf>, Qualifier为<qu>的cell值为<value>

        Args:
            cf(string): ColumnFamily名
            qu(string): Qualifier名
            value(string): cell的值
        """
        lib.tera_row_mutation_put(self.mutation, cf,
                                  qu, c_uint64(len(qu)),
                                  value, c_uint64(len(value)))

    def DeleteColumn(self, cf, qu):
        """ 删除这一行上
            ColumnFamily为<cf>, Qualifier为<qu>的cell

        Args:
            cf(string): ColumnFamily名
            qu(string): Qualifier名
        """
        lib.tera_row_mutation_delete_column(self.mutation, cf,
                                            qu, c_uint64(len(qu)))

    def RowKey(self):
        """
        Returns:
            (string): 此RowMutation对象的rowkey，例如可用在回调中
        """
        value = POINTER(c_ubyte)()
        vallen = c_uint64()
        lib.tera_row_mutation_rowkey(self.mutation,
                                     byref(value), byref(vallen))
        return copy_string_to_user(value, long(vallen.value))

    def SetCallback(self, callback):
        """ 设置回调
        调用此函数则本次变更为异步(Table.ApplyMutation()立即返回)；
        否则本次变更为同步(Table.ApplyMutation()等待写入操作完成后返回)。

        Args:
            callback(MUTATION_CALLBACK): 用户回调，不论任何情况，最终都会被调用
        """
        lib.tera_row_mutation_set_callback(self.mutation, callback)


class Table(object):
    """ 对表格的所有增删查改操作由此发起
        通过Client.OpenTable()获取一个Table对象
    """
    def __init__(self, table):
        self.table = table

    def NewRowMutation(self, rowkey):
        """ 生成一个对 rowkey 的RowMutation对象
        Args:
            rowkey(string): 待变更的rowkey
        Returns:
            (RowMutation): RowMutation对象
        """
        return RowMutation(lib.tera_row_mutation(self.table, rowkey,
                                                 c_uint64(len(rowkey))))

    def ApplyMutation(self, mutation):
        """ 应用一次变更，
            如果之前调用过 SetCallback() 则本次调用为异步，否则为同步

        Args:
            mutation(RowMutation): RowMutation对象
        """
        lib.tera_table_apply_mutation(self.table, mutation.mutation)

    def IsPutFinished(self):
        """ table的异步写操作是否*全部*完成

        Returns:
            (bool) 全部完成则返回true，否则返回false.
        """
        return lib.tera_table_is_put_finished(self.table)

    def Get(self, rowkey, cf, qu, snapshot):
        """ 同步get一个cell的值

        Args:
            rowkey(string): Rowkey的值
            cf(string): ColumnFamily名
            qu(string): Qualifier名
            snapshot(long): 快照，不关心的用户设置为0即可
        Raises:
            TeraSdkException: 读操作失败
        """
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
        return copy_string_to_user(value, long(vallen.value))

    def GetInt64(self, rowkey, cf, qu, snapshot):
        err = c_char_p()
        value = c_int64()
        result = lib.tera_table_getint64(
            self.table, rowkey, c_uint64(len(rowkey)), cf,
            qu, c_uint64(len(qu)), byref(value), byref(err),
            c_uint64(snapshot)
        )
        if not result:
            raise TeraSdkException("get record failed:" + err.value)
        return long(value.value)

    def Put(self, rowkey, cf, qu, value):
        """ 同步put一个cell的值

        Args:
            rowkey(string): Rowkey的值
            cf(string): ColumnFamily名
            qu(string): Qualifier名
            value(string): cell的值
        Raises:
            TeraSdkException: 写操作失败
        """
        err = c_char_p()
        result = lib.tera_table_put(
            self.table, rowkey, c_uint64(len(rowkey)), cf,
            qu, c_uint64(len(qu)), value, c_uint64(len(value)), byref(err)
        )
        if not result:
            raise TeraSdkException("put record failed:" + err.value)

    def PutInt64(self, rowkey, cf, qu, value):
        err = c_char_p()
        result = lib.tera_table_putint64(
            self.table, rowkey, c_uint64(len(rowkey)), cf,
            qu, c_uint64(len(qu)), value, byref(err)
        )
        if not result:
            raise TeraSdkException("put record failed:" + err.value)

    def Delete(self, rowkey, cf, qu):
        """ 同步删除某个cell

        Args:
            rowkey(string): Rowkey的值
            cf(string): ColumnFamily名
            qu(string): Qualifier名
        """
        lib.tera_table_delete(
            self.table, rowkey, c_uint64(len(rowkey)),
            cf, qu, c_uint64(len(qu))
        )

    def Scan(self, desc):
        """ 发起一次scan操作

        Args:
            desc(ScanDescriptor): scan操作描述符
        Raises:
            TeraSdkException: scan失败
        """
        err = c_char_p()
        stream = lib.tera_table_scan(
            self.table,
            desc.desc,
            byref(err)
        )
        if stream is None:
            raise TeraSdkException("scan failed:" + err.value)
        return ResultStream(stream)


class TeraSdkException(Exception):
    def __init__(self, reason):
        self.reason = reason

    def __str__(self):
        return self.reason


def init_function_prototype():
    ######################
    # scan result stream #
    ######################
    lib.tera_result_stream_done.argtypes = [c_void_p,
                                            POINTER(c_char_p)]
    lib.tera_result_stream_done.restype = c_bool

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

    lib.tera_result_stream_value_int64.argtypes = [c_void_p]
    lib.tera_result_stream_value_int64.restype = c_int64

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

    lib.tera_scan_descriptor_set_filter.argtypes = [c_void_p, c_char_p]
    lib.tera_scan_descriptor_set_filter.restype = c_bool

    ##########
    # client #
    ##########
    lib.tera_client_open.argtypes = [c_char_p, c_char_p, POINTER(c_char_p)]
    lib.tera_client_open.restype = c_void_p

    lib.tera_table_open.argtypes = [c_void_p, c_char_p, POINTER(c_char_p)]
    lib.tera_table_open.restype = c_void_p

    ################
    # row_mutation #
    ################
    lib.tera_row_mutation_put.argtypes = [c_void_p, c_char_p,
                                          c_char_p, c_uint64,
                                          c_char_p, c_uint64]
    lib.tera_row_mutation_put.restype = None

    lib.tera_row_mutation_set_callback.argtypes = [c_void_p, MUTATION_CALLBACK]
    lib.tera_row_mutation_set_callback.restype = None

    lib.tera_row_mutation_delete_column.argtypes = [c_void_p, c_char_p,
                                                    c_char_p, c_uint64]
    lib.tera_row_mutation_delete_column.restype = None

    lib.tera_row_mutation_rowkey.argtypes = [c_void_p,
                                             POINTER(POINTER(c_ubyte)),
                                             POINTER(c_uint64)]
    lib.tera_row_mutation_rowkey.restype = None

    #########
    # table #
    #########
    lib.tera_table_get.argtypes = [c_void_p, c_char_p, c_uint64,
                                   c_char_p, c_char_p, c_uint64,
                                   POINTER(POINTER(c_ubyte)),
                                   POINTER(c_uint64),
                                   POINTER(c_char_p), c_uint64]
    lib.tera_table_get.restype = c_bool

    lib.tera_table_getint64.argtypes = [c_void_p, c_char_p, c_uint64,
                                        c_char_p, c_char_p, c_uint64,
                                        POINTER(c_int64), POINTER(c_char_p),
                                        c_uint64]
    lib.tera_table_getint64.restype = c_bool

    lib.tera_table_put.argtypes = [c_void_p, c_char_p, c_uint64, c_char_p,
                                   c_char_p, c_uint64, c_char_p, c_uint64,
                                   POINTER(c_char_p)]
    lib.tera_table_put.restype = c_bool

    lib.tera_table_putint64.argtypes = [c_void_p, c_char_p, c_uint64, c_char_p,
                                        c_char_p, c_uint64, c_int64,
                                        POINTER(c_char_p)]
    lib.tera_table_putint64.restype = c_bool

    lib.tera_table_delete.argtypes = [c_void_p, c_char_p, c_uint64,
                                      c_char_p, c_char_p, c_uint64]
    lib.tera_table_delete.restype = None

    lib.tera_table_apply_mutation.argtypes = [c_void_p, c_void_p]
    lib.tera_table_apply_mutation.restype = None

    lib.tera_table_is_put_finished.argtypes = [c_void_p]
    lib.tera_table_is_put_finished.restype = c_bool

    lib.tera_row_mutation.argtypes = [c_void_p, c_char_p, c_uint64]
    lib.tera_row_mutation.restype = c_void_p


def copy_string_to_user(value, size):
    result = string_at(value, size)
    libc = cdll.LoadLibrary('libc.so.6')
    libc.free.argtypes = [c_void_p]
    libc.free.restype = None
    libc.free(value)
    return result


lib = cdll.LoadLibrary('./libtera_c.so')

init_function_prototype()
