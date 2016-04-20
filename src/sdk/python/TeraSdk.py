# -*- coding: utf-8 -*-

"""
Tera Python SDK. It needs a libtera_c.so

TODO(taocipian) __init__.py
"""


from ctypes import CFUNCTYPE, POINTER
from ctypes import byref, cdll, string_at
from ctypes import c_bool, c_char_p, c_void_p
from ctypes import c_uint32, c_int32, c_int64, c_ubyte, c_uint64


class Status(object):
    # C++ tera.h ErrorCode
    OK = 0
    NotFound = 1
    BadParam = 2
    System = 3
    Timeout = 4
    Busy = 5
    NoQuota = 6
    NoAuth = 7
    Unknown = 8
    NotImpl = 9

    reason_list_ = ["ok", "not found", "bad parameter",
                    "unknown error", "request timeout", "busy",
                    "no quota", "operation not permitted", "unknown error",
                    "not implemented"]

    def __init__(self, c):
        self.c_ = c
        if c < 0 or c > len(Status.reason_list_)-1:
            self.reason_ = "bad status code"
        else:
            self.reason_ = Status.reason_list_[c]

    def GetReasonString(self):
        return Status.reason_list_[self.c_]

    def GetReasonNumber(self):
        return self.c_


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
        """
        服务端将读取的数据攒到buffer里，最多积攒到达buffer_size以后返回一次，
        也有可能因为超时或者读取到达终点而buffer没有满就返回，默认值 64 * 1024

        这个选项对scan性能有非常明显的影响，
        我们的测试显示，1024*1024(1MB)在很多场景下都有比较好的表现，
        建议根据自己的场景进行调优

        Args:
            buffer_size: scan操作buffer的size，单位Byte
        """
        lib.tera_scan_descriptor_set_buffer_size(self.desc, buffer_size)

    def SetIsAsync(self, is_async):
        """
        sdk内部启用并行scan操作，加快scan速率
        开启或者不开启，给用户的逻辑完全一样，默认不开启

        Args:
            is_async(bool): 是否启用并行scan
        """
        lib.tera_scan_descriptor_set_is_async(self.desc, is_async)

    def SetPackInterval(self, interval):
        """
        设置scan操作的超时时长，单位ms
        服务端在scan操作达到约 interval 毫秒后尽快返回给client结果

        Args:
            iinterval(long): 一次scan的超时时长，单位ms
        """
        lib.tera_scan_descriptor_set_pack_interval(self.desc, interval)

    def AddColumn(self, cf, qu):
        """
        scan时选择某个Column(ColumnFamily + Qualifier)，其它Column过滤掉不返回给客户端

        Args:
            cf(string): 需要的ColumnFamily名
            qu(string): 需要的Qualifier名
        """
        lib.tera_scan_descriptor_add_column(self.desc, cf,
                                            qu, c_uint64(len(qu)))

    def AddColumnFamily(self, cf):
        """
        类同 AddColumn, 这里选择整个 ColumnFamily

        Args:
            cf(string): 需要的ColumnFamily名
        """
        lib.tera_scan_descriptor_add_column_family(self.desc, cf)

    def IsAsync(self):
        """
        Returns:
            (bool) 当前scan操作是否为async方式
        """
        return lib.tera_scan_descriptor_is_async(self.desc)

    def SetTimeRange(self, start, end):
        """
        设置返回版本的时间范围
        C++接口用户注意：C++的这个接口里start和end参数的顺序和这里相反！

        Args:
            start(long): 开始时间戳（结果包含该值），Epoch (00:00:00 UTC, January 1, 1970), measured in us
            end(long): 截止时间戳（结果包含该值），Epoch (00:00:00 UTC, January 1, 1970), measured in us
        """
        lib.tera_scan_descriptor_set_time_range(self.desc, start, end)

    def SetFilter(self, filter_str):
        """
        设置过滤器（当前只支持比较初级的功能）

        Args:
            filter_str(string): 过滤字符串
        Returns:
            (bool) 返回True表示filter_str解析成功，支持这种过滤方式，否则表示解析失败
        """
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
        """
        Returns:
            (long) 当前cell为一个int64计数器，取出该计数器的数值
            对一个非int64计数器调用该方法，属未定义行为
        """
        return lib.tera_result_stream_value_int64(self.stream)

    def Timestamp(self):
        """
        Returns:
            (long) 当前cell对应的时间戳，Epoch (00:00:00 UTC, January 1, 1970), measured in us
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

    def GetStatus(self):
        """
        返回本次Mutation的结果状态

        Returns:
            (class Status) 操作结果状态，可以获知成功或失败，若失败，具体原因
        """
        return Status(lib.tera_row_mutation_get_status_code(self.mutation))

    def Destroy(self):
        """
        销毁这个mutation，释放底层资源，以后不得再使用这个对象
        """
        lib.tera_row_mutation_destroy(self.mutation)


class Table(object):
    """ 对表格的所有增删查改操作由此发起
        通过Client.OpenTable()获取一个Table对象
    """
    def __init__(self, table):
        self.table = table

    def NewRowMutation(self, rowkey):
        """ 生成一个对 rowkey 的RowMutation对象（修改一行）
        一个RowMutation对某一行的操作（例如多列修改）是原子的

        Args:
            rowkey(string): 待变更的rowkey
        Returns:
            (class RowMutation): RowMutation对象
        """
        return RowMutation(lib.tera_row_mutation(self.table, rowkey,
                                                 c_uint64(len(rowkey))))

    def ApplyMutation(self, mutation):
        """ 应用一次变更，
            如果之前调用过 SetCallback() 则本次调用为异步，否则为同步

        Args:
            mutation(class RowMutation): RowMutation对象
        """
        lib.tera_table_apply_mutation(self.table, mutation.mutation)

    def NewRowReader(self, rowkey):
        """ 生成一个对 rowkey 的RowReader对象（读取一行）
        一个RowReader对某一行的操作（例如读取多列）是原子的

        Args:
            rowkey(string): 待读取的rowkey
        Returns:
            (class RowReader): RowReader对象
        """
        return RowReader(lib.tera_row_reader(self.table, rowkey,
                                             c_uint64(len(rowkey))))

    def ApplyReader(self, reader):
        """ 应用一次读取，
            如果之前调用过 SetCallback() 则本次调用为异步，否则为同步

        Args:
            reader(class RowReader): RowReader对象
        """
        lib.tera_table_apply_reader(self.table, reader.reader)

    def IsPutFinished(self):
        """ table的异步写操作是否*全部*完成

        Returns:
            (bool) 全部完成则返回true，否则返回false.
        """
        return lib.tera_table_is_put_finished(self.table)

    def IsGetFinished(self):
        """ table的异步读操作是否*全部*完成

        Returns:
            (bool) 全部完成则返回true，否则返回false.
        """
        return lib.tera_table_is_get_finished(self.table)

    def Get(self, rowkey, cf, qu, snapshot):
        """ 同步get一个cell的值

        Args:
            rowkey(string): Rowkey的值
            cf(string): ColumnFamily名
            qu(string): Qualifier名
            snapshot(long): 快照，不关心的用户设置为0即可
        Returns:
            (string) cell的值
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
        """ 类同Get()接口，区别是将cell的内容作为int64计数器返回
            对非int64计数器的cell调用此方法属于未定义行为

        Args:
            rowkey(string): Rowkey的值
            cf(string): ColumnFamily名
            qu(string): Qualifier名
            snapshot(long): 快照，不关心的用户设置为0即可
        Returns:
            (long) cell的数值
        Raises:
            TeraSdkException: 读操作失败
        """
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
        """ 类同Put()方法，区别是这里的参数value可以是一个数字（能够用int64表示）计数器

        Args:
            rowkey(string): Rowkey的值
            cf(string): ColumnFamily名
            qu(string): Qualifier名
            value(long): cell的数值，能够用int64表示
        Raises:
            TeraSdkException: 写操作失败
        """
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


READER_CALLBACK = CFUNCTYPE(None, c_void_p)


class RowReader(object):
    """ 提供随机读取一行的功能
    """
    def __init__(self, reader):
        self.reader = reader

    def AddColumnFamily(self, cf):
        """ 添加期望读取的ColumnFamily
        默认读取一行(row)的全部ColumnFamily

        Args:
            cf(string): 期望读取的ColumnFamily
        """
        lib.tera_row_reader_add_column_family(self.reader, cf)

    def AddColumn(self, cf, qu):
        """ 添加期望读取的Column
        默认读取一行(row)的全部Column(ColumnFamily + Qualifier)

        Args:
            cf(string): 期望读取的ColumnFamily
            qu(string): 期望读取的Qualifier
        """
        lib.tera_row_reader_add_column(self.reader, cf, qu, c_uint64(len(qu)))

    def SetCallback(self, callback):
        """ 设置回调
        调用此函数则本次随机读为异步(Table.ApplyReader()立即返回)；
        否则本次随机读为同步(Table.ApplyReader()等待读取操作完成后返回)

        可以在回调中执行 Done() 和 Next() 对返回的结果进行迭代处理

        Args:
            callback(READER_CALLBACK): 用户回调，不论任何情况，最终都会被调用
        """
        lib.tera_row_reader_set_callback(self.reader, callback)

    def SetTimestamp(self, ts):
        lib.tera_row_reader_set_timestamp(self.reader, ts)

    def SetTimeRange(self, start, end):
        lib.tera_row_reader_set_time_range(self.reader, start, end)

    def SetSnapshot(self, snapshot):
        lib.tera_row_reader_set_snapshot(self.reader, snapshot)

    def SetMaxVersions(self, versions):
        lib.tera_row_reader_set_max_versions(self.reader, versions)

    def SetTimeout(self, timeout):
        lib.tera_row_reader_set_timeout(self.reader, timeout)

    def Done(self):
        """ 结果是否已经读完

        Returns:
            (bool) 如果已经读完，则返回 true， 否则返回 false.
        """
        return lib.tera_row_reader_done(self.reader)

    def Next(self):
        """ 迭代到下一个cell
        """
        lib.tera_row_reader_next(self.reader)

    def RowKey(self):
        """
        Returns:
            (string) 当前cell对应的rowkey
        """
        value = POINTER(c_ubyte)()
        vallen = c_uint64()
        lib.tera_row_reader_rowkey(self.reader,
                                   byref(value), byref(vallen))
        return copy_string_to_user(value, long(vallen.value))

    def Value(self):
        """
        Returns:
            (string) 当前cell对应的value
        """
        value = POINTER(c_ubyte)()
        vallen = c_uint64()
        lib.tera_row_reader_value(self.reader, byref(value), byref(vallen))
        return copy_string_to_user(value, long(vallen.value))

    def Family(self):
        """
        Returns:
            (string) 当前cell对应的ColumnFamily
        """
        value = POINTER(c_ubyte)()
        vallen = c_uint64()
        lib.tera_row_reader_family(self.reader, byref(value), byref(vallen))
        return copy_string_to_user(value, long(vallen.value))

    def Qualifier(self):
        """
        Returns:
            (string) 当前cell对应的Qulifier
        """
        value = POINTER(c_ubyte)()
        vallen = c_uint64()
        lib.tera_row_reader_qualifier(self.reader, byref(value), byref(vallen))
        return copy_string_to_user(value, long(vallen.value))

    def Timestamp(self):
        """
        Returns:
            (long) 当前cell对应的时间戳，Unix time
        """
        return lib.tera_row_reader_timestamp(self.reader)

    def GetStatus(self):
        """
        返回本次RowReader读取的结果状态

        Returns:
            (class Status) 操作结果状态，可以获知成功或失败，若失败，具体原因
        """
        return Status(lib.tera_row_reader_get_status_code(self.reader))

    def Destroy(self):
        """
        销毁这个mutation，释放底层资源，以后不得再使用这个对象
        """
        lib.tera_row_reader_destroy(self.reader)


class TeraSdkException(Exception):
    def __init__(self, reason):
        self.reason = reason

    def __str__(self):
        return self.reason


##########################
# 以下代码用户不需要关心 #
##########################

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

    lib.tera_table_scan.argtypes = [c_void_p, c_void_p, POINTER(c_char_p)]
    lib.tera_table_scan.restype = c_void_p

    lib.tera_table_delete.argtypes = [c_void_p, c_char_p, c_uint64,
                                      c_char_p, c_char_p, c_uint64]
    lib.tera_table_delete.restype = None

    lib.tera_table_apply_mutation.argtypes = [c_void_p, c_void_p]
    lib.tera_table_apply_mutation.restype = None

    lib.tera_table_is_put_finished.argtypes = [c_void_p]
    lib.tera_table_is_put_finished.restype = c_bool

    lib.tera_table_apply_reader.argtypes = [c_void_p]
    lib.tera_table_apply_reader.restype = None

    lib.tera_table_is_get_finished.argtypes = [c_void_p]
    lib.tera_table_is_get_finished.restype = c_bool

    lib.tera_row_mutation.argtypes = [c_void_p, c_char_p, c_uint64]
    lib.tera_row_mutation.restype = c_void_p

    lib.tera_row_mutation_get_status_code.argtypes = [c_void_p]
    lib.tera_row_mutation_get_status_code.restype = c_int64

    lib.tera_row_mutation_destroy.argtypes = [c_void_p]
    lib.tera_row_mutation_destroy.restype = None

    ##############
    # row_reader #
    ##############
    lib.tera_row_reader_add_column_family.argtypes = [c_void_p, c_char_p]
    lib.tera_row_reader_add_column_family.restype = None

    lib.tera_row_reader_add_column.argtypes = [c_void_p, c_char_p,
                                               c_void_p, c_uint64]
    lib.tera_row_reader_add_column.restype = None

    lib.tera_row_reader_set_callback.argtypes = [c_void_p, READER_CALLBACK]
    lib.tera_row_reader_set_callback.restype = None

    lib.tera_row_reader_set_timestamp.argtypes = [c_void_p, c_int64]
    lib.tera_row_reader_set_timestamp.restype = None

    lib.tera_row_reader_set_time_range.argtypes = [c_void_p, c_int64, c_int64]
    lib.tera_row_reader_set_time_range.restype = None

    lib.tera_row_reader_set_snapshot.argtypes = [c_void_p, c_uint64]
    lib.tera_row_reader_set_snapshot.restype = None

    lib.tera_row_reader_set_max_versions.argtypes = [c_void_p, c_uint32]
    lib.tera_row_reader_set_max_versions.restype = None

    lib.tera_row_reader_set_timeout.argtypes = [c_void_p, c_int64]
    lib.tera_row_reader_set_timeout.restype = None

    lib.tera_row_reader_done.argtypes = [c_void_p]
    lib.tera_row_reader_done.restype = c_bool

    lib.tera_row_reader_next.argtypes = [c_void_p]
    lib.tera_row_reader_next.restype = None

    lib.tera_row_reader_rowkey.argtypes = [c_void_p,
                                           POINTER(POINTER(c_ubyte)),
                                           POINTER(c_uint64)]
    lib.tera_row_reader_rowkey.restype = None

    lib.tera_row_reader_value.argtypes = [c_void_p,
                                          POINTER(POINTER(c_ubyte)),
                                          POINTER(c_uint64)]
    lib.tera_row_reader_value.restype = None

    lib.tera_row_reader_family.argtypes = [c_void_p,
                                           POINTER(POINTER(c_ubyte)),
                                           POINTER(c_uint64)]
    lib.tera_row_reader_family.restype = None

    lib.tera_row_reader_qualifier.argtypes = [c_void_p,
                                              POINTER(POINTER(c_ubyte)),
                                              POINTER(c_uint64)]
    lib.tera_row_reader_qualifier.restype = None

    lib.tera_row_reader_timestamp.argtypes = [c_void_p]
    lib.tera_row_reader_timestamp.restype = c_int64

    lib.tera_row_reader_get_status_code.argtypes = [c_void_p]
    lib.tera_row_reader_get_status_code.restype = c_int64

    lib.tera_row_reader_destroy.argtypes = [c_void_p]
    lib.tera_row_reader_destroy.restype = None


def copy_string_to_user(value, size):
    result = string_at(value, size)
    libc = cdll.LoadLibrary('libc.so.6')
    libc.free.argtypes = [c_void_p]
    libc.free.restype = None
    libc.free(value)
    return result


lib = cdll.LoadLibrary('./libtera_c.so')

init_function_prototype()
