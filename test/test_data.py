import subprocess
import filecmp
import os
import nose

tera_bench_binary = './tera_bench'
tera_mark_binary = './tera_mark'
teracli_binary = './teracli'


def create_kv_table():
    """
    create kv table
    :return:
    """
    cleanup()
    ret = subprocess.Popen('./teracli create test', stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    print ''.join(ret.stdout.readlines())


def create_table():
    cleanup()
    ret = subprocess.Popen('./teracli create "test{cf0<maxversions=20>, cf1<maxversions=20>}"',
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    print ''.join(ret.stdout.readlines())


def cleanup():
    ret = subprocess.Popen('./teracli disable test', stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    print ''.join(ret.stdout.readlines())
    ret = subprocess.Popen('./teracli drop test', stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    print ''.join(ret.stdout.readlines())

    files = os.listdir('.')
    for f in files:
        if f.endswith('.out'):
            os.remove(f)


def run_tera_mark(file_path, op, table_name, random, value_size, num, key_size, cf='', key_seed=1, value_seed=1):
    """
    This function provide means to write data into Tera and dump a copy into a specified file at the same time.
    :param file_path: a copy of data will be dumped into file_path for future use
    :param op: ['w' | 'd'], 'w' indicates write and 'd' indicates delete
    :param table_name: table name
    :param random: ['random' | 'seq']
    :param value_size: value size in Bytes
    :param num: entry number
    :param key_size: key size in Bytes
    :param is_append: append the data to an exists file
    :param cf: cf list, e.g. 'cf0:qual,cf1:flag'. Empty cf list for kv mode. Notice: no space in between
    :param key_seed: seed for random key generator
    :param value_seed: seed for random value generator
    :return: None
    """
    
    # write data into Tera
    tera_bench_args = ""
    awk_args = ""

    if cf == '':  # kv mode
        tera_bench_args += """--compression_ratio=1 --key_seed={kseed} --value_seed={vseed} """\
                           """ --value_size={vsize} --num={num} --benchmarks={random} """\
                           """ --key_size={ksize} """.format(kseed=key_seed, vseed=value_seed,
                                                             vsize=value_size, num=num, random=random, ksize=key_size)
        if op == 'd':  # delete
            awk_args += """-F '\t' '{print $1}'"""
        else:  # write
            awk_args += """-F '\t' '{print $1"\t"$2}'"""
    else:  # table
        tera_bench_args += """--cf={cf} --compression_ratio=1 --key_seed={kseed} --value_seed={vseed} """\
                           """ --value_size={vsize} --num={num} --benchmarks={random} """\
                           """ --key_size={ksize} """.format(cf=cf, kseed=key_seed, vseed=value_seed,
                                                             vsize=value_size, num=num, random=random, ksize=key_size)
        if op == 'd':  # delete
            awk_args += """-F '\t' '{print $1"\t"$3"\t"$4}'"""
        else:  # write
            awk_args += """-F '\t' '{print $1"\t"$2"\t"$3"\t"$4}'"""

    tera_mark_args = """--mode={op} --tablename={table_name} --type=async """\
                     """ --verify=false""".format(op=op, table_name=table_name)

    cmd = '{tera_bench} {bench_args} | awk {awk_args} | {tera_mark} {mark_args}'.format(
        tera_bench=tera_bench_binary, bench_args=tera_bench_args, awk_args=awk_args,
        tera_mark=tera_mark_binary, mark_args=tera_mark_args)
    
    print cmd
    ret = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    print ''.join(ret.stdout.readlines())

    # write/append data to a file for comparison
    for path, is_append in file_path:
        if cf == '':
            awk_args = """-F '\t' '{print $1"::0:"$2}'"""
        else:
            awk_args = """-F '\t' '{print $1":"$3":"$4":"$2}'"""

        redirect_op = ''
        if is_append is True:
            redirect_op += '>>'
        else:
            redirect_op += '>'

        dump_cmd = '{tera_bench} {tera_bench_args} | awk {awk_args} {redirect_op} {out}'.format(
            tera_bench=tera_bench_binary, tera_bench_args=tera_bench_args,
            redirect_op=redirect_op, awk_args=awk_args, out=path)
        print dump_cmd
        ret = subprocess.Popen(dump_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        print ''.join(ret.stdout.readlines())


def scan_table(table_name, file_path, allversion, snapshot=0):
    """
    This function scans the table and write the output into file_path
    :param table_name: table name
    :param file_path: write scan output into file_path
    :param allversion: [True | False]
    """

    allv = ''
    if allversion is True:
        allv += 'scanallv'
    else:
        allv += 'scan'

    snapshot_args = ''
    if snapshot != 0:
        snapshot_args += '--snapshot={snapshot}'.format(snapshot=snapshot)

    scan_cmd = '{teracli} {op} {table_name} "" "" {snapshot}> {out}'.format(
        teracli=teracli_binary, op=allv, table_name=table_name, snapshot=snapshot_args, out=file_path)
    print scan_cmd
    ret = subprocess.Popen(scan_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    print ''.join(ret.stdout.readlines())


def get_tablet_list(table_name):
    # TODO: need a more elegant & general way to obtain tablet info
    show_cmd = '{teracli} show {table}'.format(teracli=teracli_binary, table=table_name)
    print show_cmd
    ret = subprocess.Popen(show_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    tablet_info = ret.stdout.readlines()[5:]  # tablet info starts from the 6th line
    tablet_info = filter(lambda x: x != '\n', tablet_info)
    tablet_paths = []
    for tablet in tablet_info:
        comp = filter(None, tablet.split(' '))
        tablet_paths.append(comp[2])
    return tablet_paths


def compact_tablets(tablet_list):
    # TODO: compact may timeout
    for tablet in tablet_list:
        compact_cmd = '{teracli} tablet compact {tablet}'.format(teracli=teracli_binary, tablet=tablet)
        print compact_cmd
        ret = subprocess.Popen(compact_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        print ''.join(ret.stdout.readlines())


def snapshot_op(table_name):
    """
    This function creates | deletes a snapshot
    :param table_name: table name
    :return: snapshot id on success, None otherwise
    """
    # TODO: delete snapshot
    snapshot_cmd = '{teracli} snapshot {table_name} create'.format(teracli=teracli_binary, table_name=table_name)
    print snapshot_cmd
    ret = subprocess.Popen(snapshot_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    out = ret.stdout.readlines()
    ret = ''
    try:
        ret += out[1]
    except IndexError:
        return None

    if ret.startswith('new snapshot: '):
        snapshot_id = ret[len('new snapshot: '):-1]
        if snapshot_id.isdigit():
            return int(snapshot_id)
    return None


def compare_files(file1, file2, need_sort):
    """
    This function compares two files.
    :param file1: file path to the first file
    :param file2: file path to the second file
    :param need_sort: whether the files need to be sorted
    :return: True if the files are the same, False on the other hand
    """
    if need_sort is True:
        sort_cmd = 'sort {f1} > {f1}.sort; sort {f2} > {f2}.sort'.format(f1=file1, f2=file2)
        print sort_cmd
        ret = subprocess.Popen(sort_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        print ''.join(ret.stdout.readlines())
        os.rename(file1+'.sort', file1)
        os.rename(file2+'.sort', file2)
    return filecmp.cmp(file1, file2)


def file_is_empty(file_path):
    """
    This function test whether a file is empty
    :param file_path: file path
    :return: True if the file is empty, False on the other hand
    """
    return not os.path.getsize(file_path)


def cleanup_files(file_list):
    for file_path in file_list:
        os.remove(file_path)


@nose.tools.with_setup(create_kv_table, cleanup)
def test_kv_random_write():
    """
    kv table write
    1. write data set 1
    2. scan & compare
    :return: None
    """
    table_name = 'test'
    dump_file = 'dump.out'
    scan_file = 'scan.out'
    run_tera_mark([(dump_file, False)], op='w', table_name='test', random='random',
                  value_size=100, num=5000, key_size=20)
    scan_table(table_name=table_name, file_path=scan_file, allversion=True)
    nose.tools.assert_true(compare_files(dump_file, scan_file, need_sort=True))


@nose.tools.with_setup(create_table, cleanup)
def test_table_random_write():
    """
    table write simple
    1. write data set 1
    2. scan & compare
    :return: None
    """
    table_name = 'test'
    dump_file = 'dump.out'
    scan_file = 'scan.out'
    run_tera_mark([(dump_file, False)], op='w', table_name=table_name, cf='cf0:q,cf1:q', random='random',
                  key_seed=1, value_seed=10, value_size=100, num=10000, key_size=20)
    scan_table(table_name=table_name, file_path=scan_file, allversion=False)
    nose.tools.assert_true(compare_files(dump_file, scan_file, need_sort=True))


@nose.tools.with_setup(create_table, cleanup)
def test_table_random_write_versions():
    """
    table write w/versions
    1. write data set 1
    2. write data set 2
    3. scan & compare
    :return: None
    """
    table_name = 'test'
    dump_file = 'dump.out'
    scan_file = 'scan.out'
    run_tera_mark([(dump_file, False)], op='w', table_name=table_name, cf='cf0:q,cf1:q', random='random',
                  key_seed=1, value_seed=10, value_size=100, num=10000, key_size=20)
    run_tera_mark([(dump_file, True)], op='w', table_name=table_name, cf='cf0:q,cf1:q', random='random',
                  key_seed=1, value_seed=11, value_size=100, num=10000, key_size=20)
    scan_table(table_name=table_name, file_path=scan_file, allversion=True)
    nose.tools.assert_true(compare_files(dump_file, scan_file, need_sort=True))


@nose.tools.with_setup(create_table, cleanup)
def test_table_write_delete():
    """
    table write and deletion
    1. write data set 1
    2. delete data set 1
    3. scan & compare
    :return: None
    """
    table_name = 'test'
    scan_file = 'scan.out'
    run_tera_mark([], op='w', table_name=table_name, cf='cf0:q,cf1:q', random='random',
                  key_seed=1, value_seed=1, value_size=100, num=10000, key_size=20)
    run_tera_mark([], op='d', table_name=table_name, cf='cf0:q,cf1:q', random='random',
                  key_seed=1, value_seed=1, value_size=100, num=10000, key_size=20)
    scan_table(table_name=table_name, file_path=scan_file, allversion=True)
    nose.tools.assert_true(file_is_empty(scan_file))


@nose.tools.with_setup(create_table, cleanup)
def test_table_write_delete_version():
    """
    table write and deletion w/versions
    1. write data set 1, 2, 3, 4
    2. scan
    3. delete data set 3
    4. scan & compare
    :return: None
    """
    table_name = 'test'
    dump_file1 = 'dump1.out'
    dump_file2 = 'dump2.out'
    scan_file1 = 'scan1.out'
    scan_file2 = 'scan2.out'
    run_tera_mark([(dump_file1, False), (dump_file2, False)], op='w', table_name=table_name, cf='cf0:q,cf1:q', random='random',
                  key_seed=1, value_seed=10, value_size=100, num=10000, key_size=20)
    run_tera_mark([(dump_file1, True), (dump_file2, True)], op='w', table_name=table_name, cf='cf0:q,cf1:q', random='random',
                  key_seed=1, value_seed=11, value_size=100, num=10000, key_size=20)
    run_tera_mark([(dump_file1, True)], op='w', table_name=table_name, cf='cf0:q,cf1:q', random='random',
                  key_seed=1, value_seed=12, value_size=100, num=10000, key_size=20)
    run_tera_mark([(dump_file1, True), (dump_file2, True)], op='w', table_name=table_name, cf='cf0:q,cf1:q', random='random',
                  key_seed=1, value_seed=13, value_size=100, num=10000, key_size=20)
    compact_tablets(get_tablet_list(table_name))
    scan_table(table_name=table_name, file_path=scan_file1, allversion=True, snapshot=0)
    run_tera_mark([], op='d', table_name=table_name, cf='cf0:q,cf1:q', random='random',
                  key_seed=1, value_seed=12, value_size=100, num=10000, key_size=20)
    compact_tablets(get_tablet_list(table_name))
    scan_table(table_name=table_name, file_path=scan_file2, allversion=True, snapshot=0)
    nose.tools.assert_true(compare_files(dump_file1, scan_file1, need_sort=True))
    nose.tools.assert_true(compare_files(dump_file2, scan_file2, need_sort=True))


@nose.tools.with_setup(create_table, cleanup)
def test_table_write_snapshot():
    """
    table write w/snapshot
    1. write data set 1
    2. take snapshot
    3. write data set 2
    4. scan w/snapshot, scan w/o snapshot & compare
    :return: None
    """
    table_name = 'test'
    dump_file1 = 'dump1.out'
    dump_file2 = 'dump2.out'
    scan_file1 = 'scan1.out'
    scan_file2 = 'scan2.out'
    run_tera_mark([(dump_file1, False)], op='w', table_name=table_name, cf='cf0:q,cf1:q', random='random',
                  key_seed=1, value_seed=10, value_size=100, num=10000, key_size=20)
    snapshot = snapshot_op(table_name)
    run_tera_mark([(dump_file2, False)], op='w', table_name=table_name, cf='cf0:q,cf1:q', random='random',
                  key_seed=1, value_seed=11, value_size=100, num=10000, key_size=20)
    compact_tablets(get_tablet_list(table_name))
    scan_table(table_name=table_name, file_path=scan_file1, allversion=False, snapshot=snapshot)
    scan_table(table_name=table_name, file_path=scan_file2, allversion=False, snapshot=0)
    nose.tools.assert_true(compare_files(dump_file1, scan_file1, need_sort=True))
    nose.tools.assert_true(compare_files(dump_file2, scan_file2, need_sort=True))


@nose.tools.with_setup(create_table, cleanup)
def test_table_write_del_snapshot():
    """
    table write deletion w/snapshot
    1. write data set 1
    2. take snapshot
    3. delete data set 1
    4. scan w/snapshot, scan w/o snapshot & compare
    :return: None
    """
    table_name = 'test'
    dump_file = 'dump.out'
    scan_file1 = 'scan1.out'
    scan_file2 = 'scan2.out'
    run_tera_mark([(dump_file, False)], op='w', table_name=table_name, cf='cf0:q,cf1:q', random='random',
                  key_seed=1, value_seed=10, value_size=100, num=10000, key_size=20)
    snapshot = snapshot_op(table_name)
    run_tera_mark([], op='d', table_name=table_name, cf='cf0:q,cf1:q', random='random',
                  key_seed=1, value_seed=11, value_size=100, num=10000, key_size=20)
    compact_tablets(get_tablet_list(table_name))
    scan_table(table_name=table_name, file_path=scan_file1, allversion=False, snapshot=snapshot)
    scan_table(table_name=table_name, file_path=scan_file2, allversion=False, snapshot=0)
    nose.tools.assert_true(compare_files(dump_file, scan_file1, need_sort=True))
    nose.tools.assert_true(file_is_empty(scan_file2))


@nose.tools.with_setup(create_table, cleanup)
def test_table_write_multiversion_snapshot():
    """
    table write w/version w/snapshot
    1. write data set 1, 2
    2. take snapshot
    3. write data set 3, 4
    4. scan w/snapshot, scan w/o snapshot & compare
    :return: None
    """
    table_name = 'test'
    dump_file1 = 'dump1.out'
    dump_file2 = 'dump2.out'
    scan_file1 = 'scan1.out'
    scan_file2 = 'scan2.out'
    run_tera_mark([(dump_file1, False)], op='w', table_name=table_name, cf='cf0:q,cf1:q', random='random',
                  key_seed=1, value_seed=10, value_size=100, num=10000, key_size=20)
    run_tera_mark([(dump_file1, True)], op='w', table_name=table_name, cf='cf0:q,cf1:q', random='random',
                  key_seed=1, value_seed=11, value_size=100, num=10000, key_size=20)
    snapshot = snapshot_op(table_name)
    run_tera_mark([(dump_file2, False)], op='w', table_name=table_name, cf='cf0:q,cf1:q', random='random',
                  key_seed=1, value_seed=10, value_size=100, num=10000, key_size=20)
    run_tera_mark([(dump_file2, True)], op='w', table_name=table_name, cf='cf0:q,cf1:q', random='random',
                  key_seed=1, value_seed=11, value_size=100, num=10000, key_size=20)
    compact_tablets(get_tablet_list(table_name))
    scan_table(table_name=table_name, file_path=scan_file1, allversion=True, snapshot=snapshot)
    scan_table(table_name=table_name, file_path=scan_file2, allversion=True, snapshot=0)
    nose.tools.assert_true(compare_files(dump_file1, scan_file1, need_sort=True))
    nose.tools.assert_true(compare_files(dump_file2, scan_file2, need_sort=True))


'''
def main():
    test_kv_random_write()


if __name__ == '__main__':
    main()
'''
