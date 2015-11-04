"""
Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
"""


class Const:
    def __init__(self):
        self.tera_bench_binary = './tera_bench'
        self.tera_mark_binary = './tera_mark'
        self.teracli_binary = './teracli'
        self.kill_script = './kill_tera.sh'
        self.launch_script = './launch_tera.sh'
        self.launch_ts_first_script = './launch_ts_first.sh'
        self.data_path = 'testcase/data/'
        self.user_root_flag_path = './testcase/data/tera.flag.root'

const = Const()
