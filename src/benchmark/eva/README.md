Tera指标体系
=====

本系统的目标在于给出Tera的性能指标，并且为开发者提供简单快速的测试框架。

## 配置及要求：
* 一个搭建好的Tera集群
* pyhton2.7 并在环境变量中添加PYTHONPATH=$HOME/{path}，其中path为指标系统的根目录

## 准备工作
请将`src/benchmark/eva`文件夹拷贝到`{path}`下，然后执行`./setup.sh`。请按照下面目录结构所示将`teracli`, `teramo`, `tera_mark`, `tera_bench`及`tera.flag`拷贝到相应位置。

  ```
  |--{path}
    |--setup.sh
    |--bin
      |--teracli
      |--teramo
      |--tera_mark
      |--tera_bench
      |--run.py
      |--eva_var.py
      |--eva_utils.py
      |--helper.py
      |--__init__.py
      |--run_sample.sh
    |--conf
      |--tera.flag
    |--log
      |--eva
        |--(empty)
    |--tmp
  ```
其中，`bin`目录下为可执行文件，`conf`目录下为`tera.flag`，`log`目录下为`taracli`, `tera_mark`以及`teramo`的`log`文件，`log/eva`目录为指标系统的`log`路径。`tmp`目录下为`tera_mark`的输出文件以及指标系统产生的`schema`，`deli`以及指标结果文件。指标结果文件为html格式，可以通过机器邮件客户端发送到用户邮箱。


## 体验开始！
* 修改`/src/benchmark/eva`下的`conf.sample`文件：

  ```
  {
  "table_name": "test",      // 表名
  "read_speed_limit(Qps)": "0", // 读速度限制，写测试中设置为0即可
  "key_seed": "10",             // 产生随机key值得随机种子
  "value_seed": "10",           // 产生随机value值得随机种子
  "key_size(B)": "100",     // key大小
  "value_sizeB(B)": "100",  // value大小
  "mode": "rw",             // 测试模式：sw为顺序写，rw为随机写，r为读，s为scan
  "scan_buffer(M)": "0",    // scan测试中的scan buffer
  "tablet_number": "60",    // 测试机群的tablet数量
  "ts_number": "3",         // 集群TS数量
  "write_speed_limit(M)": "60", // 写速度限制
  "entry_number(M)": "10",  // 每个tablet的数据条数
  "split_size": 102400,     // tablet分裂阈值
  "table_schema": {"lg0": {"storage": "flash", "cf": "cf0:q"}, "lg1": {"blocksize": "32", "storage": "disk", "cf": "cf1:q1,cf1:q2,cf2:q"}}
                            // table schema，设置"table_schema": {}为kv模式
}
  ```

* 开始测试

  Tips: 可以使用`helper.py`计算一下数据量并预测一下测试时长：
  ```
  > python helper.py my_conf_file
  >     estimated running time:   5h36m
        user data size:           5.39G
  ```

  在{path}/bin目录下运行python run.py {conf}其中conf为测试配置文件路径

* 测试结束

  测试结束后会生成一份报告，位于{path}/tmp目录下。
