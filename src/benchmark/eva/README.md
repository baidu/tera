Tera指标体系
=====

本系统的目标在于给出Tera的性能指标，并且为开发者提供简单快速的测试框架。

## 配置及要求：
* 一个搭建好的Tera集群
* pyhton2.7 并在环境变量中添加PYTHONPATH=$HOME/{path}，其中path为指标系统的本目录

## 准备工作
请按以下方式将需要的文件拷贝到相应目录下：

  ```
  |--{path}
    |--bin
      |--teracli
      |--teramo
      |--tera_mark
      |--tera_bench
      |--tera.flag
      |--run.py
      |--eva_var.py
      |--eva_utils.py
      |--__init__.py
    |--conf
      |--tera.flag
    |--log
      |--eva
        |--(empty)
    |--tmp
  ```
其中，bin目录下为可执行文件，conf目录下为tera.flag，log目录下taracli, tera_mark以及teramo的log路径，log目录下的eva目录为指标系统的log路径。tmp目录下为tera_mark的输出文件以及指标系统产生的schema，deli以及指标结果文件。指标结果文件为html格式，可以通过机器邮件客户端发送到用户邮箱。

## 体验开始！
* 修改/src/benchmark/eva下的conf.sample文件：

  ```
  {
  "table_name": "test"      // 表名
  "read_speed_limit(/TS*Qps)": "0", // 读速度限制，写测试中设置为0即可
  "random": "f",            // 是否用随机种子产生随机key和value
  "value_sizeB(B)": "100",  // value大小
  "mode": "rw",             // 测试模式：sw为顺序写，rw为随机写，r为读，s为scan
  "scan_buffer(M)": "0",    // scan测试中的scan buffer
  "tablet_number": "60",    // 测试机群的tablet数量
  "ts_number": "3",         // 集群TS数量
  "key_size(B)": "100",     // key大小
  "write_speed_limit(/TS*M)": "60", // 写速度限制
  "entry_number(M)": "10",  // 每个tablet的数据条数
  "split_size": 102400,     // tablet分裂阈值
  "table_schema": {"lg0": {"storage": "flash", "cf": "cf0:q"}, "lg1": {"blocksize": 32, "storage": "disk", "cf": "cf1:q1,cf1:q2,cf2:q"}}
                            // table schema，设置"table_schema": {}为kv模式
}
  ```
  
* 开始测试
在{path}/bin目录下运行python run.py {conf}其中conf为测试配置文件路径

* 测试结束
测试结束后会生成一份报告，位于{path}/tmp目录下。
