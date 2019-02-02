#compact优化——多线程compact
##背景

leveldb的compact策略是采用单线程执行的，顺序的读写逻辑减少了磁盘抖动，有利于发挥单机的机械磁盘的高效顺序读写性能。而类似rocksdb和tera的使用场景，底层直接使用ssd或使用ssd做本地cache或使用分布式文件系统，突破了机械磁盘的限制，在这种场景下，tera目前仍然使用levelbd的单线程模型做compact，业务的写热点很容易导致写入失败。因此本次实现多线程compact，充分利用底层存储带宽，提升compact性能，保证写分裂前，不会因为写热点导致写失败。

##实现
###自动compact的改造
	
1. 每个sst文件增加compact的互斥标记位，设置该位表示该sst正在被compact。具体添加位置是FileMetaData.being_compacted
2. PickCompaction选择sst进行compact时，会判断sst的being_compacted标记位，若正在被compact，则选下一个。
3. 将新产生的sst记入manifest时，LogAndApply函数采用类似Write的流程的互斥方式，保证多线程manifest文件是安全的。
4. 删除旧sst文件时，由于正在被compact的临时文件放到了pending_outputs_集合中被保护起来，所以需要先执行ReadDir获得所有临时文件，再将不在pending_outputs_集合中的临时文件删除，确保多线程时，不会误删其他compact线程的临时文件。
5. 打分机制的修改：维护每层的打分，level0的分数在文件个数较少时，采用个数/2；若文件个数较多时，采用sqrt(个数)/2；保证灌库时打压level0分数，非灌库时积极compact level0；
6. 调度compact前，仍选择最高分的自动compact任务进行compact，但在每次PickCompaction最后，会过滤being_compacted的sst，重新打分；compact完成或失败后也重新计算各层分数。
	
###flush memtable的改造

1. memtable增加互斥标记位，保证flush imm时，有且仅有一个线程在执行flush
	
###手动compact的改造

1. manual_compact描述结构增加互斥标记，保证手动compact时，有且仅有一个线程在执行手动compact。
	
##测试
###单测
单测采用leveldb的各种单元测试，make check
	
###集群测试
	
###性能测试
1. 单机测试使用db_bench
2. 集群测试采用使用tera_bench

