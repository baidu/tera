Levledb for Tera
-----
Tera基于leveldb实现数据持久化存储，在原版leveldb的基础上开发了以下组件：
* DBTable，支持按列存储优化(Localitygroup)，通过封装多个DBImpl，并使用WAL&MVCC保证行级原子性读写。
* DFSEnv，支持分布式文件系统，可以对接HDFS，NFS等文件系统。
* FlashEnv，支持SSD盘缓存，将一个数据副本置于本地SSD，加速随机读，并使用DFS持久化数据；
* InMemoryEnv， 支持内存数据库，将一个副本置于本机内存中，加速读访问，并使用DFS持久化数据；
* Split&Merge，支持将一个leveldb分裂成两个或将两个leveldb合并成为一个，分裂合并只修改元数据，所以非常高效；
* ThreadPool，支持多个线程进行后台compact；
* MemTableOnLevelDB，支持LSM-tree版本的内存表，替代了跳表实现，解决表格展开为kv时，内存表性能不足的问题。
* AsyncWriter，支持在log写入阻塞的情况下，切换新的文件写入，降低写延时。
