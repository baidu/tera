# Create

1. Client收到命令后，检查参数是否合法。如果参数合法，client向master发起Create命令，否则返回失败。
2. Master收到命令后，检查参数是否合法。如果参数合法,更新内存meta信息，随后发起写meta操作更新meta表。
3. 如果写meta操作失败且重试超过指定次数，返回失败。如果写meta操作成功，向用户返回成功。
4. Master发起Load命令，load本次Create操作新创建的tablet(s)。

# Disable

1. Client收到命令后，检查参数是否合法。如果参数合法，Client向Master发起Disable，否则返回失败。
2. Master收到命令后，检查参数是否合法。如果参数合法,发起写meta操作更新meta表。
3. 如果写meta操作失败且重试超过指定次数，返回失败。如果写meta操作成功，更新内存meta。
4. 向用户返回。

# Enable

1. Client收到命令后，检查参数是否合法。如果参数合法，Client向Master发起Enable命令，否则返回失败。
2. Master收到命令后，检查参数是否合法。如果参数合法,发起写meta操作更新meta表。
3. 写meta成功后向用户返回成功。
4. Master发起Load命令，load本次Enable操作对应的的tablet(s)。

# Drop

1. Client收到命令后，检查参数是否合法。如果参数合法，Client向Master发起Drop命令，否则返回失败。
2. Master收到命令后，检查参数是否合法。如果参数合法,发起写meta操作更新meta表。
3. 如果写meta操作失败且重试超过指定次数，返回失败。如果写meta操作成功，更新内存meta。
4. 向用户返回。

# Query

1. Query的主要功能是收集Tabletnode Server（以下简称TS）的信息，根据这个信息，Master可以发起相应的操作，例如负载均衡，垃圾清理等。
2. Query是一个定时任务，间隔时间可由用户配置。每次Query，Master向全部TS发起Query，并用计数等待所有TS返回。
3. 全部TS返回后，Master处理Query得到的结果，并且把收集到的信息写入到stat表中。

# Load Balance
为什么需要Load Balance？其一，Load Balance可以使TS之间数据均匀，避免访问热点（又Move保证）。其二，Load Balance可以使Tablet之间数据均匀，避免某个Tablet过大，导致读，写性能下降（由Split保证）；或者某个Tablet过小浪费资源（由Merge保证）。Master通过分析Query收集的结果进行判断是否要发起以上操作。

* Split

1. Tera的Split操作是轻量的的，没有任何数据拷贝。原因是Tera底层使用分布式文件系统，Tera只需要做逻辑上的分裂操作，而不需要进行实际的数据操作。
2. Tablet的大小超过设定阈值时，Master会向TS发起命令对该Tablet进行分裂。
3. TS收到命令后，首先算出split_key，split_key恰好能将Tablet切分成大小相等的两部分。注意，split_key有可能在sst文件的中间，这就造成分裂后的两个Tablet会共用同一个或多个sst文件的情况。
4. 获取split_key后，TS Unload这个Tablet，发起写meta操作更新meta表，成功后向Master返回。
5. Master收到返回以后扫描meta表，并更新内存meta，并且发起命令，Load两个新生成的Tablet。

* Merge

1. 同Split一样，Merge操作也是轻量的，没有实际数据拷贝。
2. Tablet的大小小于设定阈值时，Master会在所有Tablet中挑选与这个Tablet区间相邻并且大小合适的Tablet与其进行合并。如果没有找到合适的Tablet就不进行合并。
3. Master首先会发起命令Unload两个被合并的Tablet。如果其中任何一个操作失败，Master会将已经Unload的Tablet重新Load，并返回操作失败。
4. Unload成功后，Master发起写meta操作更新meta表。
5. 更新meta成功后Master更新内存meta并且对合并产生的新Tablet发起Load。

* Move

1. 

# Garbage Clean

1. 为什么要Garbage Clean（已下简称GC）？Tera分裂时会产生两个Tablet共用同一个或多个文件的情况（详见Split说明），而只有当两个Tablet都不再需要这个文件时，这个共享文件才能够被删除。但是共用这个文件的两个Tablet都无法得知另一方是否还在用这个文件，于是就需要一个拥有全局信息的“上帝”来决定这个共享文件是否可以被清理。这个“上帝”就是Master，“全局信息”便来自于Query。
2. Master只负责共享文件的GC。Tablet内部产生的垃圾文件由TS进行清理，因为这些文件是某个Tablet独用的，Tablet可以独立判断这个文件是否还有用。TS通过Query把正在使用的共享文件汇报给Master。
3. Master在Query前，将所有的共享文件列表存在内存中。收到Query结果后比较内存列表和汇报列表的diff，diff部分就是不再使用的共享文件，可以被删除。

