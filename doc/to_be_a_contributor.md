# 怎样成为一个贡献者
这是一个既简单又复杂的问题，Tera需要一个文档引导有兴趣的人加入。  
借鉴linux内核的方式，我设计了5个小任务，顺利完成这5个小任务后，就能成为一个Tera的committer（至少是贡献者）。

# 第一个任务
了解tera的数据模型和表格组织，回答以下两个问题：  

1. Tera中数据可以存在多个版本，建表时通过Schema指定保留策略，这个多版本概念是什么维度的？  
a) 表格维度，每个表保留若干版本  
b) 行维度，每个行保留若干版本  
c) 单元格维度，每个单元格保留若干版本  

2. 在Tera中，一个表既可以划分为多个Tablet，也可以划分为多个LocalityGroup，这两者的关系是什么？  
a) Tablet包含LocalityGroup，表格先被划分为若干个Tablet， 每个Tabelt又被划分为若干个LocalityGroup  
b) Localitygroup包含Tablet，表格先被划分为若干个LocalityGroup，每个LocalityGroup又被换分为若干个Tablet  
c) 两者互不包含  

# 后续的任务
在你完成第一个任务后，请将解答通过邮件发至tera-user@baidu.com，
我会将第二个任务通过邮件回复给你，后面任务可能会逐渐加大难度，至少是需要动下手了，  
不过相信我，总共不超过一两个小时，你就能成为一个tera的贡献者了！
