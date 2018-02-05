实时 olap 引擎，

基于presto 改造的多维钻取的实时olap 引擎。

(1) presto worker 实现数据的存储和lru 技术的实现。
(2) 基于lucence 实现数据内存和hdfs 冷热存储。
(3) 基于倒排索引 实现数据的快速检索，实现presto 基于Lucece 的语法下推。
