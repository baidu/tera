# Tera表格字符串描述 —— PropTree

Tera中的表格结构较复杂，存在多种元素和多种属性可以配置，可配置的选项可能还会增加。

针对tera表格结构的特殊情况，我们使用一种带可配置属性的树型结构字符串（PropTree）进行描述。

树中的节点语法描述为：

    node_name[<prop1=value1,...>][{child1,...}]

其中，node_name是节点名称，对应到tera表格中可能是表格名称、locality group、column family名称等；“<>”中是节点的属性集合，可支持0个或多个属性；“{}”中是节点的孩子节点集合，支持递归嵌套（描述多层树结构），如果没有孩子节可不写。

一个典型的tera表格描述：

    table_hello <rawkey=binary, splitsize=4096, mergesize=512> {
        lg_index <storage=memory, compress=snappy, blocksize=4> {
            update_flag <maxversions=1>
        },
        lg_props <storage=flash, blocksize=32> {
            level,
            weight
        },
        lg_raw <storage=disk, blocksize=128> {
            data <maxversions=10>
        }
    }
