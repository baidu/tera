# filter介绍
目前包含ValueFilter以及多个ValueFilter的AND或OR的自由组合的FilterList

## ValueFilter
### 功能简介
1. ValueFilter是什么
举个例子，在scan时，只需要输出满足以下条件的行：针对指定的cf和qu，其下的value > 4，这就是ValueFilter
2. ValueFilter的限制条件
- ValueFilter支持对value类型为整数，小数，字符串的过滤
- ValueFilter只在scan中生效，对于相同cf和qu，只支持获取1个version的scan
- cf和qu哪个不指定，哪个被设为空串""
- 当qu为空串""时，对于指定cf下所有qu的value，有一个不满足过滤条件，所在行就会被过滤掉
### 使用方法
1. 构造ValueFilter
```c++
ValueFilterPtr value_filter =
    std::make_shared<ValueFilter>(CompareOperator::kGreater, comparator);
```
2. 设置ValueFilter相关属性
```c++
value_filter->SetColumnFamily("cf1");
value_filter->SetColumnQualifier("qu1");
value_filter->SetFilterIfMissing(true);
```
3. Set到ScanDesc中
```c++
ScanDescriptor scan_desc("");
scan_desc.SetFilter(value_filter);
```
### 需要一个comparator
我们注意到，在使用方法的第1步构造ValueFilter时，构造函数中有一个参数是comparator，它是ValueFilter
需要用到的。
#### 因为，ValueFilter可以针对任意类型的Value进行过滤，而tera中存储的Value都是二进制格式，所以提供了FilterComparator这个对象，来负责具体的编码（存储之前需要由用户调用Encode方法进行编码），解码和比较，对ValueFilter屏蔽具体的Value类型。
#### 下面是针对不同类型，构造相应的FilterComparator的方法
根据待比较的值的数据类型，构造相应的comparator，同时需要指定比较的参考值，如ValueFilter功能简介
例子中的4。
- 例子1：若ValueFilter需要对Value为int64的整数进行过滤，那么需要先构造这样的comparator
```c++
int64_t ref_value = 4;
IntegerComparatorPtr comparator = std::make_shared<IntegerComparator>(IntegerValueType::kInt64,
                                                                      ref_value);
```
- 例子2：若ValueFilter需要对Value为uint8的整数进行过滤，那么需要先构造这样的comparator
```c++
uint8_t ref_value = 4;
IntegerComparatorPtr comparator = std::make_shared<IntegerComparator>(IntegerValueType::kUint8,
                                                                      ref_value);
```
- 例子3：若ValueFilter需要对Value为float或double的小数进行过滤，那么需要先构造这样的comparator
```c++
double ref_value = 4.0;
DecimalComparatorPtr comparator = std::make_shared<DecimalComparator>(ref_value);
```
- 例子4：若ValueFilter需要对std::string的Value进行过滤，那么需要先构造这样的comparator
```c++
std::string ref_value = "abc";
BinaryComparatorPtr comparator = std::make_shared<BinaryComparator>(ref_value);
```
#### 下面是用户在存储之前，针对不同类型，使用相应的FilterComparaotr进行编码的方法
用户只需要调用Encode方法，在向tera写入Value之前将Value进行一下转换，Decode方法会在ValueFilter内部实现中使用。
- 例子1：过滤的对象集合中的Value是int64类型，写入前先使用如下接口进行转换
```c++
int64_t value = 8;
std::string out_value;
bool ret = filter::IntegerComparator::EncodeInteger(filter::IntegerValueType::kInt64,
                                                    value, &out_value);
```
对于int64的value，也可以使用已有的put接口直接写入，这里是兼容的，也只有int64类型的可以这样使用
```c++
virtual bool Put(const std::string& row_key, const std::string& family,
                 const std::string& qualifier, const int64_t value,
                 ErrorCode* err) = 0;
```
- 例子2：过滤的对象集合中的Value是uint8类型，写入前先使用如下接口进行转换
```c++
uint8_t value = 2;
std::string out_value;
bool ret = filter::IntegerComparator::EncodeInteger(filter::IntegerValueType::kUint8,
                                                    value, &out_value);
```
- 例子3：过滤的对象集合中的Value是float或double类型，写入前先使用如下接口进行转换
```c++
double value = 2.0;
std::string out_value = filter::DecimalComparator::EncodeDecimal(value);
```

## FilterList
### 功能简介
为了实现类似 Filter1 && (Filter2 || Filter3)的功能而实现
### 使用方法
1. 先构造基本的Filter，如我们这里支持的ValueFilter
2. 再构造想要的FilterList
- 举个例子：如我们想要这样的Filter组合：Filter1 && (Filter2 || Filter3)，假设3个Filter都已经构造好了
那么，按如下代码构造FilterList
```c++
FilterListPtr sub_filter_list = std::make_shared<FilterList>(FilterList::kOr);
sub_filter_list->AddFilter(value_filter_1);
sub_filter_list->AddFilter(value_filter_2);
FilterListPtr filter_list = std::make_shared<FilterList>(FilterList::kAnd);
filter_list->AddFilter(value_filter_3);
filter_list->AddFilter(sub_filter_list);
```
3. 将构造好的FilterList Set到ScanDesc中
- 构造出的FilterList仍然是一个Filter，所以Set方法同ValueFilter部分介绍的Set方法
