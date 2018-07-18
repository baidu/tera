
# utils接口说明
tera中utils操作主要用来编码和解码counter cell
##### (1) 编码
```
static std::string EncodeCounter(int64_t counter);
```

##### (2) 解码

```
static bool DecodeCounter(const std::string& buf, int64_t* counter);
```

