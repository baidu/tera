# Performance

## Hardware info
```
   CPU: 2X Intel Xeon 16 Core E5-2450 v2 2.50GHz
   RAM: 8X 16GB 1333 MHz DDR3
  DISK: 5X 3TB SATA
 FLASH: 2X 480G SSD MLC
```

## Software info
```
    OS: CentOS 4.3
KERNEL: 2.6.32
```

## Benckmark
```
SCHEMA: <rawkey=readable, splitsize=1000000000, mergesize=0> {
          lg0 <storage=flash,compress=none,blocksize=4> {
            cf0 <maxversions=1,minversions=1,ttl=0>
          }
          lg0 <storage=flash,compress=none,blocksize=4> {
            cf1 <maxversions=1,minversions=1,ttl=0>
          }
        }
  DATA: key-size=24, value-size=1000, key-count=300000000
RESULT:  
  BENCHMARK      Throughput(MB/s)    Queries/s    Latency(ms)
  Write          9                   -            30
  Random-Read    -                   32000        2
  Seq-Read       20                  -            -
```
