# Benchmark_cpp

## 1. Overview

__Benchmark_cpp is developed using C++ language and IoTDB C++ client SDK.

1. Benchmark_cpp is used to benchmark IoTDB.
2. It is often used to test IoTDB C++ client SDK's stability and performance.
3. It can test C++ client SDK's multiple interface simultaneously, such as insertTablet, insertTablets, InsertRecords, InsertStringRecords, InsertRecord, InsertStringRecord.
4. Benchmark_cpp has better performance and occupies lower CPU/Mem than Java Benchmark tool. For example, When testing insertTablet interface with same TPS, Benchmark_cpp only occupy <1/500 memory and <1/2 CPU against Java Benchmark tool.
5. Now, Benchmark_cpp tool only support x86(x86_64) + Linux.__



## 2. Usage

### 2.1 Build of Benchmark_cpp

```bash
cmake . -B ./build
cmake --build ./build
```



### 2.2 Run Benchmark_cpp after building

```bash
cmake . -B ./build
cmake --build ./build
cmake --install ./build --prefix "./out"
vi ./out/conf/main.conf                     #change the configuration 
./out/benchmark-cpp.sh                      #run benchmark_cpp
```



### 2.3 Make App package and run anywhere

```bash
cmake . -B ./build
cmake --build ./build
cmake --build ./build --target package      #make App package
cp build/benchmark-cpp-1.0.0-Linux.zip  ~   #copy App package to any directory 
cd ~
unzip benchmark-cpp-1.0.0-Linux.zip
cd benchmark-cpp-1.0.0-Linux
vi ./out/conf/main.conf                     #change the configuration
./benchmark-cpp.sh                          #run benchmark_cpp
```



**The example of running result:**

```
$./benchmark-cpp.sh 

== Read base Configuration ==
 Server Address: 127.0.0.1:6667

== Read Worker Configuration ==
 TABLET_TASK_1   ==>  Succ!
 TABLETS_TASK_1  ==>  Disable!
 RECORDS_TASK_1  ==>  Disable!
 RECORD_TASK_1   ==>  Disable!

== Clean all SG ==
 Finished ... (0.065s) 

== Create sessions ==
 InsertTablets : create 5 sessions ...  (0.003s) 

== Create schema ==
 InsertTablets : create schema ...  (0.026s) 

== Start all tasks ==
 InsertTablets : Begin to insert data ... (2022-09-23 15:43:11)
   taskName=TABLET_TASK_1
   taskType=TABLET
   workMode=0
   sessionNum=5
   storageGroupNum=5
   deviceNum=1
   sensorNum=20
   batchSize=100
   loopNum=100000

>>> InsertTablets: Finish ...  (29.301s) 
----------------------------------------------------------Result Matrix------------------------------------------------------------
Operation            SuccOperation        SuccPoint            failOperation        failPoint            throughput(point/s) 
InsertTablets        500000               1000000000           0                    0                    34128590.93         
----------------------------------------------------------Latency (ms) Matrix------------------------------------------------------
Operation        AVG      MIN      P10      P25      MEDIAN   P75      P90      P95      P99      P999     MAX      SLOWEST_THREAD    
InsertTablets    0.29     0.15     0.16     0.18     0.23     0.36     0.43     0.49     0.78     1.10     88.30    999.99    
```

