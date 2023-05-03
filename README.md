# Benchmark_cpp

## 1. Overview

__Benchmark_cpp is developed using C++ language and IoTDB C++ client SDK.

1. Benchmark_cpp is used to benchmark IoTDB.
2. It is often used to test IoTDB C++ client SDK's stability and performance.
3. It can test C++ client SDK's multiple interface simultaneously, such as insertTablet, insertTablets, InsertRecords, InsertStringRecords, InsertRecord, InsertStringRecord.
4. Benchmark_cpp has better performance and occupies lower CPU/Mem than Java Benchmark tool. For example, When testing insertTablet interface with same TPS, Benchmark_cpp only occupy <1/500 memory and <1/2 CPU against Java Benchmark tool.
5. Now, Benchmark_cpp tool only support x86(x86_64) + Linux.__



## 2. Usage

### 2.1 upgrade to the latest C++ SDK of IoTDB
This step is not necessary, and depends on your real need.
```bash
unzip apache-iotdb-0.14.0-SNAPSHOT-client-cpp-linux-x86_64-bin.zip         # get the latest IoTDB C++ SDK 
rm -rf iotdb_sdk/*                                                         # delete original old SDK
cp apache-iotdb-0.14.0-SNAPSHOT-client-cpp-bin/clientIncLib/* iotdb_sdk/   # copy IoTDB C++ SDK files
```

### 2.2 Build of Benchmark_cpp

```bash
cmake . -B ./build
cmake --build ./build
```



### 2.3 Run Benchmark_cpp after building

```bash
mkdir build
cmake . -B ./build
cmake --build ./build
cmake --install ./build --prefix "./out"
vi ./out/conf/main.conf                     #change the configuration 
./out/benchmark-cpp.sh                      #run benchmark_cpp
```



### 2.4 Make App package and run anywhere

```bash
mkdir build
cmake . -B ./build
cmake --build ./build --target package      #make App package
cp build/benchmark-cpp-1.0.0-Linux.zip  ~   #copy App package to any directory 
cd ~
unzip benchmark-cpp-1.0.0-Linux.zip
cd benchmark-cpp-1.0.0-Linux/benchmark_cpp
vi ./conf/main.conf                         #change the configuration
./benchmark-cpp.sh                          #run benchmark_cpp
```



**The example of running result:**

```
$ vi conf/main.conf        # Change configuration file
$ vi conf/record0.temp     # If need, change the template of record.
$ ./benchmark-cpp.sh       # Run benchmak tool

== Read base Configuration ==
 Server Address: 127.0.0.1:6667

== Read Worker Configuration ==
 tablet_task_1   ==>  Succ!
 tablets_task_1  ==>  Disabled!
 records_task_1  ==>  Disabled!
 record_task_1   ==>  Disabled!

== Clean test related SG ==
 Finished ... (0.363s) 

== Create sessions ==
 tablet_task_1 : create 5 sessions ...  (0.002s) 

== Create schema ==
 tablet_task_1 : create schema ...  (0.011s) 

== Start all tasks ==
 tablet_task_1 : Begin to insert data ... (2023-04-03 14:17:02)
   taskName=tablet_task_1
   taskType=TABLET
   timeAlignedEnable=True
   tagsEnable=True
   workMode=0
   sessionNum=5
   storageGroupNum=5
   deviceNum=1
   sensorNum=6
   recordInfoFile=record0.temp
   batchSize=100
   loopNum=50

>>> Finish Task: tablet_task_1  ... (0.121s) 

>>> Summary for ALL (2023.04.03 14:17:04.783) 
----------------------------------------------------------Task Status--------------------------------------------------------------
Task                 Status         Interval(sec)        RunningTime         
tablet_task_1        Finished       0.121                [2023.04.03 14:17:02.781 - 2023.04.03 14:17:02.902]
----------------------------------------------------------Result Matrix------------------------------------------------------------
Task            SuccOperation   SuccRecord      SuccPoint       FailOperation   FailRecord      FailPoint       Record/s        Point/s)       
tablet_task_1   250             25000           150000          0               0               0               207240.14       1243440.88     
----------------------------------------------------------Latency (ms) Matrix------------------------------------------------------
Task             AVG      MIN      P10      P25      MEDIAN   P75      P90      P95      P99      P999     MAX      SLOWEST_THREAD    
tablet_task_1    0.29     0.13     0.14     0.15     0.16     0.21     0.30     0.83     3.39     3.49     3.49     0.00
                   
```

