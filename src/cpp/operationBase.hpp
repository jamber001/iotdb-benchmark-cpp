/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef OPERATIONBASE_HPP
#define OPERATIONBASE_HPP

#include <vector>
#include <thread>
#include <memory>
#include <atomic>
#include <map>

#include "easyLog.hpp"
#include "paramCfg.hpp"
#include "Session.h"
#include "easyCfgBase.hpp"

using namespace std;

struct StatisticsInfo {
    int64_t beginTimeUs{0x7FFFFFFFFFFFFFFF};
    int64_t endTimeUs{0};

    uint64_t succOperationCount{0};
    uint64_t failOperationCount{0};
    uint64_t succInsertPointCount{0};
    uint64_t failInsertPointCount{0};

    vector<uint64_t> *latencyCountArrayPtr {nullptr};
    uint64_t latencyCount{0};
    uint64_t latencySumUs{0};

    //== below 2 variables are from real-time calculate
    uint maxLatencyUs{0}, minLatencyUs{0xFFFFFFFF};
};

class StatisticsResult {
public:
    void reset() {
        opName.clear();
        opStatus.clear();

        beginTimeUs = 0;
        endTimeUs = 0;
        succOperationCount = 0;
        failOperationCount = 0;
        succRecordCount = 0;
        failRecordCount = 0;
        succInsertPointCount = 0;
        failInsertPointCount = 0;

        latencyCount = 0;
        latencySumUs = 0;
        maxLatencyUs = 0;
        minLatencyUs = 0xFFFFFFFF;
        avgLatencyUs = 0;
        latencyPermillageMap.clear();
    };

    string opName;
    string opStatus;
    unsigned long long beginTimeUs{0}, endTimeUs{0};

    unsigned long long succOperationCount{0};
    unsigned long long failOperationCount{0};
    unsigned long long succRecordCount{0};
    unsigned long long failRecordCount{0};
    unsigned long long succInsertPointCount{0};
    unsigned long long failInsertPointCount{0};

    unsigned long long latencyCount{0};
    unsigned long long latencySumUs{0};
    uint maxLatencyUs{0};
    uint minLatencyUs{0xFFFFFFFF};
    uint avgLatencyUs{0};
    map<int, float> latencyPermillageMap;  // permillag => Latency(ms)
    uint latencyMaxRangUs;
};

class OperationBase {
public:
    OperationBase(const string &opName, const ServerCfg &serverCfg, const TaskCfg &workerCfg) : opName(opName),
                                                                                                serverCfg(serverCfg),
                                                                                                workerCfg(workerCfg) {
        srand(time(NULL));

        for (uint i = 0; i < 3; i++) {
            latencyArrayList[i].resize(latencyArraySize, 0);
        }
        latencyArrayPtr = &latencyArrayList[0];
        allStatisticsInfo.latencyCountArrayPtr = &latencyArrayList[2];

        threadEnd.resize(workerCfg.sessionNum, false);
    }

    virtual ~OperationBase() {};

    string getOpName() { return opName;};
    const TaskCfg& getWorkerCfg() { return workerCfg;};
    int64_t getWorkerStartTimeUs() { return workerStartTimeUs; };
    int64_t getWorkerEndTimeUs() { return workerEndTimeUs; };
    int64_t getWorkerTimeUs();

    bool createSessions();
    bool reCreatedSession(shared_ptr<Session> &session, int retryNum, int retryIntervalMs) ;

    void startWorkers();
    bool allWorkersFinished();
    void waitForAllWorkerThreadsFinished();

    virtual void prepareOneDeviceMeasurementsTypes();
    virtual bool doPreWork() = 0;

    virtual bool createSchema();
    virtual bool createSchema_NonAligned(Session *sessionPtr);  //This function will be called by createSchema()
    virtual bool createSchema_Aligned(Session *sessionPtr);     //This function will be called by createSchema()

    virtual void worker(int threadIdx) = 0;

    bool setSgTTL(Session &session, const string &sgPath, int64_t ttlValueMs);

    static string getPath(const string &sgPrefix, int sgIdx, int deviceIdx, int sensorIdx);
    static string getPath(const string &sgPrefix, int sgIdx, int deviceIdx);
    static string getPath(const string &sgPrefix, int sgIdx);
    static string getSensorStr(int sensorIdx);

    unsigned long long getSuccOperationCount() { return succOperationCount; };
    unsigned long long getFailOperationCount() { return failOperationCount; };
    unsigned long long getSuccInsertPointCount() { return succInsertPointCount; };
    unsigned long long getFailInsertPointCount() { return failInsertPointCount; };

    void addLatency(int64_t latencyUs);

    void genLatencySum(); //TODO:

    void doStatisticsCheckpoint();
    void genFullStatisticsResult(StatisticsResult &result);
    void genDeltaStatisticsResult(StatisticsResult &result);

    float getAvgLatencyMs() { return avgLatencyMs; };
    uint getminLatencyUs() { return minLatencyUs; };
    uint getmaxLatencyUs() { return maxLatencyUs; };
    uint getLatencyMaxRangUs() { return (latencyArraySize - 1) * 10; };
    const map<int, float> &getPermillageMap() { return permillagResulteMap; };

protected:
    void backupStatistics(StatisticsInfo& backupInfo);
    void mergeStatisticsInfo(StatisticsInfo &allInfo, StatisticsInfo &newInfo);
    void genStatisticsResult(StatisticsResult &result, const StatisticsInfo &statisticsInfo);

public:
    int64_t workerStartTimeUs;
    atomic_llong workerEndTimeUs {0};

protected:
    string opName;

    ServerCfg serverCfg;
    TaskCfg workerCfg;

    string sgPrefix = "sgPrefix_";

    vector<string> sensorNames4OneRecord;           //it is used by all Sessions
    vector<TSDataType::TSDataType> types4OneRecord;  //it is used by all Sessions

    vector <thread> threads;
    vector <bool> threadEnd;
    vector <shared_ptr<Session>> sessionPtrs;

    int64_t lastCheckPointTimeUs{0};

    atomic_ullong succOperationCount {0};
    atomic_ullong failOperationCount {0};
    atomic_ullong succInsertPointCount {0};
    atomic_ullong failInsertPointCount {0};

    //== For save latency data,
    static const int latencyArraySize = 100000;
    float avgLatencyMs{0.0};
    map<int, float> permillagResulteMap;  // permillag => Latency(ms)

    vector<uint64_t> latencyArrayList[3];  //record the count per 0.01ms step

    mutex latencyDataLock;          //this lock will guard below variables
    vector<uint64_t> *latencyArrayPtr;
    uint64_t latencyCount{0};
    uint64_t latencySumUs{0};
    uint maxLatencyUs{0}, minLatencyUs{0xFFFFFFFF};

    //== for backup the last checkpoint's Statistics Info
    StatisticsInfo allStatisticsInfo, newStatisticsInfo;

protected:
    void genRandData(int sensorIdx, void *dataPtr);
    string genRandDataStr(int sensorIdx);

private:
    static void thread_entrance(OperationBase *opBase, int threadIdx) {
        opBase->worker(threadIdx);
        opBase->threadEnd[threadIdx] = true;
        if (opBase->allWorkersFinished()) {
            opBase->workerEndTimeUs = getTimeUs();
        }
    };
};

#endif //OPERATIONBASE_HPP
