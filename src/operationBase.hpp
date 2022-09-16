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

class OperationBase {
public:
    OperationBase(const string &opName, const ServerCfg &serverCfg, const TaskCfg &workerCfg) : opName(opName),
                                                                                                serverCfg(serverCfg),
                                                                                                workerCfg(workerCfg) {
        srand(time(NULL));
        latencyArray.resize(latencyArraySize, 0);
        threadEnd.resize(latencyArraySize, false);
    }

    virtual ~OperationBase() { };

    string getOpName() { return opName;};
    const TaskCfg& getWorkerCfg() { return workerCfg;};
    int64_t getWorkerTimeUs() { return workerEndTimeUs - workerStartTimeUs; };

    bool createSessions();
    bool reCreatedSession(shared_ptr<Session> &session, int retryNum, int retryIntervalMs) ;

    void startWorkers();
    bool allWorkersFinished();
    void waitForAllWorkersFinished();

    virtual bool createSchema() = 0;

    virtual void worker(int threadIdx) = 0;

    bool setSgTTL(Session &session, const string &sgPath, int64_t ttlValueMs);
    static TSDataType::TSDataType getTsDataType(const string &typeStr);
    static TSEncoding::TSEncoding getTsEncodingType(const string &typeStr);
    static CompressionType::CompressionType getCompressionType(const string &typeStr);

    static string getPath(const string &sgPrefix, int sgIdx, int deviceIdx, int sensorIdx);
    static string getPath(const string &sgPrefix, int sgIdx, int deviceIdx);
    static string getPath(const string &sgPrefix, int sgIdx);
    static string getSensorStr(int sensorIdx);

    unsigned long long getSuccOperationCount() { return succOperationCount; };
    unsigned long long getFailOperationCount() { return failOperationCount; };
    unsigned long long getSuccInsertPointCount() { return succInsertPointCount; };
    unsigned long long getFailInsertPointCount() { return failInsertPointCount; };

    void addLatency(int64_t latencyUs);
    void genLatencySum();


    float getAvgLatencyMs() { return avgLatencyMs; };
    uint getminLatencyUs() { return minLatencyUs; };
    uint getmaxLatencyUs() { return maxLatencyUs; };
    uint getLatencyMaxRangUs() { return (latencyArraySize - 1) * 10; };
    const map<int, float> &getPermillageMap() { return permillagResulteMap; };

public:
    int64_t workerStartTimeUs;
    atomic_llong workerEndTimeUs;

protected:
    string opName;

    ServerCfg serverCfg;
    TaskCfg workerCfg;

    vector <thread> threads;
    vector <bool> threadEnd;
    vector <shared_ptr<Session>> sessions;

    atomic_ullong succOperationCount {0};
    atomic_ullong failOperationCount {0};
    atomic_ullong succInsertPointCount {0};
    atomic_ullong failInsertPointCount {0};

    //== For save latency data,
    static const int latencyArraySize = 100000;
    float  avgLatencyMs{0.0} ;
    map<int, float> permillagResulteMap;

    mutex latencyDataLock;      //this lock will save below variables
    vector<uint64_t> latencyArray;  //record the count per 0.01ms step
    uint64_t latencyCount{0};
    uint maxLatencyUs{0}, minLatencyUs{0xFFFFFFFF};

private:
    static void thread_entrance(OperationBase *opBase, int threadIdx) {
        opBase->worker(threadIdx);
        opBase->threadEnd[threadIdx] = true;
        opBase->workerEndTimeUs = getTimeUs();
    };
};

#endif //OPERATIONBASE_HPP
