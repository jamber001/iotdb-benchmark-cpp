//
// Created by haiyi.zb on 8/30/22.
//

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

using namespace std;

class OperationBase {
public:
    OperationBase(const string &opName, const ServerCfg &serverCfg, const WorkerCfg &workerCfg) : opName(opName),
                                                                                                  serverCfg(serverCfg),
                                                                                                  workerCfg(workerCfg) {
        latencyArray.resize(latencyArraySize, 0);
        threadEnd.resize(latencyArraySize, false);
    }

    virtual ~OperationBase() { };

    string getOpName() { return opName;};
    const WorkerCfg& getWorkerCfg() { return workerCfg;};

    bool createSessions();
    bool reCreatedSession(shared_ptr<Session> &session, int retryNum, int retryIntervalMs) ;

    void startWorkers();
    bool allWorkersFinished();
    void waitForAllWorkersFinished();

    virtual bool createSchema() = 0;

    virtual void worker(int threadIdx) = 0;

    static TSDataType::TSDataType getTsDataType(const string &typeStr);
    static TSEncoding::TSEncoding getTsEncodingType(const string &typeStr);
    static CompressionType::CompressionType getCompressionType(const string &typeStr);

    static string getPath(const string &sgPrefix, int sgIdx, int deviceIdx, int sensorIdx);
    static string getPath(const string &sgPrefix, int sgIdx, int deviceIdx);
    static string getSensorStr(int sensorIdx);

    unsigned long long getSuccOperationCount() { return succOperationCount; };
    unsigned long long getFailOperationCount() { return failOperationCount; };
    unsigned long long getSuccInsertPointCount() { return succInsertPointCount; };
    unsigned long long getFailInsertPointCount() { return failInsertPointCount; };

    void addLatency(int latencyUs);
    void genLatencySum();


    float getAvgLatencyMs() { return avgLatencyMs; };
    uint getminLatencyUs() { return minLatencyUs; };
    uint getmaxLatencyUs() { return maxLatencyUs; };
    uint getLatencyMaxRangUs() { return (latencyArraySize - 1) * 10; };
    const map<int, float> &getPermillageMap() { return permillageMap; };

protected:
    string opName;

    ServerCfg serverCfg;
    WorkerCfg workerCfg;

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
    map<int, float> permillageMap;

    mutex latencyDataLock;      //this lock will save below variables
    vector<uint64_t> latencyArray;  //record the count per 0.01ms step
    uint64_t latencyCount{0};
    uint maxLatencyUs{0}, minLatencyUs{0xFFFFFFFF};

private:
    static void thread_entrance(OperationBase *opBase, int threadIdx) {
        opBase->worker(threadIdx);
        opBase->threadEnd[threadIdx] = true;
    };
};

#endif //OPERATIONBASE_HPP
