//
// Created by haiyi.zb on 8/30/22.
//
#include "operationBase.hpp"
#include <unistd.h>

using namespace std;

void OperationBase::startWorkers() {
    threads.reserve(workerCfg.sessionNum);
    for (int i = 0; i < workerCfg.sessionNum; ++i) {
        threads.emplace_back(OperationBase::thread_entrance, this, i);
    }
}




bool OperationBase::allWorkersFinished() {
    for (int i = 0; i < workerCfg.sessionNum; ++i) {
        if (!threadEnd[i]) {
            return false;
        }
    }
    return  true;
}


void OperationBase::waitForAllWorkersFinished() {
    for (int i = 0; i < workerCfg.sessionNum; ++i) {
        threads[i].join();
    }
}

bool OperationBase::reCreatedSession(shared_ptr<Session> &session, int retryNum, int retryIntervalMs) {
    int i;
    for (i = 0; i < retryNum; ++i) {
        try {
            session.reset(new Session(serverCfg.host, serverCfg.port, serverCfg.user, serverCfg.passwd));
            session->open(false, 1000);  //enableRPCCompression=false, connectionTimeoutInMs=1000
        } catch (exception &e) {
            error_log("Re-create session. retry %d. exception. %s.", i, e.what());
            usleep(retryIntervalMs * 1000);
            continue;
        }

        return true;
    }

    return false;
}

bool OperationBase::createSessions() {
    debug_log("workerCfg.sessionNum=%d", workerCfg.sessionNum);

    sessions.reserve(workerCfg.sessionNum);
    for (int i = 0; i < workerCfg.sessionNum; ++i) {
        sessions.emplace_back(new Session(serverCfg.host, serverCfg.port, serverCfg.user, serverCfg.passwd));
        debug_log("sessions.size()=%lu, i=%d", sessions.size(), i);
        (*sessions.rbegin())->open(false, 1000);  //enableRPCCompression=false, connectionTimeoutInMs=1000
    }

    return true;
}

TSDataType::TSDataType OperationBase::getTsDataType(const string &typeStr) {
    static unordered_map<string, TSDataType::TSDataType> mapStr2Type = {
            {"BOOLEAN", TSDataType::BOOLEAN},
            {"INT32", TSDataType::INT32},
            {"INT64",TSDataType::INT64},
            {"FLOAT",TSDataType::FLOAT},
            {"DOUBLE",TSDataType::DOUBLE},
            {"TEXT",TSDataType::TEXT},
            {"NULLTYPE",TSDataType::NULLTYPE},
    };

    auto itr = mapStr2Type.find (typeStr);
    if ( itr == mapStr2Type.end() ) {
        error_log("invalid typeStr=%s", typeStr.c_str());
        return TSDataType::TEXT;
    }

    return itr->second;
}

TSEncoding::TSEncoding OperationBase::getTsEncodingType(const string &typeStr) {
    return TSEncoding::PLAIN;

    static unordered_map<string, TSEncoding::TSEncoding> mapStr2Type = {
            {"BOOLEAN", TSEncoding::RLE},
            {"INT32", TSEncoding::RLE},
            {"INT64",TSEncoding::RLE},
            {"FLOAT",TSEncoding::GORILLA},
            {"DOUBLE",TSEncoding::GORILLA},
            {"TEXT",TSEncoding::PLAIN},
            {"NULLTYPE",TSEncoding::PLAIN},
    };

    auto itr = mapStr2Type.find (typeStr);
    if ( itr == mapStr2Type.end() ) {
        error_log("invalid typeStr=%s", typeStr.c_str());
        return TSEncoding::RLE;
    }

    return itr->second;
}

CompressionType::CompressionType OperationBase::getCompressionType(const string &typeStr) {
    static unordered_map<string, CompressionType::CompressionType> mapStr2Type = {
            {"BOOLEAN", CompressionType::SNAPPY},
            {"INT32", CompressionType::SNAPPY},
            {"INT64",CompressionType::SNAPPY},
            {"FLOAT",CompressionType::SNAPPY},
            {"DOUBLE",CompressionType::SNAPPY},
            {"TEXT",CompressionType::SNAPPY},
            {"NULLTYPE",CompressionType::SNAPPY},
    };

    auto itr = mapStr2Type.find (typeStr);
    if ( itr == mapStr2Type.end() ) {
        error_log("invalid typeStr=%s", typeStr.c_str());
        return CompressionType::SNAPPY;
    }

    return itr->second;
}

string OperationBase::getPath(const string &sgPrefix, int sgIdx, int deviceIdx, int sensorIdx) {
    char pathStr[64];
    snprintf(pathStr, 64, "root.cpp.%ssg%03d.d%03d.s%03d", sgPrefix.c_str(), sgIdx, deviceIdx, sensorIdx);
    return string(pathStr);
}

string OperationBase::getPath(const string &sgPrefix, int sgIdx, int deviceIdx) {
    char pathStr[64];
    snprintf(pathStr, 64, "root.cpp.%ssg%03d.d%03d", sgPrefix.c_str(), sgIdx, deviceIdx);
    return string(pathStr);
}

string OperationBase::getSensorStr(int sensorIdx) {
    char sensorStr[64];
    snprintf(sensorStr, 64, "s%03d", sensorIdx);
    return string(sensorStr);
}

void OperationBase::addLatency(int latencyUs) {
    if (latencyUs < 0 ) {
        error_log("Error latencyUs=%d", latencyUs);
        return;
    }

    uint latency = latencyUs / 10;  //unit 0.01ms
    if (latency >= latencyArraySize) {
        latency = latencyArraySize - 1;
    }

    latencyDataLock.lock();
    latencyArray[latency]++;
    latencyCount++;
    if (maxLatencyUs < (uint32_t) latencyUs) {
        maxLatencyUs = latencyUs;
    }
    if (minLatencyUs > (uint32_t) latencyUs) {
        minLatencyUs = latencyUs;
    }
    latencyDataLock.unlock();
}

void OperationBase::genLatencySum() {
    static uint permillageGoal[]={100,250,500,750,900, 950, 990, 999};
    uint64_t count = 0;
    uint64_t latencySum = 0;
    uint permillageGoalIdx = 0;
    uint permillage = permillageGoal[permillageGoalIdx];
    for (uint i = 0; i < latencyArray.size(); i++) {
        count += latencyArray[i];
        if (permillage <= (count * 1000) / latencyCount) {
            permillageMap[permillage] = i / 100.0;
            permillageGoalIdx++;
            if (permillageGoalIdx >= sizeof(permillageGoal)) {
                break;
            }
            permillage = permillageGoal[permillageGoalIdx];
        }

        latencySum += i * latencyArray[i];
    }
    avgLatencyMs = (latencySum * 1.0) / latencyCount / 100.0;
}

