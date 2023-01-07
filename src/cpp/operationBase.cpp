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

#include "operationBase.hpp"
#include <unistd.h>

using namespace std;

void OperationBase::startWorkers() {
    threads.reserve(workerCfg.sessionNum);

    workerStartTimeUs = getTimeUs();
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
        (*sessions.rbegin())->open(serverCfg.rpcCompression, 1000);  //enableRPCCompression=false, connectionTimeoutInMs=1000
    }

    return true;
}

bool OperationBase::setSgTTL(Session &session, const string &sgPath, int64_t ttlValueMs) {
    if (ttlValueMs <= 0) {
        debug_log("ttlValueMs(%lld) <= 0, so not set TTL.", (long long) ttlValueMs);
        return false;
    }

    //e.g. set ttl to root.sgcc.** 3600000
    char sqlStr[64];
    snprintf(sqlStr, sizeof(sqlStr), "set ttl to %s %lld", sgPath.c_str(), (long long) ttlValueMs);
    try {
        session.executeNonQueryStatement(sqlStr);
    }
    catch (const exception &e) {
        error_log("cleanAllSG(), error: %s", e.what());  //TODO: maybe, IoTDB bug.
        return false;
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
    //return TSEncoding::PLAIN;

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
    snprintf(pathStr, 64, "root.cpp_%ssg%03d.d%03d.s%03d", sgPrefix.c_str(), sgIdx, deviceIdx, sensorIdx);
    return string(pathStr);
}

string OperationBase::getPath(const string &sgPrefix, int sgIdx, int deviceIdx) {
    char pathStr[64];
    snprintf(pathStr, 64, "root.cpp_%ssg%03d.d%03d", sgPrefix.c_str(), sgIdx, deviceIdx);
    return string(pathStr);
}

string OperationBase::getPath(const string &sgPrefix, int sgIdx) {
    char pathStr[64];
    snprintf(pathStr, 64, "root.cpp_%ssg%03d", sgPrefix.c_str(), sgIdx);
    return string(pathStr);
}


string OperationBase::getSensorStr(int sensorIdx) {
    char sensorStr[64];
    snprintf(sensorStr, 64, "s%03d", sensorIdx);
    return string(sensorStr);
}

void OperationBase::addLatency(int64_t latencyUs) {
    if (latencyUs < 0 ) {
        error_log("Error latencyUs=%lld", (long long) latencyUs);
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

    if (latencyCount  == 0) {   //If no point
        for (auto it : permillageGoal ) {
            permillagResulteMap[it] = 0.0;
        }
        avgLatencyMs = 0.0;
        return;
    }

    uint64_t count = 0;
    uint64_t latencySum = 0;
    uint permillageGoalIdx = 0;
    uint permillage = permillageGoal[permillageGoalIdx];
    for (uint i = 0; i < latencyArray.size(); i++) {
        count += latencyArray[i];
        if (permillage <= (count * 1000) / latencyCount) {
            permillagResulteMap[permillage] = i / 100.0;
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

