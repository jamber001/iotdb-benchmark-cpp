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

int64_t OperationBase::getWorkerTimeUs() {
    if (allWorkersFinished()) {
        return workerEndTimeUs - workerStartTimeUs;
    } else {
        return getTimeUs() - workerStartTimeUs;
    }
};

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

void OperationBase::waitForAllWorkerThreadsFinished() {
    for (int i = 0; i < workerCfg.sessionNum; ++i) {
        threads[i].join();
    }
}

void OperationBase::prepareOneDeviceMeasurementsTypes() {
    sensorNames4OneRecord.reserve(workerCfg.fieldInfo4OneRecord.size());
    types4OneRecord.reserve(workerCfg.fieldInfo4OneRecord.size());

    for (int sensorIdx = 0; sensorIdx < workerCfg.sensorNum; ++sensorIdx) {
        string sensorStr = getSensorStr(sensorIdx);
        sensorNames4OneRecord.emplace_back(workerCfg.fieldInfo4OneRecord[sensorIdx].sensorName);
        types4OneRecord.emplace_back(workerCfg.fieldInfo4OneRecord[sensorIdx].dataType);
    }
}

bool OperationBase::createSchema() {
    if (sessionPtrs.size() <= 0) {
        error_log("Invalid sessionPtrs. sessionPtrs.size()=%lu.", sessionPtrs.size());
        return false;
    }

    Session *sessionPtr = sessionPtrs[0].get();

    if (!workerCfg.timeAlignedEnable) {
        return createSchema_NonAligned(sessionPtr);
    } else {
        return createSchema_Aligned(sessionPtr);
    }
}

bool OperationBase::createSchema_NonAligned(Session *sessionPtr) {
    int count = workerCfg.storageGroupNum * workerCfg.deviceNum * workerCfg.sensorNum;
    vector <string> paths;
    vector <TSDataType::TSDataType> tsDataTypes;
    vector <TSEncoding::TSEncoding> tsEncodings;
    vector <CompressionType::CompressionType> compressionTypes;
    vector <map<string, string>> tagsList;
    tsDataTypes.reserve(count);
    tsEncodings.reserve(count);
    compressionTypes.reserve(count);
    for (int sgIdx = 0; sgIdx < workerCfg.storageGroupNum; ++sgIdx) {
        string sgPath= getPath(sgPrefix, sgIdx);
        sessionPtr->setStorageGroup(sgPath);
        setSgTTL(*sessionPtr, sgPath, workerCfg.sgTTL);
        for (int deviceIdx = 0; deviceIdx < workerCfg.deviceNum; ++deviceIdx) {
            string devicePath = getPath(sgPrefix, sgIdx, deviceIdx);
            for (int sensorIdx = 0; sensorIdx < workerCfg.sensorNum; ++sensorIdx) {
                string sensorPath = devicePath + "." + workerCfg.fieldInfo4OneRecord[sensorIdx].sensorName;
                if (sessionPtr->checkTimeseriesExists(sensorPath)) {
                    error_log("create NonAligned Schema, TimeSeries(%s) has existed.", sensorPath.c_str());
                    return  false;
                }

                paths.push_back(sensorPath);
                tsDataTypes.push_back(workerCfg.fieldInfo4OneRecord[sensorIdx].dataType);
                tsEncodings.push_back(workerCfg.fieldInfo4OneRecord[sensorIdx].encodeType);
                compressionTypes.push_back(workerCfg.fieldInfo4OneRecord[sensorIdx].compressionType);

                if (workerCfg.tagsEnable) {
                    map<string, string> tags;
                    tags["tag1"] = devicePath + "tv1";
                    tags["tag2"] = devicePath + "tv2";
                    tagsList.push_back(tags);
                }
            }
        }
    }
    vector <map<string, string>> *tagsListPtr = nullptr;
    if (!tagsList.empty()) {
        tagsListPtr = &tagsList;
    }

    sessionPtr->createMultiTimeseries(paths, tsDataTypes, tsEncodings, compressionTypes, nullptr, tagsListPtr, nullptr, nullptr);

    return true;
}

bool OperationBase::createSchema_Aligned(Session *sessionPtr) {
    vector<string> measurements;
    vector <TSDataType::TSDataType> tsDataTypes;
    vector <TSEncoding::TSEncoding> tsEncodings;
    vector <CompressionType::CompressionType> compressionTypes;
    measurements.reserve(workerCfg.sensorNum);
    tsDataTypes.reserve(workerCfg.sensorNum);
    tsEncodings.reserve(workerCfg.sensorNum);
    compressionTypes.reserve(workerCfg.sensorNum);

    for (int sensorIdx = 0; sensorIdx < workerCfg.sensorNum; ++sensorIdx) {
        measurements.push_back(workerCfg.fieldInfo4OneRecord[sensorIdx].sensorName);
        tsDataTypes.push_back(workerCfg.fieldInfo4OneRecord[sensorIdx].dataType);
        tsEncodings.push_back(workerCfg.fieldInfo4OneRecord[sensorIdx].encodeType);
        compressionTypes.push_back(workerCfg.fieldInfo4OneRecord[sensorIdx].compressionType);
    }

    for (int sgIdx = 0; sgIdx < workerCfg.storageGroupNum; ++sgIdx) {
        string sgPath= getPath(sgPrefix, sgIdx);
        sessionPtr->setStorageGroup(sgPath);
        setSgTTL(*sessionPtr, sgPath, workerCfg.sgTTL);

        for (int deviceIdx = 0; deviceIdx < workerCfg.deviceNum; ++deviceIdx) {
            for (int sensorIdx = 0; sensorIdx < workerCfg.sensorNum; ++sensorIdx) {
                string sensorPath = getPath(sgPrefix, sgIdx, deviceIdx, sensorIdx);
                if (sessionPtr->checkTimeseriesExists(sensorPath)) {
                    error_log("create Time-Aligned Schema, TimeSeries(%s) has existed.", sensorPath.c_str());
                    return  false;
                }
            }
            string devicePath = getPath(sgPrefix, sgIdx, deviceIdx);
            sessionPtr->createAlignedTimeseries(devicePath, measurements, tsDataTypes, tsEncodings, compressionTypes);


//            //TODO: wait for Session::createAlignedTimeseries() to support tagsList
//            vector <map<string, string>> tagsList;
//            if (workerCfg.tagsEnable) {
//                map<string, string> tags;
//                tags["tag1"] = devicePath + "tv1";
//                tags["tag2"] = devicePath + "tv2";
//                tagsList.push_back(tags);
//            }
//            vector <map<string, string>> *tagsListPtr = nullptr;
//            if (!tagsList.empty()) {
//                tagsListPtr = &tagsList;
//            }
//            sessionPtr->createAlignedTimeseries(devicePath, measurements, tsDataTypes, tsEncodings, compressionTypes, tagsListPtr);
        }
    }

    return true;
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
    debug_log("createSessions(), workerCfg.sessionNum=%d", workerCfg.sessionNum);

    sessionPtrs.reserve(workerCfg.sessionNum);
    for (int i = 0; i < workerCfg.sessionNum; ++i) {
        sessionPtrs.emplace_back(new Session(serverCfg.host, serverCfg.port, serverCfg.user, serverCfg.passwd));
        debug_log("sessionPtrs.size()=%lu, i=%d", sessionPtrs.size(), i);
        (*sessionPtrs.rbegin())->open(serverCfg.rpcCompression, 1000);  //enableRPCCompression=false, connectionTimeoutInMs=1000
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
    (*latencyArrayPtr)[latency]++;
    latencyCount++;
    latencySumUs += latencyUs;
    if (maxLatencyUs < (uint32_t) latencyUs) {
        maxLatencyUs = latencyUs;
    }
    if (minLatencyUs > (uint32_t) latencyUs) {
        minLatencyUs = latencyUs;
    }
    latencyDataLock.unlock();
}


void OperationBase::doStatisticsCheckpoint() {
    backupStatistics(newStatisticsInfo);
    mergeStatisticsInfo(allStatisticsInfo, newStatisticsInfo);
}

void OperationBase::genFullStatisticsResult(StatisticsResult &result) {
    genStatisticsResult(result, allStatisticsInfo);
}

void OperationBase::genDeltaStatisticsResult(StatisticsResult &result) {
    genStatisticsResult(result, newStatisticsInfo);
}

void OperationBase::genLatencySum() {
    static uint permillageGoal[] = {100, 250, 500, 750, 900, 950, 990, 999};

    if (latencyCount == 0) {   //If no point
        for (auto it: permillageGoal) {
            permillagResulteMap[it] = 0.0;
        }
        avgLatencyMs = 0.0;
        return;
    }

    uint64_t count = 0;
    uint permillageGoalIdx = 0;
    uint permillage = permillageGoal[permillageGoalIdx];
    for (uint i = 0; i < (*latencyArrayPtr).size(); i++) {
        count += (*latencyArrayPtr)[i];
        if (permillage <= (count * 1000) / latencyCount) {
            permillagResulteMap[permillage] = i / 100.0;
            permillageGoalIdx++;
            if (permillageGoalIdx >= sizeof(permillageGoal)) {
                break;
            }
            permillage = permillageGoal[permillageGoalIdx];
        }
    }
    avgLatencyMs = (latencySumUs * 1.0) / latencyCount / 1000.0;
}


void OperationBase::genStatisticsResult(StatisticsResult &result, const StatisticsInfo &statisticsInfo) {
    static uint permillageGoal[] = {100, 250, 500, 750, 900, 950, 990, 999};

    result.reset();
    result.opName = opName;
    result.opStatus = allWorkersFinished() ? "Finished" : "Running";
    result.beginTimeUs = statisticsInfo.beginTimeUs;
    result.endTimeUs = statisticsInfo.endTimeUs;
    if (statisticsInfo.latencyCount == 0) {   //If no point
        for (auto it: permillageGoal) {
            result.latencyPermillageMap[it] = 0.0;
        }
        result.minLatencyUs = 0;
        return;
    }

    result.succOperationCount = statisticsInfo.succOperationCount;
    result.succOperationCount = statisticsInfo.succOperationCount;
    result.succInsertPointCount = statisticsInfo.succInsertPointCount;
    result.failInsertPointCount = statisticsInfo.failInsertPointCount;

    result.latencyCount = statisticsInfo.latencyCount;
    result.latencySumUs = statisticsInfo.latencySumUs;
    result.maxLatencyUs = statisticsInfo.maxLatencyUs;
    result.minLatencyUs = statisticsInfo.minLatencyUs;

    uint64_t count = 0;
    uint permillageGoalIdx = 0;
    uint permillage = permillageGoal[permillageGoalIdx];
    for (uint i = 0; i < latencyArraySize; i++) {
        count += (*statisticsInfo.latencyCountArrayPtr)[i];
        if (permillage <= (count * 1000) / statisticsInfo.latencyCount) {
            result.latencyPermillageMap[permillage] = i / 100.0;
            permillageGoalIdx++;
            if (permillageGoalIdx >= sizeof(permillageGoal)) {
                break;
            }
            permillage = permillageGoal[permillageGoalIdx];
        }
    }
    result.avgLatencyUs = statisticsInfo.latencySumUs / statisticsInfo.latencyCount;
    result.latencyMaxRangUs = getmaxLatencyUs();
}

void OperationBase::backupStatistics(StatisticsInfo& backupInfo) {
    if (lastCheckPointTimeUs == 0) {
        lastCheckPointTimeUs = workerStartTimeUs;
    }
    backupInfo.beginTimeUs = lastCheckPointTimeUs;
    if (allWorkersFinished()) {
        lastCheckPointTimeUs = workerEndTimeUs;
    } else {
        lastCheckPointTimeUs = getTimeUs();
    }
    backupInfo.endTimeUs = lastCheckPointTimeUs;

    backupInfo.succOperationCount = succOperationCount.exchange(0);
    backupInfo.failOperationCount = failOperationCount.exchange(0);
    backupInfo.succInsertPointCount = succInsertPointCount.exchange(0);
    backupInfo.failInsertPointCount = failInsertPointCount.exchange(0);

    vector<uint64_t> *newLatencyArrayPtr = &latencyArrayList[1];
    if (newLatencyArrayPtr == latencyArrayPtr) {
        newLatencyArrayPtr = &latencyArrayList[0];
    }
    for (uint i = 0; i < (*newLatencyArrayPtr).size(); i++) {
        (*newLatencyArrayPtr)[i] = 0;
    }

    latencyDataLock.lock();
    backupInfo.latencyCountArrayPtr = latencyArrayPtr;
    backupInfo.latencyCount = latencyCount;
    backupInfo.latencySumUs = latencySumUs;
    backupInfo.minLatencyUs = minLatencyUs;
    backupInfo.maxLatencyUs = maxLatencyUs;

    latencyArrayPtr = newLatencyArrayPtr;
    latencyCount = 0;
    latencySumUs = 0;
    minLatencyUs = 0xFFFFFFFF;
    maxLatencyUs = 0;
    latencyDataLock.unlock();
}

void OperationBase::mergeStatisticsInfo(StatisticsInfo& allInfo , StatisticsInfo& newInfo) {
    if (allInfo.beginTimeUs > newInfo.beginTimeUs) {
        allInfo.beginTimeUs = newInfo.beginTimeUs;
    }
    if (allInfo.endTimeUs < newInfo.endTimeUs) {
        allInfo.endTimeUs = newInfo.endTimeUs;
    }

    allInfo.succOperationCount += newInfo.succOperationCount;
    allInfo.failOperationCount += newInfo.failOperationCount;
    allInfo.succInsertPointCount += newInfo.succInsertPointCount;
    allInfo.failInsertPointCount += newInfo.failInsertPointCount;

    for (int i = 0; i < latencyArraySize; ++i) {
        (*allInfo.latencyCountArrayPtr)[i] += (*newInfo.latencyCountArrayPtr)[i];
    }

    allInfo.latencyCount += newInfo.latencyCount;
    allInfo.latencySumUs += newInfo.latencySumUs;

    if (allInfo.maxLatencyUs < newInfo.maxLatencyUs) {
        allInfo.maxLatencyUs = newInfo.maxLatencyUs;
    }
    if (allInfo.minLatencyUs > newInfo.minLatencyUs) {
        allInfo.minLatencyUs = newInfo.minLatencyUs;
    }
}

//
//
//void OperationBase::genRandData(TSDataType::TSDataType tsDataType, void *dataPtr, int TextSize /*= 2*/) {
//    int randInt = rand();
//    switch (tsDataType) {
//        case TSDataType::BOOLEAN: {
//            *(bool *) dataPtr = (randInt % 2) == 0 ? false : true;
//            break;
//        }
//        case TSDataType::INT32: {
//            *(int32_t *) dataPtr = randInt;
//            break;
//        }
//        case TSDataType::INT64: {
//            *(int64_t *) dataPtr = randInt * (int64_t) randInt;
//            break;
//        }
//        case TSDataType::FLOAT: {
//            *(float *) dataPtr = (float) (randInt / 33.3);
//            break;
//        }
//        case TSDataType::DOUBLE: {
//            *(double *) dataPtr = (double) (randInt / 11.3);
//            break;
//        }
//        case TSDataType::TEXT: {
//            string str(workerCfg.textDataLen, 's');
//            char *p = (char *) str.c_str();
//            for (uint i = 0; i < str.size(); i = i + 2) {
//                if ((i % 2) == 0) {
//                    *p++ = 'a' + (randInt & 0x07) + (i & 0x0F);
//                } else {
//                    *p++ = 'A' + (randInt & 0x0F) + (i & 0X07);
//                }
//            }
//            *(string *) dataPtr = str;
//            break;
//        }
//        case TSDataType::NULLTYPE:
//            break;
//        default:
//            return;
//    }
//}


void OperationBase::genRandData(int sensorIdx, void *dataPtr) {
    int randInt = rand();
    switch (workerCfg.fieldInfo4OneRecord[sensorIdx].dataType) {
        case TSDataType::BOOLEAN: {
            *(bool *) dataPtr = (randInt % 2) == 0 ? false : true;
            break;
        }
        case TSDataType::INT32: {
            *(int32_t *) dataPtr = randInt;
            break;
        }
        case TSDataType::INT64: {
            *(int64_t *) dataPtr = randInt * (int64_t) randInt;
            break;
        }
        case TSDataType::FLOAT: {
            *(float *) dataPtr = (float) (randInt / 33.3);
            break;
        }
        case TSDataType::DOUBLE: {
            *(double *) dataPtr = (double) (randInt / 11.3);
            break;
        }
        case TSDataType::TEXT: {
            int textPrefixSize = workerCfg.fieldInfo4OneRecord[sensorIdx].textPrefix.size();
            int textSize = workerCfg.fieldInfo4OneRecord[sensorIdx].textSize;
            if (textSize < textPrefixSize) {
                textSize = textPrefixSize;
            }
            string &randStr = *(string *) dataPtr;
            randStr.reserve(textSize);
            randStr.assign(workerCfg.fieldInfo4OneRecord[sensorIdx].textPrefix);
            randStr.append(textSize - textPrefixSize, 's');
            char *p = (char *) randStr.c_str() + textPrefixSize;
            for (uint i = textPrefixSize; i < randStr.size(); i++) {
                if ((i % 2) == 0) {
                    *p++ = 'a' + (randInt & 0x07) + (i & 0x0F);
                } else {
                    *p++ = 'A' + (randInt & 0x0F) + (i & 0X07);
                }
            }
            break;
        }
        case TSDataType::NULLTYPE:
            break;
        default:
            return;
    }
}
//
//string OperationBase::genRandDataStr(TSDataType::TSDataType tsDataType) {
//    int randInt = rand();
//    switch (tsDataType) {
//        case TSDataType::BOOLEAN:
//            return (randInt % 2) == 0 ? string("0") : string("1");
//        case TSDataType::INT32:
//            return to_string(randInt);
//        case TSDataType::INT64:
//            return to_string(randInt * (long long int) randInt);
//        case TSDataType::FLOAT:
//            return to_string((float) (randInt / 33.3));
//        case TSDataType::DOUBLE:
//            return to_string((double) (randInt / 31.7));
//        case TSDataType::TEXT: {
//            string randStr(workerCfg.textDataLen, 's');
//            char *p = (char *) randStr.c_str();
//            for (uint i = 0; i < randStr.size(); i = i + 2) {
//                if ((i % 2) == 0) {
//                    *p++ = 'a' + (randInt & 0x07) + (i & 0x0F);
//                } else {
//                    *p++ = 'A' + (randInt & 0x0F) + (i & 0X07);
//                }
//            }
//            return randStr;
//        }
//        case TSDataType::NULLTYPE:
//        default:
//            return string();
//    }
//}

string OperationBase::genRandDataStr(int sensorIdx) {
    int randInt = rand();
    switch (workerCfg.fieldInfo4OneRecord[sensorIdx].dataType) {
        case TSDataType::BOOLEAN:
            return (randInt % 2) == 0 ? string("0") : string("1");
        case TSDataType::INT32:
            return to_string(randInt);
        case TSDataType::INT64:
            return to_string(randInt * (long long int) randInt);
        case TSDataType::FLOAT:
            return to_string((float) (randInt / 33.3));
        case TSDataType::DOUBLE:
            return to_string((double) (randInt / 31.7));
        case TSDataType::TEXT: {
            int textPrefixSize = workerCfg.fieldInfo4OneRecord[sensorIdx].textPrefix.size();
            int textSize = workerCfg.fieldInfo4OneRecord[sensorIdx].textSize;
            if (textSize < textPrefixSize ) {
                textSize = textPrefixSize;
            }
            string randStr;
            randStr.reserve(textSize);
            randStr.assign(workerCfg.fieldInfo4OneRecord[sensorIdx].textPrefix);
            randStr.append(textSize - textPrefixSize, 's');
            char *p = (char *) randStr.c_str() + textPrefixSize;
            for (uint i = textPrefixSize; i < randStr.size(); i++) {
                if ((i % 2) == 0) {
                    *p++ = 'a' + (randInt & 0x07) + (i & 0x0F);
                } else {
                    *p++ = 'A' + (randInt & 0x0F) + (i & 0X07);
                }
            }
            return randStr;
        }
        case TSDataType::NULLTYPE:
        default:
            return string();
    }
}






