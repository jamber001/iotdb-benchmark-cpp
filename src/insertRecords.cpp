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

#include "insertRecords.hpp"
#include <stdio.h>
#include "easyUtility.hpp"
#include <stdio.h>

using namespace  std;

bool InsertRecordsOperation::createSchema() {
    if (sessions.size() <= 0) {
        error_log("Invalid sessions. sessions.size()=%lu.", sessions.size());
        return false;
    }

    Session *session = sessions[0].get();

    int count = workerCfg.storageGroupNum * workerCfg.deviceNum * workerCfg.sensorNum;
    vector <string> paths;
    vector <TSDataType::TSDataType> tsDataTypes;
    vector <TSEncoding::TSEncoding> tsEncodings;
    vector <CompressionType::CompressionType> compressionTypes;
    tsDataTypes.reserve(count);
    tsEncodings.reserve(count);
    compressionTypes.reserve(count);
    for (int sgIdx = 0; sgIdx < workerCfg.storageGroupNum; ++sgIdx) {
        string sgPath= getPath(sgPrefix, sgIdx);
        session->setStorageGroup(sgPath);
        setSgTTL(*session, sgPath, workerCfg.sgTTL);
        for (int deviceIdx = 0; deviceIdx < workerCfg.deviceNum; ++deviceIdx) {
            for (int sensorIdx = 0; sensorIdx < workerCfg.sensorNum; ++sensorIdx) {
                string path= getPath(sgPrefix, sgIdx, deviceIdx, sensorIdx);
                if (!session->checkTimeseriesExists(path)) {
                    paths.push_back(path);
                    int typeIdx = sensorIdx % workerCfg.dataTypeList.size();
                    tsDataTypes.push_back(getTsDataType(workerCfg.dataTypeList[typeIdx]));
                    tsEncodings.push_back(getTsEncodingType(workerCfg.dataTypeList[typeIdx]));
                    compressionTypes.push_back(getCompressionType(workerCfg.dataTypeList[typeIdx]));
                }
            }
        }
    }

    session->createMultiTimeseries(paths, tsDataTypes, tsEncodings, compressionTypes, nullptr, nullptr, nullptr, nullptr);

    prepareData();

    return true;
}


void InsertRecordsOperation::worker(int threadIdx) {
    debug_log("Enter InsertRecordsOperation::worker(%d)", threadIdx);

    shared_ptr<Session> &session= sessions[threadIdx % sessions.size()];

    int64_t startTs = workerCfg.startTimestamp;
    for (int i = 0; i < workerCfg.loopNum; ++i) {
        for (int sgIdx = 0; sgIdx < workerCfg.storageGroupNum; ++sgIdx) {
            for (int deviceIdx = 0; deviceIdx < workerCfg.deviceNum; ++deviceIdx) {
                if ((sgIdx % workerCfg.sessionNum) == threadIdx) {
                    insertRecordsBatch(session, sgIdx, deviceIdx, startTs);
                }
            }
        }
        startTs += workerCfg.batchSize;

        if (workerCfg.loopIntervalMs > 0 ) {
            usleep(workerCfg.loopIntervalMs * 1000);
        }
    }

}


string InsertRecordsOperation::genValueStr(TSDataType::TSDataType tsDataType) {
    int randInt = rand();
    switch (tsDataType) {
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
            string randStr(workerCfg.textDataLen, 's');
            char *p = (char *) randStr.c_str();
            for (uint i = 0; i < randStr.size(); i = i + 2) {
                *p++ = 'a' + (randInt & 0x07) + (i & 0x0F);
                *p++ = 'A' + (randInt & 0x0F) + (i & 0X07);
            }
            return randStr;
        }
        case TSDataType::NULLTYPE:
        default:
            return string();
    }
}

void InsertRecordsOperation::prepareData() {
    vector<string> measurementsInOneRecord;
    vector<TSDataType::TSDataType> typesInOneRecord;
    measurementsInOneRecord.reserve(workerCfg.sensorNum);
    typesInOneRecord.reserve(workerCfg.sensorNum);
    for (int sensorIdx = 0; sensorIdx < workerCfg.sensorNum; ++sensorIdx) {
        string sensorStr = getSensorStr(sensorIdx);
        measurementsInOneRecord.emplace_back(sensorStr);
        int typeIdx = sensorIdx % workerCfg.dataTypeList.size();
        TSDataType::TSDataType tsDataType = getTsDataType(workerCfg.dataTypeList[typeIdx]);
        typesInOneRecord.emplace_back(tsDataType);
    }

    measurementsList.reserve(workerCfg.batchSize);
    typesList.reserve(workerCfg.batchSize);
    valuesList.resize(workerCfg.batchSize);
    valuesList2.resize(workerCfg.batchSize);
    for (int recordIdx = 0; recordIdx < workerCfg.batchSize; ++recordIdx) {
        measurementsList.push_back(measurementsInOneRecord);
        typesList.push_back(typesInOneRecord);

        auto &valuesInOneRecord = valuesList[recordIdx];
        auto &valuesInOneRecord2 = valuesList2[recordIdx];
        valuesInOneRecord.resize(workerCfg.sensorNum);
        valuesInOneRecord2.resize(workerCfg.sensorNum);
        for (int sensorIdx = 0; sensorIdx < workerCfg.sensorNum; ++sensorIdx) {
            TSDataType::TSDataType tsDataType = typesInOneRecord[sensorIdx];
            valuesInOneRecord[sensorIdx] = move(genValueStr(tsDataType));
            valuesInOneRecord2[sensorIdx] = (char *) valuesInOneRecord.rbegin()->c_str();
        }
    }
}


void InsertRecordsOperation::insertRecordsBatch(shared_ptr<Session> &session, int sgIdx, int deviceIdx, int64_t startTs) {
    string deviceId = getPath(sgPrefix, sgIdx, deviceIdx);
    vector<string> deviceIds;
    deviceIds.resize(workerCfg.batchSize, deviceId);

    vector<int64_t> timestamps;
    timestamps.reserve(workerCfg.batchSize);
    for (int i = 0; i < workerCfg.batchSize; ++i) {
        timestamps.push_back(startTs + i);
    }

    if (workerCfg.workMode == 0) {
        sendInsertRecords(session, deviceIds, timestamps, measurementsList, valuesList);
    } else {
        sendInsertRecords2(session, deviceIds, timestamps, measurementsList, typesList, valuesList2);
    }

}


void InsertRecordsOperation::sendInsertRecords(shared_ptr<Session> &session,
                                               const std::vector<std::string> &deviceIds,
                                               const std::vector<int64_t> &timestamps,
                                               const std::vector<std::vector<std::string>> &measurementsList,
                                               const std::vector<std::vector<string>> &valuesList) {
    uint64_t pointCount = typesList.size() * typesList[0].size();
    try {
        int64_t startTimeUs = getTimeUs();
        session->insertRecords(deviceIds, timestamps, measurementsList, valuesList);
        addLatency(getTimeUs() - startTimeUs);
        succOperationCount += 1;
        succInsertPointCount += pointCount;
    } catch (exception & e) {
        failOperationCount ++;
        failInsertPointCount += pointCount;

        error_log("session exception: %s. Try to recover session.", e.what());
        int retryNum = 60, retryIntervalMs = 2000;
        if (reCreatedSession(session, retryNum, retryIntervalMs) ) {
            info_log("Succeed to recover session after retry.");
        }
        else {
            error_log("Can not recover session after %d retry. Exit.", retryNum);
            exit(-1);
        }

        return;
    }
}



void InsertRecordsOperation::sendInsertRecords2(shared_ptr<Session> &session,
                                               const std::vector<std::string> &deviceIds,
                                               const std::vector<int64_t> &timestamps,
                                               const std::vector<std::vector<std::string>> &measurementsList,
                                               const vector<vector<TSDataType::TSDataType>> &typesList,
                                               const std::vector<std::vector<char *>> &valuesList) {
    uint64_t pointCount = typesList.size() * typesList[0].size();
    try {
        int64_t startTimeUs = getTimeUs();
        session->insertRecords(deviceIds, timestamps, measurementsList, typesList, valuesList);
        addLatency(getTimeUs() - startTimeUs);
        succOperationCount += 1;
        succInsertPointCount += pointCount;
    } catch (exception & e) {
        failOperationCount ++;
        failInsertPointCount += pointCount;

        error_log("session exception: %s. Try to recover session.", e.what());
        int retryNum = 60, retryIntervalMs = 2000;
        if (reCreatedSession(session, retryNum, retryIntervalMs) ) {
            info_log("Succeed to recover session after retry.");
        }
        else {
            error_log("Can not recover session after %d retry. Exit.", retryNum);
            exit(-1);
        }

        return;
    }
}



