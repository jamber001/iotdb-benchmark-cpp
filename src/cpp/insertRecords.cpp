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

void InsertRecordsOperation::worker(int threadIdx) {
    debug_log("Enter InsertRecordsOperation::worker(%d)", threadIdx);

    shared_ptr<Session> &session= sessionPtrs[threadIdx % sessionPtrs.size()];

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



bool InsertRecordsOperation::doPreWork() {
    prepareOneDeviceMeasurementsTypes();

    measurementsList.reserve(workerCfg.batchSize);
    typesList.reserve(workerCfg.batchSize);
    recordValueList.resize(workerCfg.batchSize);
    recordValueList2.resize(workerCfg.batchSize);
    for (int recordIdx = 0; recordIdx < workerCfg.batchSize; ++recordIdx) {
        measurementsList.push_back(sensorNames4OneRecord);
        typesList.push_back(types4OneRecord);

        auto &valuesInOneRecord = recordValueList[recordIdx];
        auto &valuesInOneRecord2 = recordValueList2[recordIdx];
        valuesInOneRecord.resize(workerCfg.sensorNum);
        valuesInOneRecord2.resize(workerCfg.sensorNum);
        for (int sensorIdx = 0; sensorIdx < workerCfg.sensorNum; ++sensorIdx) {
            valuesInOneRecord[sensorIdx] = move(genRandDataStr(sensorIdx));
            valuesInOneRecord2[sensorIdx] = (char *) valuesInOneRecord.rbegin()->c_str();
        }
    }

    return true;
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
        sendInsertRecords(session, deviceIds, timestamps, measurementsList, recordValueList);
    } else {
        sendInsertRecords2(session, deviceIds, timestamps, measurementsList, typesList, recordValueList2);
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
        if (!workerCfg.timeAlignedEnable) {
            session->insertRecords(deviceIds, timestamps, measurementsList, valuesList);
        } else {
            session->insertAlignedRecords(deviceIds, timestamps, measurementsList, valuesList);
        }

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
        if (!workerCfg.timeAlignedEnable) {
            session->insertRecords(deviceIds, timestamps, measurementsList, typesList, valuesList);
        } else {
            session->insertAlignedRecords(deviceIds, timestamps, measurementsList, typesList, valuesList);
        }
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



