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

#include "insertRecord.hpp"
#include <stdio.h>
#include "easyUtility.hpp"
#include <stdio.h>

using namespace  std;

void InsertRecordOperation::worker(int threadIdx) {
    debug_log("Enter InsertRecordOperation::worker(%d)", threadIdx);

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

bool InsertRecordOperation::doPreWork() {
    prepareOneDeviceMeasurementsTypes();

    int num = 128;
    recordValueList.resize(num);
    recordValueList2.resize(num);
    for (int i = 0; i < num; ++i) {
        auto &values4OneRecord = recordValueList[i];
        auto &values4OneRecord2 = recordValueList2[i];
        values4OneRecord.reserve(workerCfg.sensorNum);
        values4OneRecord2.reserve(workerCfg.sensorNum);
        for (int sensorIdx = 0; sensorIdx < workerCfg.sensorNum; ++sensorIdx) {
            values4OneRecord.push_back(move(genRandDataStr(sensorIdx)));
            values4OneRecord2.push_back((char *) values4OneRecord.rbegin()->c_str());
        }
    }

    return true;
}


void InsertRecordOperation::insertRecordsBatch(shared_ptr<Session> &session, int sgIdx, int deviceIdx, int64_t startTs) {
    string deviceId = getPath(sgPrefix, sgIdx, deviceIdx);

    uint64_t succCount = 0;
    for (int i = 0; i < workerCfg.batchSize; ++i) {
        if (workerCfg.workMode == 0) {
            auto &values4OneRecord = recordValueList[i % recordValueList.size()];
            if (sendInsertRecord(session, deviceId, startTs, sensorNames4OneRecord, values4OneRecord) ){
                succCount++;
            }
        } else {
            auto &values4OneRecord2 = recordValueList2[i % recordValueList2.size()];
            if (sendInsertRecord2(session, deviceId, startTs, sensorNames4OneRecord, types4OneRecord, values4OneRecord2)) {
                succCount++;
            }
        }

        startTs++;
    }

    if ( succCount > 0 ) {
        succOperationCount += succCount;
        succInsertPointCount += (succCount * sensorNames4OneRecord.size());
    }

    uint64_t failCount = workerCfg.batchSize - succCount;
    if ( failCount > 0 ) {
        failOperationCount += failCount;
        failInsertPointCount += (failCount * sensorNames4OneRecord.size());
    }
}


bool InsertRecordOperation::sendInsertRecord(shared_ptr<Session> &session,
                                             const string &deviceId,
                                             int64_t timestamps,
                                             const vector<string> &measurements,
                                             const vector<string> &valuesList){
    try {
        int64_t startTimeUs = getTimeUs();
        if (!workerCfg.timeAlignedEnable) {
            session->insertRecord(deviceId, timestamps, measurements, valuesList);
        } else {
            session->insertAlignedRecord(deviceId, timestamps, measurements, valuesList);
        }
        addLatency(getTimeUs() - startTimeUs);
        return true;
    } catch (exception & e) {
        error_log("session exception: %s. Try to recover session.", e.what());
        int retryNum = 60, retryIntervalMs = 2000;
        if (reCreatedSession(session, retryNum, retryIntervalMs) ) {
            info_log("Succeed to recover session after retry.");
        }
        else {
            error_log("Can not recover session after %d retry. Exit.", retryNum);
            exit(-1);
        }

        return false;
    }
}


bool InsertRecordOperation::sendInsertRecord2(shared_ptr<Session> &session,
                                              const string &deviceId,
                                              int64_t timestamps,
                                              const vector<string> &measurementsList,
                                              const vector<TSDataType::TSDataType> &typesList,
                                              const vector<char *> &valuesList) {
    try {
        int64_t startTimeUs = getTimeUs();
        session->insertRecord(deviceId, timestamps, measurementsList, typesList, valuesList);
        if (!workerCfg.timeAlignedEnable) {
            session->insertRecord(deviceId, timestamps, measurementsList, typesList, valuesList);
        } else {
            session->insertAlignedRecord(deviceId, timestamps, measurementsList, typesList, valuesList);
        }
        addLatency(getTimeUs() - startTimeUs);
        return true;
    } catch (exception & e) {
        error_log("session exception: %s. Try to recover session.", e.what());
        int retryNum = 60, retryIntervalMs = 2000;
        if (reCreatedSession(session, retryNum, retryIntervalMs) ) {
            info_log("Succeed to recover session after retry.");
        }
        else {
            error_log("Can not recover session after %d retry. Exit.", retryNum);
            exit(-1);
        }

        return false;
    }
}






