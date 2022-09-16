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

bool InsertRecordOperation::createSchema() {
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


void InsertRecordOperation::worker(int threadIdx) {
    debug_log("Enter InsertRecordOperation::worker(%d)", threadIdx);

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

string InsertRecordOperation::genValue(TSDataType::TSDataType tsDataType) {
    int randInt = rand();
    switch (tsDataType) {
        case TSDataType::BOOLEAN: {
            return (randInt % 2) == 0 ? string("0") : string("1");
        }
        case TSDataType::INT32: {
            return to_string(randInt);
        }
        case TSDataType::INT64: {
            return to_string(randInt * (long long int) randInt);
        }
        case TSDataType::FLOAT: {
            return to_string((float) (randInt / 33.3));
        }
        case TSDataType::DOUBLE: {
            return to_string((double) (randInt / 11.3));
        }
        case TSDataType::TEXT: {
            string str(workerCfg.textDataLen, 's');
            char *p = (char *) str.c_str();
            for (uint i = 0; i < str.size(); i = i + 2) {
                *p++ = 'a' + (randInt & 0x07) + (i & 0x0F);
                *p++ = 'A' + (randInt & 0x0F) + (i & 0X07);
            }
            return str;
        }
        case TSDataType::NULLTYPE:
        default:
            return string();
    }
}

void InsertRecordOperation::prepareData() {
    measurements4OneRecord.reserve(workerCfg.sensorNum);
    types4OneRecord.reserve(workerCfg.sensorNum);

    for (int sensorIdx = 0; sensorIdx < workerCfg.sensorNum; ++sensorIdx) {
        string sensorStr = getSensorStr(sensorIdx);
        measurements4OneRecord.emplace_back(sensorStr);
        int typeIdx = sensorIdx % workerCfg.dataTypeList.size();
        TSDataType::TSDataType tsDataType = getTsDataType(workerCfg.dataTypeList[typeIdx]);
        types4OneRecord.emplace_back(tsDataType);
    }

    int num = 64;
    valuesList.resize(num);
    valuesList2.resize(num);
    for (int i = 0; i < num; ++i) {
        auto &values4OneRecord = valuesList[i];
        auto &values4OneRecord2 = valuesList2[i];
        values4OneRecord.reserve(workerCfg.sensorNum);
        values4OneRecord2.reserve(workerCfg.sensorNum);
        for (auto tsDataType: types4OneRecord) {
            values4OneRecord.push_back(move(genValue(tsDataType)));
            values4OneRecord2.push_back((char *) values4OneRecord.rbegin()->c_str());
        }
    }

}


void InsertRecordOperation::insertRecordsBatch(shared_ptr<Session> &session, int sgIdx, int deviceIdx, int64_t startTs) {
    string deviceId = getPath(sgPrefix, sgIdx, deviceIdx);

    uint64_t succCount = 0;
    for (int i = 0; i < workerCfg.batchSize; ++i) {
        auto &values4OneRecord = valuesList[i % valuesList.size()];
        auto &values4OneRecord2 = valuesList2[i % valuesList.size()];
        if (workerCfg.workMode == 0) {
            if (sendInsertRecord(session, deviceId, startTs, measurements4OneRecord, values4OneRecord) ){
                succCount++;
            }
        } else {
            if (sendInsertRecord2(session, deviceId, startTs, measurements4OneRecord, types4OneRecord, values4OneRecord2)) {
                succCount++;
            }
        }

        startTs++;
    }

    if ( succCount > 0 ) {
        succOperationCount += succCount;
        succInsertPointCount += (succCount * measurements4OneRecord.size());
    }

    uint64_t failCount = workerCfg.batchSize - succCount;
    if ( failCount > 0 ) {
        failOperationCount += failCount;
        failInsertPointCount += (failCount * measurements4OneRecord.size());
    }
}


bool InsertRecordOperation::sendInsertRecord(shared_ptr<Session> &session,
                                             const string &deviceId,
                                             int64_t timestamps,
                                             const vector<string> &measurements,
                                             const vector<string> &valuesList){
    try {
        int64_t startTimeUs = getTimeUs();
        session->insertRecord(deviceId, timestamps, measurements, valuesList);
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






