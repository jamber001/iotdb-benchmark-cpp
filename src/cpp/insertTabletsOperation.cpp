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

#include "insertTabletsOperation.hpp"
#include <stdio.h>
#include "easyUtility.hpp"


bool InsertTabletsOperation::createSchema() {
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


void InsertTabletsOperation::worker(int threadIdx) {
    debug_log("Enter InsertTabletsOperation::worker(%d)", threadIdx);

    shared_ptr<Session> &session= sessions[threadIdx % sessions.size()];

    int64_t startTs = workerCfg.startTimestamp;
    for (int i = 0; i < workerCfg.loopNum; ++i) {
        for (int sgIdx = 0; sgIdx < workerCfg.storageGroupNum; ++sgIdx) {
            if ((sgIdx % workerCfg.sessionNum) == threadIdx) {
                insertTabletsBatch(session, sgIdx, startTs);
            }
        }
        startTs += workerCfg.batchSize;

        if (workerCfg.loopIntervalMs > 0 ) {
            usleep(workerCfg.loopIntervalMs * 1000);
        }
    }
}

void InsertTabletsOperation::prepareData() {
    vector<pair<string, TSDataType::TSDataType>> schemaList4Device;
    schemaList4Device.reserve(workerCfg.sensorNum);
    for (int sensorIdx = 0; sensorIdx < workerCfg.sensorNum; ++sensorIdx) {
        string sensorStr = getSensorStr(sensorIdx);
        int typeIdx = sensorIdx % workerCfg.dataTypeList.size();
        schemaList4Device.emplace_back(sensorStr, getTsDataType(workerCfg.dataTypeList[typeIdx]));
    }

    tabletMapList.resize(workerCfg.storageGroupNum);
    tabletsList.resize(workerCfg.storageGroupNum);
    for (int sgIdx=0; sgIdx<workerCfg.storageGroupNum; sgIdx++) {
        vector<Tablet> &tablets = tabletsList[sgIdx];
        unordered_map<string, Tablet *> &tabletMap = tabletMapList[sgIdx];
        tablets.reserve(workerCfg.deviceNum);
        tabletMap.reserve(workerCfg.deviceNum);
        for (int deviceIdx = 0; deviceIdx < workerCfg.deviceNum; ++deviceIdx) {
            string devicePath = getPath(sgPrefix, sgIdx, deviceIdx);
            tablets.emplace_back(devicePath, schemaList4Device, workerCfg.batchSize); //maxRowNumber=workerCfg.batchSize, _isAligned = false
            tabletMap[devicePath] = &tablets[deviceIdx];
        }

        for (int i = 0; i < workerCfg.batchSize; ++i) {
            for (int deviceIdx = 0; deviceIdx < workerCfg.deviceNum; ++deviceIdx) {
                Tablet &tablet = tablets[deviceIdx];
                size_t rowIdx = tablet.rowSize++;
                tablet.timestamps[rowIdx] = workerCfg.startTimestamp + i;

                int randInt = rand();
                for (int sensorIdx = 0; sensorIdx < workerCfg.sensorNum; ++sensorIdx) {
                    switch (schemaList4Device[sensorIdx].second) {
                        case TSDataType::BOOLEAN: {
                            bool randBool = (randInt % 2 == 1) ? true: false;
                            tablet.addValue(sensorIdx, rowIdx, &randBool);
                            break;
                        }
                        case TSDataType::INT32:
                            tablet.addValue(sensorIdx, rowIdx, &randInt);
                            break;
                        case TSDataType::INT64: {
                            int64_t randInt64 = randInt * (int64_t) randInt;
                            tablet.addValue(sensorIdx, rowIdx, &randInt64);
                            break;
                        }
                        case TSDataType::FLOAT: {
                            float randFloat = randInt / 33.3;
                            tablet.addValue(sensorIdx, rowIdx, &randFloat);
                            break;
                        }
                        case TSDataType::DOUBLE: {
                            double randDouble = randInt / 3.3;
                            tablet.addValue(sensorIdx, rowIdx, &randDouble);
                            break;
                        }
                        case TSDataType::TEXT: {
                            string randStr(workerCfg.textDataLen, 's');
                            char *p = (char *) randStr.c_str();
                            for (uint i = 0; i < randStr.size(); i = i + 2) {
                                *p++ = 'a' + (randInt & 0x07) + (i & 0x0F);
                                *p++ = 'A' + (randInt & 0x0F) + (i & 0X07);
                            }
                            tablet.addValue(sensorIdx, rowIdx, &randStr);
                            break;
                        }
                        case TSDataType::NULLTYPE:
                        default:
                            break;
                    }
                }
            }
        }
    }


}

void InsertTabletsOperation::insertTabletsBatch(shared_ptr<Session> &session, int sgIdx, int64_t startTs) {
    unordered_map<string, Tablet *> &tabletMap = tabletMapList[sgIdx];
    for (auto itr : tabletMap) {
        Tablet &tablet = *(itr.second);
        for (int i = 0; i < workerCfg.batchSize; i++) {
            tablet.timestamps[i] = startTs + i;
        }
    }

    sendInsertTablets(session, tabletMap);
}

void InsertTabletsOperation::sendInsertTablets(shared_ptr<Session> &session, unordered_map<string, Tablet *> &tabletMap) {
    uint64_t pointCount = tabletMap.begin()->second->rowSize * workerCfg.sensorNum * workerCfg.deviceNum;
    try {
        int64_t startTimeUs = getTimeUs();
        session->insertTablets(tabletMap, true);
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