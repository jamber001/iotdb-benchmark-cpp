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

void InsertTabletsOperation::worker(int threadIdx) {
    debug_log("Enter InsertTabletsOperation::worker(%d)", threadIdx);

    shared_ptr<Session> &session= sessionPtrs[threadIdx % sessionPtrs.size()];

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

bool InsertTabletsOperation::doPreWork() {
    prepareOneDeviceMeasurementsTypes();

    vector<pair<string, TSDataType::TSDataType>> schemaList4Device;
    schemaList4Device.reserve(workerCfg.sensorNum);
    for (int sensorIdx = 0; sensorIdx < workerCfg.sensorNum; ++sensorIdx) {
        schemaList4Device.emplace_back(workerCfg.fieldInfo4OneRecord[sensorIdx].sensorName, workerCfg.fieldInfo4OneRecord[sensorIdx].dataType);
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

            if (workerCfg.tagsEnable) {
                std::vector<std::map<std::string, std::string>> tags(workerCfg.sensorNum);
                for (int i = 0; i < workerCfg.sensorNum; i++) {
                    tags[i]["tag1"] = devicePath + "tv1";
                    tags[i]["tag2"] = devicePath + "tv2";
                }
                //tablets.rbegin()->setTags(tags);
            }
            tablets.rbegin()->setAligned(workerCfg.timeAlignedEnable);
            tabletMap[devicePath] = &tablets[deviceIdx];
        }

        for (int i = 0; i < workerCfg.batchSize; ++i) {
            for (int deviceIdx = 0; deviceIdx < workerCfg.deviceNum; ++deviceIdx) {
                Tablet &tablet = tablets[deviceIdx];
                size_t rowIdx = tablet.rowSize++;
                tablet.timestamps[rowIdx] = workerCfg.startTimestamp + i;

                char randVal[16];
                string randStr;
                for (int sensorIdx = 0; sensorIdx < workerCfg.sensorNum; ++sensorIdx) {
                    TSDataType::TSDataType dataType = schemaList4Device[sensorIdx].second;
                    switch (dataType) {
                        case TSDataType::BOOLEAN:
                        case TSDataType::INT32:
                        case TSDataType::INT64:
                        case TSDataType::FLOAT:
                        case TSDataType::DOUBLE: {
                            genRandData(sensorIdx, &randVal);
                            tablet.addValue(sensorIdx, rowIdx, &randVal);
                            break;
                        }
                        case TSDataType::TEXT: {
                            genRandData(sensorIdx, &randStr);
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

    return true;
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