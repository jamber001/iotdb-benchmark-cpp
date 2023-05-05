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

#include "insertTabletOperation.hpp"
#include <stdio.h>
#include "easyUtility.hpp"

bool InsertTabletOperation::doPreWork() {
    prepareOneDeviceMeasurementsTypes();

    vector<pair<string, TSDataType::TSDataType>> schemaList4Device;
    schemaList4Device.reserve(workerCfg.fieldInfo4OneRecord.size());
    for (FieldInfo & fieldInfo : workerCfg.fieldInfo4OneRecord) {
        schemaList4Device.emplace_back(fieldInfo.sensorName, fieldInfo.dataType);
    }

    //== Prepare requestList ===
    tabletList.reserve(workerCfg.sessionNum);
    requestList.resize(workerCfg.sessionNum);
    for (int sgIdx = 0; sgIdx < workerCfg.storageGroupNum; ++sgIdx) {
        tabletList.emplace_back("fakeDeviceId", schemaList4Device, workerCfg.batchSize);
        Tablet &tablet = *tabletList.rbegin();
        string sgPath = getPath(sgPrefix, sgIdx);
        for (int64_t i = 0; i < workerCfg.batchSize; i++) {
            size_t rowIdx = tablet.rowSize++;
            tablet.timestamps[rowIdx] = workerCfg.startTimestamp + i;
            if (workerCfg.tagsEnable) {
                std::vector <std::map<std::string, std::string>> tags(workerCfg.sensorNum);
                for (int i = 0; i< workerCfg.sensorNum; i++) {
                    tags[i]["tag1"] = sgPath + "tv1";
                    tags[i]["tag2"] = sgPath + "tv2";
                }
                //tablet.setTags(tags);
            }

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
        tablet.setAligned(workerCfg.timeAlignedEnable);
        Session::buildInsertTabletReq(requestList[sgIdx], 0ll, tablet, true);
    }

    return true;
}

void InsertTabletOperation::worker(int threadIdx) {
    debug_log("Enter InsertTabletOperation_old::worker(%d)", threadIdx);

    shared_ptr<Session> &session= sessionPtrs[threadIdx % sessionPtrs.size()];

    //int64_t startTs = workerCfg.startTimestamp;  //TODO:
    int64_t startTs = (workerCfg.startTimestamp >> 8) << 8 ;
    for (int i = 0; i < workerCfg.loopNum; ++i) {
        for (int sgIdx = 0; sgIdx < workerCfg.storageGroupNum; ++sgIdx) {
            for (int deviceIdx = 0; deviceIdx < workerCfg.deviceNum; ++deviceIdx) {
                if ((sgIdx % workerCfg.sessionNum) == threadIdx) {
                    if (workerCfg.workMode == 0) {
                        insertTabletBatch(session, sgIdx, deviceIdx, startTs);
                    } else {
                        insertTabletBatch2(session, sgIdx, deviceIdx, startTs);
                    }
                }
            }
        }
        startTs += workerCfg.batchSize;

        if (workerCfg.loopIntervalMs > 0 ) {
            usleep(workerCfg.loopIntervalMs * 1000);
        }
    }

}

void InsertTabletOperation::insertTabletBatch(shared_ptr<Session> &session, int sgIdx, int deviceIdx, int64_t startTs) {
    TSInsertTabletReq &tsInsertTabletReq = requestList[sgIdx];
    tsInsertTabletReq.sessionId = session->getSessionId();
    tsInsertTabletReq.prefixPath = move(getPath(sgPrefix, sgIdx, deviceIdx));

    char *p0 = (char *) &startTs;
    char *p1 = (char *) tsInsertTabletReq.timestamps.data() + 7;
    bool needReset = false;
    for (int i = 0; i < 8; ++i) {
        if (*p0 != *p1) {
            *p1 = *p0;
            needReset = true;
        }
        p0++;
        p1--;
    }

    if (needReset) {
        int64_t *time64Ptr = (int64_t *) tsInsertTabletReq.timestamps.data();
        int64_t tpmInt64 = *time64Ptr;
        for (uint i = 1; i < tsInsertTabletReq.timestamps.size() / 8; ++i) {
            p1 = (char *) &tpmInt64;
            if (++p1[7] == 0) {
                if (++p1[6] == 0) {
                    if (++p1[5] == 0) {
                        if (++p1[4] == 0) {
                            if (++p1[3] == 0) {
                                if (++p1[2] == 0) {
                                    if (++p1[1] == 0) {
                                        ++p1[0];
                                    }
                                }
                            }
                        }
                    }
                }
            }
            *(++time64Ptr) = tpmInt64;
        }
    }

    sendInsertTablet(session, tsInsertTabletReq);
}

void InsertTabletOperation::insertTabletBatch2(shared_ptr<Session> &session, int sgIdx, int deviceIdx, int64_t startTs) {
    Tablet &tablet = tabletList[sgIdx];

    tablet.deviceId = move(getPath(sgPrefix, sgIdx, deviceIdx));
    for (int64_t i = 0; i < workerCfg.batchSize; i++) {
        tablet.timestamps[i] = startTs + i;
    }

    sendInsertTablet(session, tablet);
}

void InsertTabletOperation::sendInsertTablet(shared_ptr<Session> &session, TSInsertTabletReq &tsInsertTabletReq){
    uint64_t pointCount =  tsInsertTabletReq.timestamps.size()/8u * workerCfg.sensorNum; //TODO:

    try {
        int64_t startTimeUs = getTimeUs();
        session->insertTablet(tsInsertTabletReq);
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

void InsertTabletOperation::sendInsertTablet(shared_ptr<Session> &session, Tablet &tablet) {
    uint64_t pointCount = tablet.rowSize * workerCfg.sensorNum;

    try {
        int64_t startTimeUs = getTimeUs();
        session->insertTablet(tablet, true);
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


