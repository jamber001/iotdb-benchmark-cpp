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


bool InsertTabletOperation::createSchema() {
    if (sessions.size() <= 0) {
        error_log("Invalid sessions. sessions.size()=%lu.", sessions.size());
        return false;
    }

    shared_ptr<Session> session = sessions[0];

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


void InsertTabletOperation::prepareData() {
    schemaList4Device.reserve(workerCfg.sensorNum);
    for (int sensorIdx = 0; sensorIdx < workerCfg.sensorNum; ++sensorIdx) {
        string sensorStr = getSensorStr(sensorIdx);
        int typeIdx = sensorIdx % workerCfg.dataTypeList.size();
        schemaList4Device.emplace_back(sensorStr, getTsDataType(workerCfg.dataTypeList[typeIdx]));
    }

    //== Prepare requestList ===
    tabletList.reserve(workerCfg.sessionNum);
    requestList.resize(workerCfg.sessionNum);
    for (int sgIdx = 0; sgIdx < workerCfg.storageGroupNum; ++sgIdx) {
        tabletList.emplace_back("fakeDeviceId", schemaList4Device, workerCfg.batchSize);
        Tablet &tablet = *tabletList.rbegin();
        for (int64_t i = 0; i < workerCfg.batchSize; i++) {
            size_t rowIdx = tablet.rowSize++;
            tablet.timestamps[rowIdx] = workerCfg.startTimestamp + i;

            int randInt = rand();
            for (int sensorIdx = 0; sensorIdx < workerCfg.sensorNum; ++sensorIdx) {
                switch (schemaList4Device[sensorIdx].second) {
                    case TSDataType::BOOLEAN: {
                        bool randBool = (randInt % 2 == 1) ? true : false;
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
                        double randDouble = randInt / 13.3;
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
        Session::buildInsertTabletReq(requestList[sgIdx], 0ll, tablet, true);
    }
}

void InsertTabletOperation::worker(int threadIdx) {
    debug_log("Enter InsertTabletOperation_old::worker(%d)", threadIdx);

    shared_ptr<Session> &session= sessions[threadIdx % sessions.size()];

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


