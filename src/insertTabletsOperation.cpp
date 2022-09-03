//
// Created by haiyi.zb on 8/30/22.
//

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

    return true;
}


void InsertTabletsOperation::worker(int threadIdx) {
    debug_log("Enter InsertTabletsOperation::worker(%d)", threadIdx);

    shared_ptr<Session> &session= sessions[threadIdx % sessions.size()];

    int64_t startTs = workerCfg.startTimestamp;
    for (int i = 0; i < workerCfg.loopNum; ++i) {
        //show("threadIdx=%d, loopIdx=%d.", threadIdx, i);
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


void InsertTabletsOperation::insertTabletsBatch(shared_ptr<Session> &session, int sgIdx, int64_t startTs) {
    vector<pair<string, TSDataType::TSDataType>> schemaList4Device;
    schemaList4Device.reserve(workerCfg.sensorNum);
    for (int sensorIdx = 0; sensorIdx < workerCfg.sensorNum; ++sensorIdx) {
        string sensorStr = getSensorStr(sensorIdx);
        int typeIdx = sensorIdx % workerCfg.dataTypeList.size();
        schemaList4Device.emplace_back(sensorStr, getTsDataType(workerCfg.dataTypeList[typeIdx]));
    }

    int deviceNum = workerCfg.deviceNum;
    vector<Tablet> tabletList;
    unordered_map<string, Tablet *> tabletMap;
    tabletList.reserve(deviceNum);
    tabletMap.reserve(deviceNum);
    for (int deviceIdx = 0; deviceIdx < deviceNum; ++deviceIdx) {
        string devicePath = getPath(sgPrefix, sgIdx, deviceIdx);
        tabletList.emplace_back(devicePath, schemaList4Device, workerCfg.batchSize); //maxRowNumber=workerCfg.batchSize, _isAligned = false
        tabletMap[devicePath] = &tabletList[deviceIdx];
    }

    for (int i =0; i< workerCfg.batchSize; ++i) {
        for (int deviceIdx = 0; deviceIdx < deviceNum; ++deviceIdx) {
            Tablet &tablet = tabletList[deviceIdx];
            size_t rowIdx = tablet.rowSize++;
            tablet.timestamps[rowIdx] = startTs + i;

            int randInt = rand();
            for (int sensorIdx = 0; sensorIdx < workerCfg.sensorNum; ++sensorIdx) {
                //show("sensorIdx = %d", sensorIdx);
                switch (schemaList4Device[sensorIdx].second) {
                    case TSDataType::BOOLEAN: {
                        bool randBool = (randInt % 2 == 0);
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
                        double randDouble = randInt / 99.9;
                        tablet.addValue(sensorIdx, rowIdx, &randDouble);
                        break;
                    }
                    case TSDataType::TEXT: {
                        string randStr = "str" + to_string(randInt);
                        tablet.addValue(sensorIdx, rowIdx, &randStr);
                        break;
                    }
                    case TSDataType::NULLTYPE:
                    default:
                        break;
                }
            }
        }

        if (tabletList[0].rowSize >= tabletList[0].maxRowNumber) {
            sendInsertTablets(session, tabletMap);

            for (auto it :tabletMap) {
                it.second->reset();
            }
        }
    }

    if (tabletList[0].rowSize != 0) {
        sendInsertTablets(session, tabletMap);
    }
}

void InsertTabletsOperation::sendInsertTablets(shared_ptr<Session> &session, unordered_map<string, Tablet *> &tabletMap) {
    uint64_t pointCount = tabletMap.begin()->second->rowSize * workerCfg.sensorNum * workerCfg.deviceNum;
    try {
        uint64_t startTimeUs = getTimeUs();
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