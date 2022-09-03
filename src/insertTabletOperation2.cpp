//
// Created by haiyi.zb on 9/1/22.
//

#include "insertTabletOperation2.hpp"
#include <stdio.h>
#include "easyUtility.hpp"


bool InsertTabletOperation2::createSchema() {
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


void InsertTabletOperation2::worker(int threadIdx) {
    debug_log("Enter InsertTabletOperation::worker(%d)", threadIdx);

    shared_ptr<Session> &session= sessions[threadIdx % sessions.size()];

    //int64_t startTs = workerCfg.startTimestamp;  //TODO:
    int64_t startTs = (workerCfg.startTimestamp >> 8) << 8 ;
    for (int i = 0; i < workerCfg.loopNum; ++i) {
        for (int sgIdx = 0; sgIdx < workerCfg.storageGroupNum; ++sgIdx) {
            for (int deviceIdx = 0; deviceIdx < workerCfg.deviceNum; ++deviceIdx) {
                if ((sgIdx % workerCfg.sessionNum) == threadIdx) {
                    insertTabletBatch(session, sgIdx, deviceIdx, startTs);
                }
            }
        }
        //startTs += workerCfg.batchSize;
        startTs += 256; //TODO:

        if (workerCfg.loopIntervalMs > 0 ) {
            usleep(workerCfg.loopIntervalMs * 1000);
        }
    }

}

void InsertTabletOperation2::prepareData() {
    schemaList4Device.reserve(workerCfg.sensorNum);
    for (int sensorIdx = 0; sensorIdx < workerCfg.sensorNum; ++sensorIdx) {
        string sensorStr = getSensorStr(sensorIdx);
        int typeIdx = sensorIdx % workerCfg.dataTypeList.size();
        schemaList4Device.emplace_back(sensorStr, getTsDataType(workerCfg.dataTypeList[typeIdx]));
    }

    //=========================
    requestList.resize(workerCfg.sessionNum);
    for (int sessionIdx = 0; sessionIdx < workerCfg.sessionNum; ++sessionIdx) {
        Tablet tablet("sg", schemaList4Device, workerCfg.batchSize);

        for (int64_t i = 0; i < workerCfg.batchSize; i++) {
            size_t rowIdx = tablet.rowSize++;
            //tablet.timestamps[rowIdx] = i;  //TODO:
            tablet.timestamps[rowIdx] = ((workerCfg.startTimestamp >> 32) << 32 )+ i;

            int randInt = rand();
            for (int sensorIdx = 0; sensorIdx < workerCfg.sensorNum; ++sensorIdx) {
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
                        string randStr = "s" + to_string(randInt);
                        tablet.addValue(sensorIdx, rowIdx, &randStr);
                        break;
                    }
                    case TSDataType::NULLTYPE:
                    default:
                        break;
                }
            }
        }
        Session::buildInsertTabletReq(requestList[sessionIdx], 0ll, tablet, true);
    }

}


void InsertTabletOperation2::insertTabletBatch(shared_ptr<Session> &session, int sgIdx, int deviceIdx, int64_t startTs) {
    TSInsertTabletReq &tsInsertTabletReq = requestList[sgIdx];
    tsInsertTabletReq.sessionId = session->getSessionId();
    tsInsertTabletReq.prefixPath = move(getPath(sgPrefix, sgIdx, deviceIdx));


    //TODO:
//    u_char *timePtr = (u_char *) tsInsertTabletReq.timestamps.data();
//    timePtr += 2;
//    for (uint i = 0; i < tsInsertTabletReq.timestamps.size() / 8; ++i) {
//        (*timePtr)++;
//        if ((*timePtr) = 0 )
//        *timePtr = i;
//        timePtr = timePtr + 8;
//    }

    //TODO:
    uint32_t *time32Ptr = (uint32_t *) tsInsertTabletReq.timestamps.data();
    uint32_t tpmInt32 = time32Ptr[tsInsertTabletReq.timestamps.size() / 4 -1];
    u_char & p0 = *(((u_char*)&tpmInt32) + 3);
    u_char & p1 = *(((u_char*)&tpmInt32) + 2);
    u_char & p2 = *(((u_char*)&tpmInt32) + 1);
    u_char & p3 = *(((u_char*)&tpmInt32) );
    time32Ptr++;
    for (uint i = 0; i < tsInsertTabletReq.timestamps.size() / 8; ++i) {
        if (!(++p0)) {
            if (!(++p1)) {
                if (!(++p2)) {
                    ++p3;
                }
            }
        }
        *(time32Ptr) = tpmInt32;
        time32Ptr += 2;
    }

    //TODO:
//    int64_t *timePtr = (int64_t *) tsInsertTabletReq.timestamps.data();
//    int64_t tpmInt64;
//    char *p = (char*)&tpmInt64;
//    char c;
//    for (uint i = 0; i < tsInsertTabletReq.timestamps.size() / 8; ++i) {
//        tpmInt64 = startTs + i;
//        c = p[0];
//        p[0] = p[3];
//        p[3] = c;
//        c = p[1];
//        p[1] = p[2];
//        p[2] = c;
//
//        timePtr[i] = tpmInt64;
//    }



    sendInsertTablet(session, tsInsertTabletReq);

//    int rowIdx = 0;
//    for (int i = 0; i < workerCfg.batchSize; i++) {
//        tablet.timestamps[rowIdx++] = startTs + i;
//
//        if (rowIdx == (int) tablet.maxRowNumber) {
//            tablet.rowSize = rowIdx;
//            sendInsertTablet(session, tablet);
//            rowIdx = 0;
//        }
//    }
//
//    if (rowIdx != 0) {
//        tablet.rowSize = rowIdx;
//        sendInsertTablet(session, tablet);
//    }
}

void InsertTabletOperation2::insertTabletBatch2(shared_ptr<Session> &session, int sgIdx, int deviceIdx, int64_t startTs) {
    Tablet tablet(getPath(sgPrefix, sgIdx, deviceIdx), schemaList4Device, workerCfg.batchSize);
    for (int64_t i = 0; i < workerCfg.batchSize; i++) {
        size_t rowIdx = tablet.rowSize++;
        tablet.timestamps[rowIdx] = startTs + i;

        int randInt = rand();
        for (int sensorIdx = 0; sensorIdx < workerCfg.sensorNum; ++sensorIdx) {
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
                    float randFloat = 1.11;//randInt / 33.3;
                    tablet.addValue(sensorIdx, rowIdx, &randFloat);
                    break;
                }
                case TSDataType::DOUBLE: {
                    double randDouble = 2.22;//randInt / 99.9;
                    tablet.addValue(sensorIdx, rowIdx, &randDouble);
                    break;
                }
                case TSDataType::TEXT: {
                    string randStr = "ss"; //""str" + to_string(randInt);
                    tablet.addValue(sensorIdx, rowIdx, &randStr);
                    break;
                }
                case TSDataType::NULLTYPE:
                default:
                    break;
            }
        }

        if (tablet.rowSize == tablet.maxRowNumber) {
            sendInsertTablet(session, tablet);
            tablet.reset();
        }
    }

    if (tablet.rowSize != 0) {
        sendInsertTablet(session, tablet);
        tablet.reset();
    }
}

void InsertTabletOperation2::sendInsertTablet(shared_ptr<Session> &session, TSInsertTabletReq &tsInsertTabletReq){
    uint64_t pointCount =  tsInsertTabletReq.timestamps.size()/8u * workerCfg.sensorNum; //TODO:

    try {
        uint64_t startTimeUs = getTimeUs();
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

void InsertTabletOperation2::sendInsertTablet(shared_ptr<Session> &session, Tablet &tablet) {
    uint64_t pointCount = tablet.rowSize * workerCfg.sensorNum;

    try {
        uint64_t startTimeUs = getTimeUs();
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


