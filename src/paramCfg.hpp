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

#ifndef PARAMCFG_HPP
#define PARAMCFG_HPP

#include <string>
#include <vector>
#include "easyCfgBase.hpp"
#include "easyUtility.hpp"

using namespace std;

struct ServerCfg {
    string host;
    int port;
    string user="root";
    string passwd="root";
    bool rpcCompression=false;
};


struct WorkerCfg {
    int workMode = 0;    //optional parameter
    int sessionNum = 5;

    long long sgTTL = 0;  //optional parameter
    int storageGroupNum;
    int deviceNum;
    int sensorNum;
    vector<string> dataTypeList;
    int textDataLen = 2;     //optional parameter
    int batchSize = 100;
    int64_t startTimestamp;

    long long loopNum;
    int loopIntervalMs;


    bool extractCfg(EasyCfgBase &config, const string &prefix) {
        bool enable = false;
        if (!config.GetParamBool(prefix + "ENABLE", enable)) {
            return false;
        };
        if (!enable) {
            return false;
        }

        workMode = 0;
        config.GetParamInt(prefix + "WORK_MODE", workMode); //optional parameter

        sessionNum = config.GetParamInt(prefix + "SESSION_NUMBER");
        config.GetParamLL(prefix + "SG_TTL", sgTTL);    //optional parameter
        storageGroupNum = config.GetParamInt(prefix + "SG_NUMBER");
        deviceNum = config.GetParamInt(prefix + "DEVICE_NUMBER");
        sensorNum = config.GetParamInt(prefix + "SENSOR_NUMBER");
        string sensorDataTypeStr = config.GetParamStr(prefix + "SENSOR_DATA_TYPE");
        config.ParseParamList(sensorDataTypeStr, dataTypeList);
        if (dataTypeList.size() <= 0) {
            error_log("Invalid configure %s=%s", (prefix + "SENSOR_DATA_TYPE").c_str(), sensorDataTypeStr.c_str());
            return false;
        }
        textDataLen = config.GetParamInt(prefix + "TEXT_DATA_LEN");
        batchSize = config.GetParamInt(prefix + "BATCH_SIZE");
        if ( config.GetParamStr(prefix + "START_TIMESTAMP") == "NOW" ) {
            startTimestamp = getTimeUs() / 1000;
        } else {
            startTimestamp = config.GetParamLL(prefix + "START_TIMESTAMP");
        }
        loopIntervalMs = config.GetParamInt(prefix + "LOOP_INTERVAL_MS");
        loopNum = config.GetParamLL(prefix + "LOOP_NUM");

        return true;
    }

    void printCfg() const {
        printf("   workMode=%d\n", workMode);
        printf("   sessionNum=%d\n", sessionNum);
        printf("   storageGroupNum=%d\n", storageGroupNum);
        printf("   deviceNum=%d\n", deviceNum);
        printf("   sensorNum=%d\n", sensorNum);
        printf("   batchSize=%d\n", batchSize);
        printf("   loopNum=%lld\n", loopNum);
    }
};

#endif //PARAMCFG_HPP
