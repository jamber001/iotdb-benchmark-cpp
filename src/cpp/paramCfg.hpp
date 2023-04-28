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


struct TaskCfg {
    string taskName;
    string taskType;

    int workMode = 0;    //optional parameter
    bool createSchema = true;        //optional parameter
    bool timeAlignedEnable = false;  //optional parameter
    bool tagsEnable = false;         //optional parameter
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


    bool extractCfg(const string &taskName, EasyCfgBase &config) {
        bool enable = false;
        if (!config.getParamBool("TASK_ENABLE", enable, taskName)) {
            return false;
        };
        if (!enable) {
            return false;
        }

        this->taskName = taskName;
        taskType = config.getParamStr("TASK_TYPE", taskName);
        workMode = 0;
        config.getParamInt("WORK_MODE", workMode, taskName);  //optional parameter
        createSchema = true;
        config.getParamBool("CREATE_SCHEMA", createSchema, taskName);  //optional parameter
        timeAlignedEnable = false;
        config.getParamBool("TIME_ALIGNED_ENABLE", timeAlignedEnable, taskName);  //optional parameter
        tagsEnable = false;
        config.getParamBool("TAGS_ENABLE", tagsEnable, taskName);  //optional parameter

        sessionNum = config.getParamInt("SESSION_NUMBER", taskName);
        config.getParamLL("SG_TTL", sgTTL, taskName);    //optional parameter
        storageGroupNum = config.getParamInt("SG_NUMBER", taskName);
        deviceNum = config.getParamInt("DEVICE_NUMBER", taskName);
        sensorNum = config.getParamInt("SENSOR_NUMBER", taskName);
        string sensorDataTypeStr = config.getParamStr("SENSOR_DATA_TYPE", taskName);
        config.parseValueBunch(sensorDataTypeStr, dataTypeList);
        if (dataTypeList.size() <= 0) {
            error_log("Invalid configure %s=%s", "SENSOR_DATA_TYPE", sensorDataTypeStr.c_str());
            return false;
        }
        textDataLen = config.getParamInt("TEXT_DATA_LEN", taskName);
        batchSize = config.getParamInt("BATCH_SIZE", taskName);
        if (config.getParamStr("START_TIMESTAMP", taskName) == "NOW" ) {
            startTimestamp = getTimeUs() / 1000;
        } else {
            startTimestamp = config.getParamLL("START_TIMESTAMP", taskName);
        }
        loopNum = config.getParamLL("LOOP_NUM", taskName);
        loopIntervalMs = config.getParamInt("LOOP_INTERVAL_MS", taskName);

        return true;
    }

    void printCfg() const {
        printf("   taskName=%s\n", taskName.c_str());
        printf("   taskType=%s\n", taskType.c_str());
        printf("   timeAlignedEnable=%s\n", timeAlignedEnable ? "True" : "False");
        printf("   tagsEnable=%s\n", tagsEnable ? "True" : "False");
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
