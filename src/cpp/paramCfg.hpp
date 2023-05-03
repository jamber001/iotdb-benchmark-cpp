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
#include "Session.h"

using namespace std;

struct ServerCfg {
    string host;
    int port;
    string user="root";
    string passwd="root";
    bool rpcCompression=false;
};


struct FieldInfo {
    string sensorName;
    TSDataType::TSDataType dataType;
    TSEncoding::TSEncoding encodeType;
    CompressionType::CompressionType compressionType;
    int textSize;
    string textPrefix;

    FieldInfo() {
        reset();
    };
    void reset() {
        sensorName.clear();
        dataType = TSDataType::NULLTYPE;
        encodeType = TSEncoding::PLAIN;
        compressionType = CompressionType::UNCOMPRESSED;
        textSize = 0;
        textPrefix.clear();
    }
};

class TaskCfg {
public:
    string taskName;
    bool taskEnable;
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
    //vector<TSDataType::TSDataType> dataTypeList;
    int textDataLen = 2;     //optional parameter
    string recordInfoFile;   //optional parameter
    vector<FieldInfo> fieldInfo4OneRecord;
    int batchSize = 100;
    int64_t startTimestamp;

    long long loopNum;
    int loopIntervalMs;

public:
    bool extractCfg(const string &taskName, EasyCfgBase &config);
    bool genFieldInfo4OneRecordFromFile(const char *fileName);
    bool genFieldInfo4OneRecord(const vector<string> &dataTypeStrList, int textLen);
    void printCfg() const;

    static bool strToDataType(const string &typeStr, TSDataType::TSDataType &dataType);
    static bool strToEncodingType(const string &encodeTypeStr, TSEncoding::TSEncoding &encodingType);
    static bool strToCompressionType(const string &compressionTypeStr, CompressionType::CompressionType &compressionType);

    static bool getDefaultEncodingType(TSDataType::TSDataType dataType, TSEncoding::TSEncoding &encodingType) ;
    static TSEncoding::TSEncoding getDefaultEncodingType(TSDataType::TSDataType dataType);
    static bool getDefaultEncodingType(const string &dataTypeStr, TSEncoding::TSEncoding &encodingType) ;

    static bool getDefaultCompressionType(TSDataType::TSDataType dataType, CompressionType::CompressionType &compressionType) ;
    static CompressionType::CompressionType getDefaultCompressionType(const TSDataType::TSDataType dataType);
    static bool getDefaultCompressionType(const string &dataTypeStr, CompressionType::CompressionType &compressionType) ;

private:
    int parserFieldInfo(string line, FieldInfo &fieldInfo);
    void lineToItems(const string &line, vector<string> &itemsList);

};

#endif //PARAMCFG_HPP
