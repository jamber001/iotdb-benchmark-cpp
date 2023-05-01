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

#include <fstream>
#include "paramCfg.hpp"

using namespace std;


bool TaskCfg::extractCfg(const string &taskName, EasyCfgBase &config) {
    taskEnable = false;
    config.getParamBool("TASK_ENABLE", taskEnable, taskName); //optional parameter
    if (!taskEnable) {  //If task is disabled, need not parse other parameters.
        return true;
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

    string sensorDataTypeStr;
    config.getParamStr("SENSOR_DATA_TYPE", sensorDataTypeStr, taskName);    //optional parameter
    vector<string> dataTypeStrList;   //optional parameter
    config.parseValueBunch(sensorDataTypeStr, dataTypeStrList);
    textDataLen = config.getParamInt("TEXT_DATA_LEN", taskName);

    config.getParamStr("RECORD_INFO_FILE", recordInfoFile, taskName);    //optional parameter

    //== If RECORD_TYPE_FILE is valid, use it and ignore sensorNum, SENSOR_DATA_TYPE, textDataLen
    if (!recordInfoFile.empty()) {
        if (!genFieldInfo4OneRecordFromFile(recordInfoFile.c_str())) {
            return false;
        }
        if (sensorNum != (long long) fieldInfo4OneRecord.size()) {
//            log_warn("sensorNum(%d) is not consistent to RECORD_TYPE_FILE's sensor_num(%lld), use RECORD_TYPE_FILE's configuration.",
//                    sensorNum, fieldInfo4OneRecord.size());
            sensorNum = fieldInfo4OneRecord.size();
        }
    } else if (!dataTypeStrList.empty()) {
        if (!genFieldInfo4OneRecord(dataTypeStrList, textDataLen)) {
            return false;
        }
    } else {
        error_log("Invalid configure, both RECORD_TYPE_FILE and SENSOR_DATA_TYPE are empty.");
        return false;
    }

    batchSize = config.getParamInt("BATCH_SIZE", taskName);
    if (config.getParamStr("START_TIMESTAMP", taskName) == "NOW") {
        startTimestamp = getTimeUs() / 1000;
    } else {
        startTimestamp = config.getParamLL("START_TIMESTAMP", taskName);
    }
    loopNum = config.getParamLL("LOOP_NUM", taskName);
    loopIntervalMs = config.getParamInt("LOOP_INTERVAL_MS", taskName);

    return true;
}

//bool TaskCfg::genDataTypeList(const vector<string> &dataTypeStrList) {
//    for (const string &dataTypeStr: dataTypeStrList) {
//        TSDataType::TSDataType dataType;
//        if (!strToDataType(dataTypeStr, dataType)) {
//            return false;
//        }
//        dataTypeList.push_back(dataType);
//    }
//    return true;
//}

bool TaskCfg::genFieldInfo4OneRecordFromFile(const char *fileName) {
    string filePath = "../conf/";
    filePath += fileName;

    ifstream ifs(filePath, ifstream::in);

    if (ifs.fail()) {
        info_log("TaskCfg::genFieldInfo4OneRecordFromFile(), open file error. file=%s.", filePath.c_str());
        return false;
    }

    string line, section, key, value, tmpSection;
    getline(ifs, line);
    FieldInfo fieldInfo;
    while (!ifs.fail()) {
        int res = parserFieldInfo(line, fieldInfo);
        if (res < 0) {
            ifs.close();
            log_error("Invalid CFG in file: %s. Line: %s", fileName, line.c_str());
            return false;
        } else if (res > 0) {
            fieldInfo4OneRecord.push_back(fieldInfo);
        }
        getline(ifs, line);
    }

    bool res = true;
    if ((ifs.rdstate() & ifstream::eofbit) == 0) {
        info_log("TaskCfg::genFieldInfo4OneRecordFromFile(), read file error. file=%s", filePath.c_str());
        res = false;
    }

    ifs.close();
    return res;
}

bool TaskCfg::genFieldInfo4OneRecord(const vector<string> &dataTypeStrList, int textLen) {
    FieldInfo fieldInfo;
    for (int sensorIdx = 0; sensorIdx < sensorNum; sensorIdx++) {
        const string &dataTypeStr = dataTypeStrList[sensorIdx % dataTypeStrList.size()];
        TSDataType::TSDataType dataType;
        if (!strToDataType(dataTypeStr, dataType)) {
            return false;
        }

        fieldInfo.reset();
        char sensorName[32];
        snprintf(sensorName, sizeof(sensorName), "s%03d", sensorIdx);
        fieldInfo.sensorName = sensorName;
        fieldInfo.dataType = dataType;
        fieldInfo.encodeType = getDefaultEncodingType(dataType);
        fieldInfo.compressionType = getDefaultCompressionType(dataType);
        fieldInfo.textSize = textLen;
        fieldInfo.textPrefix = "";
        fieldInfo4OneRecord.push_back(fieldInfo);
    }

    return true;
}

void TaskCfg::lineToItems(const string &line, vector<string> &items) {
    const char *ptr = line.c_str();
    const char *pos1 = 0;
    bool inItem = false;
    while (*ptr != 0) {
        if (!inItem) {
            if (*ptr != ' ' && *ptr != '\t') {
                pos1 = ptr;
                inItem = true;
            }
        } else {
            if (*ptr == ' ' || *ptr == '\t') {
                items.emplace_back(string(pos1, ptr - pos1));
                inItem = false;
            }
        }
        ptr++;
    }

    if (inItem) {
        items.emplace_back(string(pos1, ptr - pos1));
    }
}


/**
 *
 * @param line
 * @param fieldInfo
 * @return  1 - have valid fieldInfo, 0 - no fieldInfo, -1 - error
 */
int TaskCfg::parserFieldInfo(string line, FieldInfo &fieldInfo) {
    fieldInfo.reset();

    size_t pos = line.find_first_of("#");
    if (string::npos != pos)
        line.erase(pos);

    //trimStr(line);
    vector<string> items;
    lineToItems(line, items);
    if (items.empty()) {
        return 0;
    }
    if (items.size() < 2) {  //If no SensorName + DataType
        return -1;
    }

    //== Parse: SensorName DataType EncodeType Compress TextSize TextPrefix
    //== Set SensorName ==
    fieldInfo.sensorName = items[0];

    //== Set dataType ==
    string &dataTypeStr = items[1];
    transform(dataTypeStr.begin(), dataTypeStr.end(), dataTypeStr.begin(), ::toupper);
    if (!strToDataType(dataTypeStr, fieldInfo.dataType)) {
        return -1;
    }

    //== Set encodeType ==
    if ((items.size() < 3) || (items[2] == "NULL")) {
        if (!getDefaultEncodingType(fieldInfo.dataType, fieldInfo.encodeType)) {
            return -1;
        }
    } else {
        string &encodeTypeStr = items[2];
        transform(encodeTypeStr.begin(), encodeTypeStr.end(), encodeTypeStr.begin(), ::toupper);
        if (!strToEncodingType(encodeTypeStr, fieldInfo.encodeType)) {
            return -1;
        }
    }

    //== Set compressionType ==
    if ((items.size() < 4) || (items[3] == "NULL")) {
        if (!getDefaultCompressionType(fieldInfo.dataType, fieldInfo.compressionType)) {
            return -1;
        }
    } else {
        string &compressionTypeStr = items[3];
        transform(compressionTypeStr.begin(), compressionTypeStr.end(), compressionTypeStr.begin(), ::toupper);
        if (!strToCompressionType(compressionTypeStr, fieldInfo.compressionType)) {
            return -1;
        }
    }

    //== Set TextSize ==
    if ((items.size() >= 5) && (items[4] != "NULL")) {
        fieldInfo.textSize = atoi(items[4].c_str());
    } else {
        fieldInfo.textSize = 0;
    }

    //== Set TextPrefix ==
    if (items.size() >= 6) {
        fieldInfo.textPrefix = items[5];
    }

    return 1;
}


bool TaskCfg::strToDataType(const string &typeStr, TSDataType::TSDataType &dataType) {
    static unordered_map<string, TSDataType::TSDataType> mapStr2Type = {
            {"BOOLEAN",  TSDataType::BOOLEAN},
            {"INT32",    TSDataType::INT32},
            {"INT64",    TSDataType::INT64},
            {"FLOAT",    TSDataType::FLOAT},
            {"DOUBLE",   TSDataType::DOUBLE},
            {"TEXT",     TSDataType::TEXT},
            {"VECTOR",   TSDataType::VECTOR},
            {"NULLTYPE", TSDataType::NULLTYPE}
    };

    auto itr = mapStr2Type.find(typeStr);
    if (itr == mapStr2Type.end()) {
        error_log("Invalid DataType: %s.", typeStr.c_str());
        return false;
    }

    dataType = itr->second;
    return true;
}

bool TaskCfg::strToEncodingType(const string &encodeTypeStr, TSEncoding::TSEncoding &encodingType) {
    static unordered_map<string, TSEncoding::TSEncoding> str2EncodeType = {
            {"PLAIN",      TSEncoding::PLAIN},
            {"DICTIONARY", TSEncoding::DICTIONARY},
            {"RLE",        TSEncoding::RLE},
            {"DIFF",       TSEncoding::DIFF},
            {"TS_2DIFF",   TSEncoding::TS_2DIFF},
            {"BITMAP",     TSEncoding::BITMAP},
            {"GORILLA_V1", TSEncoding::GORILLA_V1},
            {"REGULAR",    TSEncoding::REGULAR},
            {"GORILLA",    TSEncoding::GORILLA},
            {"ZIGZAG",     TSEncoding::ZIGZAG},
            {"FREQ",       TSEncoding::FREQ}
    };

    auto itr = str2EncodeType.find(encodeTypeStr);
    if (itr == str2EncodeType.end()) {
        error_log("Invalid EncodeType: %s", encodeTypeStr.c_str());
        return false;
    };

    encodingType = itr->second;
    return true;
}

bool
TaskCfg::strToCompressionType(const string &compressionTypeStr, CompressionType::CompressionType &compressionType) {
    static unordered_map<string, CompressionType::CompressionType> str2CompressType = {
            {"UNCOMPRESSED", CompressionType::UNCOMPRESSED},
            {"SNAPPY",       CompressionType::SNAPPY},
            {"GZIP",         CompressionType::GZIP},
            {"LZO",          CompressionType::LZO},
            {"SDT",          CompressionType::SDT},
            {"PAA",          CompressionType::PAA},
            {"PLA",          CompressionType::PLA},
            {"LZ4",          CompressionType::LZ4}
    };

    auto itr = str2CompressType.find(compressionTypeStr);
    if (itr == str2CompressType.end()) {
        error_log("Invalid compressionType: %s", compressionTypeStr.c_str());
        return false;
    };

    compressionType = itr->second;
    return true;
}

bool TaskCfg::getDefaultEncodingType(TSDataType::TSDataType dataType, TSEncoding::TSEncoding &encodingType) {
    static unordered_map<int, TSEncoding::TSEncoding> dataType2Encoding = {
            {TSDataType::BOOLEAN,  TSEncoding::RLE},
            {TSDataType::INT32,    TSEncoding::RLE},
            {TSDataType::INT64,    TSEncoding::RLE},
            {TSDataType::FLOAT,    TSEncoding::GORILLA},
            {TSDataType::DOUBLE,   TSEncoding::GORILLA},
            {TSDataType::TEXT,     TSEncoding::PLAIN},
            {TSDataType::VECTOR,   TSEncoding::PLAIN},
            {TSDataType::NULLTYPE, TSEncoding::PLAIN},
    };

    auto itr = dataType2Encoding.find (dataType);
    if (itr == dataType2Encoding.end() ) {
        error_log("Invalid DataType: %d", dataType);
        return false;
    }

    encodingType = itr->second;
    return true;
}

TSEncoding::TSEncoding TaskCfg::getDefaultEncodingType(TSDataType::TSDataType dataType) {
    TSEncoding::TSEncoding encodingType;
    if (getDefaultEncodingType(dataType, encodingType)) {
        return encodingType;
    }
    return TSEncoding::PLAIN;
}

bool TaskCfg::getDefaultEncodingType(const string &typeStr, TSEncoding::TSEncoding &encodingType) {
    static unordered_map<string, TSEncoding::TSEncoding> mapStr2Type = {
            {"BOOLEAN",  TSEncoding::RLE},
            {"INT32",    TSEncoding::RLE},
            {"INT64",    TSEncoding::RLE},
            {"FLOAT",    TSEncoding::GORILLA},
            {"DOUBLE",   TSEncoding::GORILLA},
            {"TEXT",     TSEncoding::PLAIN},
            {"NULLTYPE", TSEncoding::PLAIN},
    };

    auto itr = mapStr2Type.find (typeStr);
    if ( itr == mapStr2Type.end() ) {
        error_log("invalid DataType: %s", typeStr.c_str());
        return false;
    }

    encodingType = itr->second;
    return true;
};

bool TaskCfg::getDefaultCompressionType(const TSDataType::TSDataType dataType, CompressionType::CompressionType &compressionType) {
    static unordered_map<int, CompressionType::CompressionType> dataType2Compress = {
            {TSDataType::BOOLEAN,  CompressionType::SNAPPY},
            {TSDataType::INT32,    CompressionType::SNAPPY},
            {TSDataType::INT64,    CompressionType::SNAPPY},
            {TSDataType::FLOAT,    CompressionType::SNAPPY},
            {TSDataType::DOUBLE,   CompressionType::SNAPPY},
            {TSDataType::TEXT,     CompressionType::SNAPPY},
            {TSDataType::VECTOR,   CompressionType::SNAPPY},
            {TSDataType::NULLTYPE, CompressionType::SNAPPY},
    };

    auto itr = dataType2Compress.find (dataType);
    if (itr == dataType2Compress.end() ) {
        error_log("invalid typeStr=%d", dataType);
        return false;
    }

    compressionType = itr->second;
    return true;
}

CompressionType::CompressionType TaskCfg::getDefaultCompressionType(const TSDataType::TSDataType dataType) {
    CompressionType::CompressionType compressionType;
    if (getDefaultCompressionType(dataType, compressionType)) {
        return compressionType;
    }
    return CompressionType::UNCOMPRESSED;
}

bool TaskCfg::getDefaultCompressionType(const string &typeStr, CompressionType::CompressionType &compressionType) {
    static unordered_map<string, CompressionType::CompressionType> mapStr2Type = {
            {"BOOLEAN",  CompressionType::SNAPPY},
            {"INT32",    CompressionType::SNAPPY},
            {"INT64",    CompressionType::SNAPPY},
            {"FLOAT",    CompressionType::SNAPPY},
            {"DOUBLE",   CompressionType::SNAPPY},
            {"TEXT",     CompressionType::SNAPPY},
            {"NULLTYPE", CompressionType::SNAPPY},
    };

    auto itr = mapStr2Type.find (typeStr);
    if ( itr == mapStr2Type.end() ) {
        error_log("invalid typeStr=%s", typeStr.c_str());
        return false;
    }

    compressionType = itr->second;
    return true;
}


void TaskCfg::printCfg() const {
    printf("   taskName=%s\n", taskName.c_str());
    printf("   taskType=%s\n", taskType.c_str());
    printf("   timeAlignedEnable=%s\n", timeAlignedEnable ? "True" : "False");
    printf("   tagsEnable=%s\n", tagsEnable ? "True" : "False");
    printf("   workMode=%d\n", workMode);
    printf("   sessionNum=%d\n", sessionNum);
    printf("   storageGroupNum=%d\n", storageGroupNum);
    printf("   deviceNum=%d\n", deviceNum);
    printf("   sensorNum=%d\n", sensorNum);
    printf("   recordInfoFile=%s\n", recordInfoFile.c_str());
    printf("   batchSize=%d\n", batchSize);
    printf("   loopNum=%lld\n", loopNum);
}
