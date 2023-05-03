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

#include "easyCfgBase.hpp"
#include "easyLog.hpp"
#include <fstream>
#include <stdlib.h>
#include <unistd.h>
#include <sys/param.h>
#include <algorithm>
#include <mutex>
#include <iostream>

using namespace std;

//EasyCfgBase* EasyCfgBase::_instance = 0;

// EasyCfgBase *EasyCfgBase::GetInstance()
// {
//     if (_instance)
//     {
//         return _instance;
//     }

//     static mutex mtx;
//     lock_guard<mutex> lockGuard(mtx);

//     if (_instance)
//     {
//         return _instance;
//     }

//     try
//     {
//         _instance = new EasyCfgBase;
//     }
//     catch (const std::exception &e)
//     {
//         info_log("EasyCfgBase::GetInstance(), new EasyCfgBase error. Exception: %s.", e.what());
//         return NULL;
//     }

//     return _instance;
// }

EasyCfgBase::EasyCfgBase(const string &cfgFileName, bool isRelativePath /*= true*/) {
    if (isRelativePath) {
        string exePath;
        if (!getExePath(exePath)) {
            throw exception();
        }
        _cfgFile = exePath + "/" + cfgFileName;
    } else {
        _cfgFile = cfgFileName;
    }

    if (!readConfigFile(_cfgFile.c_str())) {
        throw exception();
    }
}

bool EasyCfgBase::readConfigFile(const char *fileName) {
    ifstream ifs(fileName, ifstream::in);

    if (ifs.fail()) {
        info_log("EasyCfgBase::readConfigFile(), open file error. file=%s", fileName);
        return false;
    }

    string line, section, key, value, tmpSection;
    getline(ifs, line);
    while (!ifs.fail()) {
        if (parserSKV(line, tmpSection, key, value)) {
            if (!tmpSection.empty()) {
                section = tmpSection;
                if (_skvMap.find(section) == _skvMap.end()) {
                    _sectionList.push_back(section);
                    _skvMap[section];
                }
            } else {
                _skvMap[section][key] = value;
            }
        }
        getline(ifs, line);
    }

    bool res = true;
    if ((ifs.rdstate() & ifstream::eofbit) == 0) {
        info_log("EasyCfgBase::readConfigFile(), read file error. file=%s", fileName);
        res = false;
    }

    ifs.close();
    return res;
}

string EasyCfgBase::trim(const string &s) {
    std::size_t pos1 = s.find_first_not_of(" \t");
    if (string::npos == pos1)
        return "";

    std::size_t pos2 = s.find_last_not_of(" \t");

    return s.substr(pos1, pos2 - pos1 + 1);
}

bool EasyCfgBase::parserKV(string line, string &key, string &value) {
    key.clear();
    value.clear();

    size_t pos = line.find_first_of("#");
    if (string::npos != pos)
        line.erase(pos);

    line = trim(line);

    pos = line.find_first_of("=");
    if (string::npos == pos) {
        return false;
    }

    key = trim(line.substr(0, pos));
    value = trim(line.substr(pos + 1));
    if (key.empty()) {
        return false;
    }

    //cout << "parserKV(), " << line << " : " << key << "=" << value << endl;
    return true;
}

bool EasyCfgBase::parserSKV(string line, string &section, string &key, string &value) {
    section.clear();
    key.clear();
    value.clear();

    size_t pos = line.find_first_of("#");
    if (string::npos != pos)
        line.erase(pos);

    line = trim(line);
    if (line.size() <=2) {
        return false;
    }

    if ((line[0] == '[') && ((*line.rbegin()) == ']')) {
        section = line.substr(1, line.size()-2);
        return true;
    }

    pos = line.find_first_of("=");
    if (string::npos == pos) {
        return false;
    }

    key = trim(line.substr(0, pos));
    value = trim(line.substr(pos + 1));
    if (key.empty()) {
        return false;
    }

    //cout << "parserKV(), " << line << " : " << key << "=" << value << endl;
    return true;
}

void EasyCfgBase::getSectionList(vector<string> &out) {
    out = _sectionList;
}

bool EasyCfgBase::getKVMap(map<string, string> &out, const string &section /* = "" */) {
    out.clear();

    map <string, map<string,string>>::iterator itr1;
    itr1 = _skvMap.find(section);
    if (_skvMap.end() == itr1 ) {
        return  false;
    }

    out = itr1->second;
    return true;
}


bool EasyCfgBase::getParamStr(const string &paramName, string &value, const string &section /* ="" */) {
    map <string, map<string,string>>::iterator itr1 = _skvMap.find(section);
    if (_skvMap.end() == itr1) {
        return false;
    }

    map<string, string>::iterator itr2 = itr1->second.find(paramName);
    if (itr1->second.end() == itr2) {
        return false;
    }

    value = itr2->second;
    return true;
}

string EasyCfgBase::getParamStr(const string &paramName, const string &section /* ="" */ ) {
    string value;
    if (getParamStr(paramName, value, section)) {
        return value;
    }

    info_log("EasyCfgBase::getParamStr(), fatal error, abort(). Miss section=%s, paramName=%s", section.c_str(),
             paramName.c_str());
    abort();
}

bool EasyCfgBase::getParamLL(const string &paramName, long long &value, const string &section /* ="" */) {
    string strValue;
    if (getParamStr(paramName, strValue, section)) {
        value = atoll(strValue.c_str());
        return true;
    }

    return false;
}

long long EasyCfgBase::getParamLL(const string &paramName, const string &section /* ="" */) {
    long long value = 0;
    if (getParamLL(paramName, value, section)) {
        return value;
    }

    info_log("EasyCfgBase::getParamLL(), fatal error, abort(). Miss section=%s, paramName=%s", section.c_str(),
             paramName.c_str());
    abort();
}

bool EasyCfgBase::getParamInt(const string &paramName, int &value, const string &section /* ="" */) {
    string strValue;
    if (getParamStr(paramName, strValue, section)) {
        value = atoi(strValue.c_str());
        return true;
    }

    return false;
}

int EasyCfgBase::getParamInt(const string &paramName, const string &section /* ="" */) {
    int value = 0;
    if (getParamInt(paramName, value, section)) {
        return value;
    }

    info_log("EasyCfgBase::getParamInt(), fatal error, abort(). Miss section=%s, paramName=%s", section.c_str(),
             paramName.c_str());
    abort();
    return 0;
}

bool EasyCfgBase::getParamBool(const string &paramName, bool &value, const string &section /* ="" */) {
    string strValue;
    if (getParamStr(paramName, strValue, section)) {
        transform(strValue.begin(), strValue.end(), strValue.begin(), ::tolower);

        if ((strValue == "true") || (strValue == "yes") || (strValue == "on")) {
            value = true;
            return true;
        } else if ((strValue == "false") || (strValue == "no") || (strValue == "off")) {
            value = false;
            return true;
        } else {
            return false;
        }
    }

    return false;
}

bool EasyCfgBase::getParamBool(const string &paramName, const string &section /* ="" */) {
    bool value = false;
    if (getParamBool(paramName, value, section)) {
        return value;
    }

    info_log("EasyCfgBase::getParamBool(), fatal error, abort(). Miss section=%s, paramName=%s", section.c_str(),
             paramName.c_str());
    abort();
}

bool EasyCfgBase::getParamDouble(const string &paramName, double &value, const string &section /* ="" */) {
    string strValue;
    if (getParamStr(paramName, strValue, section)) {
        value = atof(strValue.c_str());
        return true;
    }

    return false;
}

double EasyCfgBase::getParamDouble(const string &paramName, const string &section /* ="" */) {
    double value = 0.0;
    if (getParamDouble(paramName, value, section)) {
        return value;
    }

    info_log("EasyCfgBase::getParamDouble(), fatal error, abort(). Miss section=%s, paramName=%s", section.c_str(),
             paramName.c_str());
    abort();
}

bool EasyCfgBase::insertParamStr(const string &paramName, const string &value, const string &section, bool notReplace /*= false*/) {
    map<string, map<string, string>>::iterator itr1 = _skvMap.find(section);
    if (notReplace) {
        if (itr1 != _skvMap.end()) {
            map<string, string>::iterator itr2 = itr1->second.find(paramName);
            if (itr2 != itr1->second.end()) {
                return false;   //not replace
            }
        }
    }

    if (_skvMap.end() == itr1) {
        _sectionList.push_back(section);
    }

    _skvMap[section][paramName] = value;
    return true;
}

void EasyCfgBase::printAllParam() {
    vector<string>::iterator itr1;
    for (itr1 = _sectionList.begin(); itr1 != _sectionList.end(); itr1++) {
        cout << "[" << (*itr1) << "]" << endl;
        map<string, string>::iterator itr2;
        for (itr2 = _skvMap[*itr1].begin(); itr2 != _skvMap[*itr1].end(); itr2++) {
            cout << itr2->first << " = " << itr2->second << endl;
        }
        cout << endl;
    }
}

bool EasyCfgBase::getExePath(string &path) {
    char buf[MAXPATHLEN];
    char proc[32];
    sprintf(proc, "/proc/%d/exe", getpid());
    int ret = readlink(proc, buf, sizeof(buf));
    if (ret <= 0) {
        error_log("EasyCfgBase::getExePath(), readlink error, ret=%d, path=%s.", ret, proc);
        return false;
    }

    //buf[ret] = '\0';    //optional for std::string
    path.assign(buf, ret);

    size_t pos = path.rfind('/');
    if (string::npos == pos) {
        error_log("EasyCfgBase::getExePath(), exe path has no '/'. path=%s.", path.c_str());
        return false;
    }
    path.erase(pos);

    return true;
}

bool EasyCfgBase::parseValueBunch(const string &listStr, vector<string> &out) {
    out.clear();

    size_t pos1 = 0;
    size_t pos2 = 0;
    while ((pos2 = listStr.find(":", pos1)) != string::npos) {
        if (pos2 > pos1) {
            out.push_back(listStr.substr(pos1, pos2 - pos1));
        }
        pos1 = pos2 + 1;
    }

    if (pos1 < listStr.size()) {
        out.push_back(listStr.substr(pos1));
    }

    return true;
}


