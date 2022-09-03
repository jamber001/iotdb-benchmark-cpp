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
        if (!GetExePath(exePath)) {
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

    string line, key, value;
    getline(ifs, line);
    while (!ifs.fail()) {
        //cout << line << endl;
        if (parserKV(line, key, value)) {
            _key2Value[key] = value;
            //cout << "createInstance(), " << line << " : " << key << "=" << value << endl;
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

bool EasyCfgBase::GetParamStr(const string &paramName, string &value) {
    map<string, string>::iterator itr = _key2Value.find(paramName);
    if (itr != _key2Value.end()) {
        value = itr->second;
        return true;
    }

    return false;
}

string EasyCfgBase::GetParamStr(const string &paramName) {
    string value;
    if (GetParamStr(paramName, value)) {
        return value;
    }

    info_log("EasyCfgBase::GetParamStr(), fatal error, abort(). Miss paramName=%s", paramName.c_str());
    abort();
    return value;
}

bool EasyCfgBase::GetParamLL(const string &paramName, long long &value) {
    string strValue;
    if (GetParamStr(paramName, strValue)) {
        value = atoll(strValue.c_str());
        return true;
    }

    return false;
}

long long EasyCfgBase::GetParamLL(const string &paramName) {
    long long value = 0;
    if (GetParamLL(paramName, value)) {
        return value;
    }

    info_log("EasyCfgBase::GetParamLL(), fatal error, abort(). Miss paramName=%s", paramName.c_str());
    abort();
    return 0;
}

bool EasyCfgBase::GetParamInt(const string &paramName, int &value) {
    string strValue;
    if (GetParamStr(paramName, strValue)) {
        value = atoi(strValue.c_str());
        return true;
    }

    return false;
}

int EasyCfgBase::GetParamInt(const string &paramName) {
    int value = 0;
    if (GetParamInt(paramName, value)) {
        return value;
    }

    info_log("EasyCfgBase::GetParamInt(), fatal error, abort(). Miss paramName=%s", paramName.c_str());
    abort();
    return 0;
}

bool EasyCfgBase::GetParamBool(const string &paramName, bool &value) {
    string strValue;
    if (GetParamStr(paramName, strValue)) {
        transform(strValue.begin(), strValue.end(), strValue.begin(), ::tolower);

        if (strValue == "true") {
            value = true;
            return true;
        } else if (strValue == "false") {
            value = false;
            return true;
        } else {
            return false;
        }
    }

    return false;
}

bool EasyCfgBase::GetParamBool(const string &paramName) {
    bool value = false;
    if (GetParamBool(paramName, value)) {
        return value;
    }

    info_log("EasyCfgBase::GetParamBool(), fatal error, abort(). Miss paramName=%s", paramName.c_str());
    abort();
    return 0;
}

bool EasyCfgBase::GetParamDouble(const string &paramName, double &value) {
    string strValue;
    if (GetParamStr(paramName, strValue)) {
        value = atof(strValue.c_str());
        return true;
    }

    return false;
}


bool EasyCfgBase::InsertParamStr(const string &paramName, const string &value, bool notReplace /*= false*/) {
    if (notReplace) {
        map<string, string>::iterator itr = _key2Value.find(paramName);
        if (_key2Value.end() == itr) {
            _key2Value[paramName] = value;
        }
    } else {
        _key2Value[paramName] = value;
    }

    return true;
}

void EasyCfgBase::printAllParam() {
    map<string, string>::iterator itr;
    for (itr = _key2Value.begin(); itr != _key2Value.end(); itr++) {
        cerr << itr->first << " = " << itr->second << endl;
    }
}

bool EasyCfgBase::GetExePath(string &path) {
    char buf[MAXPATHLEN];
    char proc[32];
    sprintf(proc, "/proc/%d/exe", getpid());
    int ret = readlink(proc, buf, sizeof(buf));
    if (ret <= 0) {
        info_log("EasyCfgBase::GetExePath(), readlink error, ret=%d, path=%s.", ret, proc);
        return false;
    }

    //buf[ret] = '\0';    //optional for std::string
    path.assign(buf, ret);

    size_t pos = path.rfind('/');
    if (string::npos == pos) {
        info_log("EasyCfgBase::GetExePath(), exe path has no '/'. path=%s.", path.c_str());
        return false;
    }
    path.erase(pos);

    return true;
}

bool EasyCfgBase::ParseParamList(const string &listStr, vector<string> &out) {
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
