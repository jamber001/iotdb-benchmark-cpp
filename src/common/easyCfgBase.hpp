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

#ifndef EASY_CFG_BASE_HPP
#define EASY_CFG_BASE_HPP
#include <string>
#include <vector>
#include <map>
#include <mutex>
#include "easyLog.hpp"

using namespace std;

class EasyCfgBase {
public:
    EasyCfgBase(const string &cfgFileName, bool isRelativepath = true);

    // static EasyCfgBase* GetInstance();
    static bool GetExePath(string &path);
    static bool ParseParamList(const string &listStr, vector<string> &out);

    bool GetParamStr(const string &paramName, string &value);     //get parameter as string
    string GetParamStr(const string &paramName);

    bool GetParamLL(const string &paramName, long long &value);   //get parameter as Long Long
    long long GetParamLL(const string &paramName);

    bool GetParamInt(const string &paramName, int &value);        //get parameter as Int
    int GetParamInt(const string &paramName);        //get parameter as Int

    bool GetParamBool(const string &paramName, bool &value);     //get parameter as Bool
    bool GetParamBool(const string &paramName);

    bool GetParamDouble(const string &paramName, double &value);  //get parameter as Double

    bool InsertParamStr(const string &paramName, const string &value, bool notReplace = false);

    void printAllParam();       //print all parameters to stdout, mainly for testing

    virtual ~EasyCfgBase() {};

private:
    EasyCfgBase(const EasyCfgBase &s);

    EasyCfgBase &operator=(EasyCfgBase &s);

    bool readConfigFile(const char *fileName);

    bool parserKV(string line, string &key, string &value);

    string trim(const string &s);

protected:
    //static EasyCfgBase* _instance;
    string _cfgFile;
    map <string, string> _key2Value;
};


#endif //EASY_CFG_BASE_HPP