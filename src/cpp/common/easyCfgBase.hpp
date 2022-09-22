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
#include <set>
#include "easyLog.hpp"

using namespace std;

class EasyCfgBase {
public:
    EasyCfgBase(const string &cfgFileName, bool isRelativepath = true);

    // static EasyCfgBase* GetInstance();
    static bool getExePath(string &path);

    // Parse value bunch. e.g. "123:abc:true" => ["123", "abc", "true"]
    static bool parseValueBunch(const string &listStr, vector<string> &out);

    void getSectionList(vector<string> &out);
    bool getKVMap(map<string, string> &out, const string &section = "");


    bool getParamStr(const string &paramName, string &value, const string &section = "");
    string getParamStr(const string &paramName, const string &section = "");

    bool getParamLL(const string &paramName, long long &value, const string &section = "");   //get parameter as Long Long
    long long getParamLL(const string &paramName, const string &section = "");

    bool getParamInt(const string &paramName, int &value, const string &section = "");        //get parameter as Int
    int getParamInt(const string &paramName, const string &section = "");

    bool getParamBool(const string &paramName, bool &value, const string &section = "");     //get parameter as Bool
    bool getParamBool(const string &paramName, const string &section = "");

    bool getParamDouble(const string &paramName, double &value, const string &section = "");  //get parameter as Double
    double getParamDouble(const string &paramName, const string &section = "");


    bool insertParamStr(const string &paramName, const string &value, const string &section, bool notReplace = false);

    void printAllParam();       //print all parameters to stdout, mainly for testing

    virtual ~EasyCfgBase() {};

private:
    EasyCfgBase(const EasyCfgBase &s);

    EasyCfgBase &operator=(EasyCfgBase &s);

    bool readConfigFile(const char *fileName);

    bool parserKV(string line, string &key, string &value);
    bool parserSKV(string line, string &section, string &key, string &value);

    string trim(const string &s);

protected:
    //static EasyCfgBase* _instance;
    string _cfgFile;
    vector<string> _sectionList;   //for saving original section sequence
    map<string, map<string,string>> _skvMap;  //Section => Key => Value
};


#endif //EASY_CFG_BASE_HPP