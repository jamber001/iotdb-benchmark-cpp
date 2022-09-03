//
// Created by haiyi.zb on 8/30/22.
//

#ifndef PARAMCFG_HPP
#define PARAMCFG_HPP

#include <string>
#include <vector>
using namespace std;

struct ServerCfg {
    string host;
    int port;
    string user;
    string passwd;
};


struct WorkerCfg {
    int sessionNum;
    int storageGroupNum;
    int deviceNum;
    int sensorNum;
    vector<string> dataTypeList;
    int batchSize;
    int64_t startTimestamp;

    int loopIntervalMs;
    int loopNum;
};

#endif //PARAMCFG_HPP
