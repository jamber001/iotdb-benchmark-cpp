#include <string>
#include <thread>
#include <iostream>
#include <mutex>
#include <unistd.h>

#include "easyLog.hpp"
#include "easyCfgBase.hpp"
#include "paramCfg.hpp"
#include "easyUtility.hpp"
#include "Session.h"

#include "operationBase.hpp"
#include "insertTabletsOperation.hpp"
#include "insertTabletOperation.hpp"
#include "insertTabletOperation2.hpp"


using namespace std;

bool cleanAllSG(Session &session) {
//    vector<string> storageGroups;
//    storageGroups.emplace_back("root.cpp");
//    try {
//    session.deleteStorageGroups(storageGroups);
//    } catch (const exception &e) {
//        if (string(e.what()).find("Path [root.cpp] does not exist") != string::npos) {
//            return true;
//        }
//        info_log("cleanAllSG(), error: %s", e.what());
//        return false;
//    }
//    return true;


    string DELETE_ALL_SG_SQL = "delete storage group root.**";
    //string DELETE_ALL_SG_SQL = "delete storage group root.cpp.**;";

    try {
        std::unique_ptr <SessionDataSet> dataset = session.executeQueryStatement(DELETE_ALL_SG_SQL);
        cout << "00" << endl;
        bool isExisted = dataset->hasNext();
        cout << "11" << endl;
        dataset->closeOperationHandle();
        cout << "22" << endl;
        return isExisted;
    }
    catch (const exception &e) {
        if (string(e.what()).find("Path [root.**] does not exist") != string::npos) {
        //if (string(e.what()).find("Path [root.cpp.**] does not exist") != string::npos) {
            return true;
        }
        info_log("cleanAllSG(), error: %s", e.what());
        return false;
    }

    return true;
};

void printStatistics(OperationBase &op, double interval) {
    printf("----------------------------------------------------------Result Matrix------------------------------------------------------------\n");
    printf("%-20s %-20s %-20s %-20s %-20s %-20s\n", "Operation", "SuccOperation", "SuccPoint", "failOperation",
           "failPoint", "throughput(point/s)");
    printf("%-20s %-20llu %-20llu %-20llu %-20llu %-20.2f\n", op.getOpName().c_str(), op.getSuccOperationCount(),
           op.getSuccInsertPointCount(), op.getFailOperationCount(), op.getFailInsertPointCount(), op.getSuccInsertPointCount() / interval);

    op.genLatencySum();
    auto p = op.getPermillageMap();
    printf("----------------------------------------------------------Latency (ms) Matrix------------------------------------------------------\n");
    printf("%-16s %-8s %-8s %-8s %-8s %-8s %-8s %-8s %-8s %-8s %-8s %-8s %-18s\n",
           "Operation", "AVG", "MIN",
           "P10", "P25", "MEDIAN", "P75", "P90", "P95", "P99", "P999", "MAX", "SLOWEST_THREAD");
    printf("%-16s %-8.2f %-8.2f %-8.2f %-8.2f %-8.2f %-8.2f %-8.2f %-8.2f %-8.2f %-8.2f %-8.2f %-18.2f\n",
           op.getOpName().c_str(), op.getAvgLatencyMs(), op.getminLatencyUs() / 1000.0,
           p[100], p[250], p[500], p[750], p[900], p[950], p[990], p[999], op.getmaxLatencyUs()/1000.0, op.getLatencyMaxRangUs()/1000.0);
}

bool getCfgGroup(EasyCfgBase &config, const string &prefix, WorkerCfg &workerCfg) {
    string enableStr;
    if (!config.GetParamStr(prefix + "ENABLE", enableStr)) {
        return false;
    };
    transform(enableStr.begin(), enableStr.end(), enableStr.begin(), ::toupper);
    if ((enableStr.compare("ON") != 0) && (enableStr.compare("YES") != 0)) {
        return false;
    }

    workerCfg.sessionNum = config.GetParamInt(prefix + "SESSION_NUMBER");
    workerCfg.storageGroupNum = config.GetParamInt(prefix + "SG_NUMBER");
    workerCfg.deviceNum = config.GetParamInt(prefix + "DEVICE_NUMBER");
    workerCfg.sensorNum = config.GetParamInt(prefix + "SENSOR_NUMBER");
    string sensorDataTypeStr = config.GetParamStr(prefix + "SENSOR_DATA_TYPE");
    config.ParseParamList(sensorDataTypeStr, workerCfg.dataTypeList);
    if (workerCfg.dataTypeList.size() <= 0) {
        error_log("Invalid configure %s=%s", (prefix + "SENSOR_DATA_TYPE").c_str(), sensorDataTypeStr.c_str());
        return false;
    }
    workerCfg.batchSize = config.GetParamInt(prefix + "BATCH_SIZE");
    workerCfg.startTimestamp = config.GetParamInt64(prefix + "START_TIMESTAMP");
    workerCfg.loopIntervalMs = config.GetParamInt(prefix + "LOOP_INTERVAL_MS");
    workerCfg.loopNum = config.GetParamInt(prefix + "LOOP_NUM");

    return true;
}


int main() {
    flag_Debug = true;

    initEasyLog();
    EasyCfgBase config("../conf/config.properties");

    ServerCfg serverCfg;
    serverCfg.host = config.GetParamStr("HOST");
    serverCfg.port = config.GetParamInt("PORT");
    serverCfg.user = "root";
    serverCfg.passwd = "root";
    show("Server Address: %s:%d\n", serverCfg.host.c_str(), serverCfg.port);


    vector<shared_ptr<OperationBase>> OperationList;
    WorkerCfg tabletsCfg;
    if (getCfgGroup(config, "TABLETS_", tabletsCfg)) {
        show("Read TABLETS Configuration: Succ!\n");
        OperationList.emplace_back(new InsertTabletsOperation(serverCfg, tabletsCfg));
    } else {
        show("Read TABLETS Configuration: Disable.\n");
    }

    WorkerCfg tabletCfg;
    if (getCfgGroup(config, "TABLET_", tabletCfg)) {
        show("Read TABLET Configuration: Succ!\n");
        OperationList.emplace_back(new InsertTabletOperation2(serverCfg, tabletCfg));
    } else {
        show("Read TABLET Configuration: Disable.\n");
    }


    uint64_t startTime, endTime;
    show("\n== Clean all SG ==");
    startTime = getTimeUs();
    Session tmpSession(serverCfg.host, serverCfg.port, serverCfg.user, serverCfg.passwd);
    tmpSession.open(false, 2000);  //enableRPCCompression=false, connectionTimeoutInMs=1000
    cleanAllSG(tmpSession);
    tmpSession.close();
    endTime = getTimeUs();
    show(" (%.3fs) \n", (endTime - startTime)/1000000.0);

    sleep(1);


    show("\n== Create sessions ==\n");
    for (auto &op : OperationList) {
        show(" %-12s: create %d sessions ... ", op->getOpName().c_str(), op->getWorkerCfg().sessionNum);
        startTime = getTimeUs();
        op->createSessions();
        endTime = getTimeUs();
        show(" (%.3fs) \n", (endTime - startTime) / 1000000.0);
    }

    show("\n== Create schema ==\n");
    for (auto &op : OperationList) {
        show(" %-12s: create schema ... ", op->getOpName().c_str());
        startTime = getTimeUs();
        if (!op->createSchema()) {
            return -1;
        }
        endTime = getTimeUs();
        show(" (%.3fs) \n", (endTime - startTime) / 1000000.0);
    }

    sleep(1);

    show("\n== Insert data ==\n");
    startTime = getTimeUs();
    for (auto &op: OperationList) {
        show(" %-12s: Begin to insert data ... \n", op->getOpName().c_str());
        op->startWorkers();
    }

    list<uint> opFinished;
    for (uint i = 0; i < OperationList.size(); i++) {
        opFinished.push_back(i);
    }
    while (opFinished.size() > 0) {
        for (auto itr = opFinished.begin(); itr != opFinished.end();) {
            auto &op = OperationList[*itr];
            if (op->allWorkersFinished()) {
                endTime = getTimeUs();
                show("\n %-12s: Finish ...  (%.3fs) \n", op->getOpName().c_str(), (endTime - startTime) / 1000000.0);
                printStatistics(*op, (endTime - startTime) / 1000000.0);

                itr = opFinished.erase(itr);
            } else {
                itr++;
            }
        }
        sleep(1);
    }

    for (auto op: OperationList) {
        op->waitForAllWorkersFinished();
    }

    return 0;

}

