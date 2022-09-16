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

#include <string>
#include <thread>
#include <unistd.h>

#include "easyLog.hpp"
#include "easyCfgBase.hpp"
#include "paramCfg.hpp"
#include "easyUtility.hpp"
#include "Session.h"

#include "operationBase.hpp"
#include "insertTabletOperation.hpp"
#include "insertRecords.hpp"
#include "insertRecord.hpp"
#include "insertTabletsOperation.hpp"


using namespace std;

bool cleanAllSG(Session &session) {
    string DELETE_ALL_SG_SQL = "delete storage group root.cpp.**;";

    try {
        session.executeNonQueryStatement(DELETE_ALL_SG_SQL);
    }
    catch (const exception &e) {
        if (string(e.what()).find("Path [root.cpp.**] does not exist") != string::npos) {
            return true;
        }
        error_log("cleanAllSG(), error: %s", e.what());
        return false;
    }

    return true;
};

void printOpStatistics(OperationBase &op) {
    double interval = op.getWorkerTimeUs() / 1000000.0;
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

void printAllStatistics(vector<shared_ptr<OperationBase>> &OperationList) {
    unsigned long long succOperationCount = 0;
    unsigned long long failOperationCount = 0;
    unsigned long long succInsertPointCount = 0;
    unsigned long long failInsertPointCount = 0;

    int64_t workerTimeUs = 0;
    for (auto &op: OperationList) {
        succOperationCount += op->getSuccOperationCount();
        failOperationCount += op->getFailOperationCount();
        succInsertPointCount += op->getSuccInsertPointCount();
        failInsertPointCount += op->getFailInsertPointCount();
        if (op->getWorkerTimeUs() > workerTimeUs) {
            workerTimeUs = op->getWorkerTimeUs();
        }
    }
    double workerTimeSec = workerTimeUs / 1000000.0;
    printf("----------------------------------------------------------Result Matrix------------------------------------------------------------\n");
    printf("%-20s %-20s %-20s %-20s %-20s %-20s\n", "Operation", "SuccOperation", "SuccPoint", "failOperation",
           "failPoint", "throughput(point/s)");
    printf("%-20s %-20llu %-20llu %-20llu %-20llu %-20.2f\n", "All", succOperationCount,
           succInsertPointCount, failOperationCount, failInsertPointCount, succInsertPointCount / workerTimeSec);

//    op->genLatencySum();
//    auto p = op->getPermillageMap();
//    printf("----------------------------------------------------------Latency (ms) Matrix------------------------------------------------------\n");
//    printf("%-16s %-8s %-8s %-8s %-8s %-8s %-8s %-8s %-8s %-8s %-8s %-8s %-18s\n",
//           "Operation", "AVG", "MIN",
//           "P10", "P25", "MEDIAN", "P75", "P90", "P95", "P99", "P999", "MAX", "SLOWEST_THREAD");
//    printf("%-16s %-8.2f %-8.2f %-8.2f %-8.2f %-8.2f %-8.2f %-8.2f %-8.2f %-8.2f %-8.2f %-8.2f %-18.2f\n",
//           op->getOpName().c_str(), op->getAvgLatencyMs(), op->getminLatencyUs() / 1000.0,
//           p[100], p[250], p[500], p[750], p[900], p[950], p[990], p[999], op->getmaxLatencyUs()/1000.0, op->getLatencyMaxRangUs()/1000.0);
}

int main() {
    flag_Debug = true;

    initEasyLog();
    EasyCfgBase config("../conf/config.properties");

    printf("\n== Read base Configuration ==\n");
    ServerCfg serverCfg;
    serverCfg.host = config.GetParamStr("HOST");
    serverCfg.port = config.GetParamInt("PORT");
    serverCfg.user = "root";
    serverCfg.passwd = "root";
    serverCfg.rpcCompression = config.GetParamInt("RPC_COMPRESSION");
    printf(" Server Address: %s:%d\n", serverCfg.host.c_str(), serverCfg.port);


    printf("\n== Read Worker Configuration ==\n");
    vector<shared_ptr<OperationBase>> OperationList;
    WorkerCfg insertTabletsCfg;
    if (insertTabletsCfg.extractCfg(config, "TABLETS_")) {
        printf(" %-13s ==> %s\n","InsertTablets", "Succ!");
        OperationList.emplace_back(new InsertTabletsOperation(serverCfg, insertTabletsCfg));
    } else {
        printf(" %-13s ==> %s\n","InsertTablets", "Disable!");
    }

    WorkerCfg insertTabletCfg;
    if (insertTabletCfg.extractCfg(config, "TABLET_")) {
        printf(" %-13s ==> %s\n","InsertTablet", "Succ!");
        OperationList.emplace_back(new InsertTabletOperation(serverCfg, insertTabletCfg));
    } else {
        printf(" %-13s ==> %s\n","InsertTablet", "Disable!");
    }

    WorkerCfg insertRecordsCfg;
    if (insertRecordsCfg.extractCfg(config, "RECORDS_")) {
        printf(" %-13s ==> %s\n","InsertRecords", "Succ!");
        OperationList.emplace_back(new InsertRecordsOperation(serverCfg, insertRecordsCfg));
    } else {
        printf(" %-13s ==> %s\n","InsertRecords", "Disable!");
    }

    WorkerCfg insertRecordCfg;
    if (insertRecordCfg.extractCfg(config, "RECORD_")) {
        printf(" %-13s ==> %s\n","InsertRecord", "Succ!");
        OperationList.emplace_back(new InsertRecordOperation(serverCfg, insertRecordCfg));
    } else {
        printf(" %-13s ==> %s\n","InsertRecord", "Disable!");
    }


    int64_t startTime, endTime;
    printf("\n== Clean all SG ==\n");
    startTime = getTimeUs();
    Session tmpSession(serverCfg.host, serverCfg.port, serverCfg.user, serverCfg.passwd);
    tmpSession.open(serverCfg.rpcCompression, 2000);  //enableRPCCompression=false, connectionTimeoutInMs=1000
    cleanAllSG(tmpSession);
    tmpSession.close();
    endTime = getTimeUs();
    printf(" Finished ... (%.3fs) \n", (endTime - startTime)/1000000.0);

    sleep(1);


    printf("\n== Create sessions ==\n");
    for (auto &op : OperationList) {
        printf(" %-14s: create %d sessions ... ", op->getOpName().c_str(), op->getWorkerCfg().sessionNum);
        startTime = getTimeUs();
        op->createSessions();
        endTime = getTimeUs();
        printf(" (%.3fs) \n", (endTime - startTime) / 1000000.0);
    }

    printf("\n== Create schema ==\n");
    for (auto &op : OperationList) {
        printf(" %-14s: create schema ... ", op->getOpName().c_str());
        startTime = getTimeUs();
        if (!op->createSchema()) {
            return -1;
        }
        endTime = getTimeUs();
        printf(" (%.3fs) \n", (endTime - startTime) / 1000000.0);
    }

    sleep(1);

    printf("\n== Insert data ==\n");
    startTime = getTimeUs();
    for (auto &op: OperationList) {
        printf(" %-14s: Begin to insert data ... (%s)\n", op->getOpName().c_str(), getTimeStr().c_str());
        op->getWorkerCfg().printCfg();
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
                printf("\n>>> %-12s: Finish ...  (%.3fs) \n", op->getOpName().c_str(), op->getWorkerTimeUs() / 1000000.0);
                printOpStatistics(*op);

                itr = opFinished.erase(itr);
            } else {
                itr++;
            }
        }
        sleep(1);
    }

//    if (OperationList.size() > 1) {
//        printf("\n\n== Summary for all workers ==\n");
//        printAllStatistics(OperationList);
//    }

    for (auto op: OperationList) {
        op->waitForAllWorkersFinished();
    }

    return 0;

}

