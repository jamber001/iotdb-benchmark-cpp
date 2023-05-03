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

#include "benchmark_version.h"
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
    string DELETE_ALL_SG_SQL = "delete storage group root.cpp_*";
    try {
        session.executeNonQueryStatement(DELETE_ALL_SG_SQL);
    }
    catch (const exception &e) {
        if (string(e.what()).find("Path [root.cpp_*] does not exist") != string::npos) {
            //info_log(" %s", DELETE_ALL_SG_SQL.c_str());
            return true;
        } else {
            error_log("cleanAllSG(), error: %s", e.what());
            return false;
        }
    }

    return true;
};

void printTaskStatus(vector<StatisticsResult> &resultList) {
    //== print title for Running time
    printf("----------------------------------------------------------Task Status----------------------------------------------------------------\n");
    printf("%-20s %-14s %-20s %-20s\n", "Task", "Status", "Interval(sec)", "RunningTime");
    for (auto &result: resultList) {
        string timeStr;
        timeStr = "[" + msToTimeStr(result.beginTimeUs / 1000) + " - " +  msToTimeStr(result.endTimeUs / 1000) + "]";
        printf("%-20s %-14s %-20.3f %-20s\n", result.opName.c_str(), result.opStatus.c_str(),
               (result.endTimeUs - result.beginTimeUs) / 1000000.0, timeStr.c_str());
    }
}

void printCountMatric(vector<StatisticsResult> &resultList) {
    //== print title for Count Matrix
    printf("----------------------------------------------------------Result Matrix--------------------------------------------------------------\n");
    printf("%-16s %-14s %-14s %-14s %-14s %-14s %-14s %-14s %-14s\n", "Task", "SuccOperation", "SuccRecord", "SuccPoint", "FailOperation",
           "FailRecord", "FailPoint", "Record/s", "Point/s)");

    for (auto &result: resultList) {
        double intervalSec = (result.endTimeUs - result.beginTimeUs) / 1000000.0;
        float rps, pps;
        if (intervalSec < 0.00001) {
            rps = 0.0;
            pps = 0.0;
        } else {
            rps = result.succRecordCount / intervalSec;
            pps = result.succInsertPointCount / intervalSec;
        }
        printf("%-16s %-14llu %-14llu %-14llu %-14llu %-14llu %-14llu %-14.2f %-14.2f\n", result.opName.c_str(), result.succOperationCount, result.succRecordCount,
               result.succInsertPointCount, result.failOperationCount, result.failRecordCount, result.failInsertPointCount, rps, pps);
    }
}


void printLatencyMatric(vector<StatisticsResult> &resultList) {
    //== print title for Latency Matrix
    printf("----------------------------------------------------------Latency (ms) Matrix--------------------------------------------------------\n");
    printf("%-16s %-8s %-8s %-8s %-8s %-8s %-8s %-8s %-8s %-8s %-8s %-8s %-18s\n", "Task", "AVG", "MIN", "P10",
           "P25", "MEDIAN", "P75", "P90", "P95", "P99", "P999", "MAX", "SLOWEST_THREAD");

    for (auto &result: resultList) {
        auto &p = result.latencyPermillageMap;
        printf("%-16s %-8.2f %-8.2f %-8.2f %-8.2f %-8.2f %-8.2f %-8.2f %-8.2f %-8.2f %-8.2f %-8.2f %-18.2f\n",
               result.opName.c_str(), result.avgLatencyUs / 1000.0, result.minLatencyUs / 1000.0,
               p[100], p[250], p[500], p[750], p[900], p[950], p[990], p[999], result.maxLatencyUs / 1000.0,
               result.latencyMaxRangUs / 1000.0);
    }
}

void printSummary(vector<shared_ptr<OperationBase>> &opList, bool printDelta = true, bool printAll = true) {
    for (auto &opPtr: opList) {
        opPtr->doStatisticsCheckpoint();
    }

    vector<StatisticsResult> resultList;
    resultList.resize(opList.size());

    string curTimeStr = msToTimeStr(getTimeUs() / 1000).c_str();
    if (printDelta) {
        printf("\n>>> Summary for INCREMENT (%s) \n", curTimeStr.c_str());

        int i = 0;
        for (auto &opPtr: opList) {
            opPtr->genDeltaStatisticsResult(resultList[i++]);
        }
        printTaskStatus(resultList);
        printCountMatric(resultList);
        printLatencyMatric(resultList);
    }

    if (printAll) {
        printf("\n>>> Summary for ALL (%s) \n", curTimeStr.c_str());
        int i = 0;
        for (auto &opPtr: opList) {
            opPtr->genFullStatisticsResult(resultList[i++]);
        }
        printTaskStatus(resultList);
        printCountMatric(resultList);
        printLatencyMatric(resultList);
    }
}

static void showUsage(const char * cmd) {
    printf("\n Usage:\n");
    printf("      %s -h    # help\n", cmd);
    printf("      %s -v    # print version\n", cmd);
    printf("\n");
}


int main(int argc, char* argv[]) {
    if (argc > 1) {
        if (0 == strcmp(argv[1], "-v")) {
            printf("\n Benchmark version: %s\n", BENCHMARK_VERSION);
            return 0;
        }
        showUsage(argv[0]);
        return 0;
    }

    flag_Debug = true;

    initEasyLog();
    EasyCfgBase config("../conf/main.conf");

    printf("\n== Read base Configuration ==\n");
    ServerCfg serverCfg;
    serverCfg.host = config.getParamStr("HOST");
    serverCfg.port = config.getParamInt("PORT");
    serverCfg.user = "root";
    serverCfg.passwd = "root";
    serverCfg.rpcCompression = config.getParamInt("RPC_COMPRESSION");
    printf(" Server Address: %s:%d\n", serverCfg.host.c_str(), serverCfg.port);
    fflush(stdout);

    printf("\n== Read Worker Configuration ==\n");
    vector <shared_ptr<OperationBase>> OperationList;
    vector <TaskCfg> taskCfgList;

    vector <string> sectionList;
    config.getSectionList(sectionList);
    taskCfgList.reserve(sectionList.size());
    for (string &section: sectionList) {
        if (section.empty()) {
            continue;
        }

        TaskCfg taskCfg;
        printf(" %-15s ==>  ", section.c_str());
        if (taskCfg.extractCfg(section, config)) {
            taskCfgList.push_back(taskCfg);
            if (taskCfg.taskEnable) {
                printf("%s\n", "Succ!");
            } else {
                printf("%s\n", "Disabled!");
            }
        } else {
            return -1;
        }
    }
    fflush(stdout);

    for (TaskCfg &taskCfg: taskCfgList) {
        if ("TABLET" == taskCfg.taskType) {
            OperationList.emplace_back(new InsertTabletOperation(serverCfg, taskCfg));
        } else if ("TABLETS" == taskCfg.taskType) {
            OperationList.emplace_back(new InsertTabletsOperation(serverCfg, taskCfg));
        } else if ("RECORD" == taskCfg.taskType) {
            OperationList.emplace_back(new InsertRecordOperation(serverCfg, taskCfg));
        } else if ("RECORDS" == taskCfg.taskType) {
            OperationList.emplace_back(new InsertRecordsOperation(serverCfg, taskCfg));
        }
    }

    int64_t startTime, endTimeUs;

    bool cleanSg = true;
    config.getParamBool("CLEAN_SG", cleanSg);
    if (cleanSg) {
        printf("\n== Clean test related SG ==\n");
        startTime = getTimeUs();
        Session tmpSession(serverCfg.host, serverCfg.port, serverCfg.user, serverCfg.passwd);
        tmpSession.open(serverCfg.rpcCompression, 2000);  //enableRPCCompression=false, connectionTimeoutInMs=1000
        cleanAllSG(tmpSession);
        tmpSession.close();
        endTimeUs = getTimeUs();
        printf(" Finished ... (%.3fs) \n", (endTimeUs - startTime) / 1000000.0);

        sleep(1);
    }
    fflush(stdout);

    printf("\n== Create sessions ==\n");
    for (auto &op: OperationList) {
        printf(" %-14s: create %d sessions ... ", op->getOpName().c_str(), op->getWorkerCfg().sessionNum);
        startTime = getTimeUs();
        op->createSessions();
        endTimeUs = getTimeUs();
        printf(" (%.3fs) \n", (endTimeUs - startTime) / 1000000.0);

        // here, also do pre-work.
        op->doPreWork();
    }
    fflush(stdout);

    printf("\n== Create schema ==\n");
    for (auto &op: OperationList) {
        printf(" %-14s: create schema ... ", op->getOpName().c_str());
        if (op->getWorkerCfg().createSchema) {
            startTime = getTimeUs();
            if (!op->createSchema()) {
                return -1;
            }
            endTimeUs = getTimeUs();
            printf(" (%.3fs) \n", (endTimeUs - startTime) / 1000000.0);
        } else {
            printf(" Disabled! \n");
        }
    }
    fflush(stdout);

    sleep(1);

    printf("\n== Start all tasks ==\n");
    startTime = getTimeUs();
    for (auto &op: OperationList) {
        printf(" %-14s: Begin to insert data ... (%s)\n", op->getOpName().c_str(), getTimeStr().c_str());
        op->getWorkerCfg().printCfg();
        op->startWorkers();
    }
    fflush(stdout);

    //== print statistic result
    int summaryIntervalUs = 0;
    config.getParamInt("SUMMARY_INTERVAL_SEC", summaryIntervalUs);
    summaryIntervalUs = summaryIntervalUs * 1000000;

    list<uint> opRunList;
    for (uint i = 0; i < OperationList.size(); i++) {
        opRunList.push_back(i);
    }

    int64_t lastTimeUs = getTimeUs();
    while (opRunList.size() > 0) {
        endTimeUs = getTimeUs();
        if ((summaryIntervalUs > 0) && (endTimeUs - lastTimeUs) >= summaryIntervalUs) {
            printSummary(OperationList, true, false);
            fflush(stdout);
            lastTimeUs = endTimeUs;
        }

        for (auto itr = opRunList.begin(); itr != opRunList.end();) {
            auto &op = OperationList[*itr];
            if (op->allWorkersFinished()) {
                printf("\n>>> Finish Task: %-14s ... (%.3fs) \n", op->getOpName().c_str(), op->getWorkerTimeUs() / 1000000.0);
                fflush(stdout);
                itr = opRunList.erase(itr);
            } else {
                itr++;
            }
        }
        sleep(1);
        //int key = getchar();
    }

    //== print overall statistics
    printSummary(OperationList, false, true);
    fflush(stdout);

    for (auto op: OperationList) {
        op->waitForAllWorkerThreadsFinished();
    }

    return 0;
}

