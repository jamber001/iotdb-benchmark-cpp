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


#ifndef INSERTRECORDS_HPP
#define INSERTRECORDS_HPP

#include "operationBase.hpp"
#include <thread>

using namespace  std;

class InsertRecordsOperation : public OperationBase {
public:
    InsertRecordsOperation(const ServerCfg &serverCfg, const WorkerCfg &workerCfg) : OperationBase("InsertRecords",
                                                                                                   serverCfg,
                                                                                                   workerCfg) {};

    bool createSchema() override;

    void worker(int threadIdx) override;

private:
    void insertRecordsBatch(shared_ptr<Session> &session, int sgIdx, int deviceIdx, int64_t startTs);

    void sendInsertRecords(shared_ptr<Session> &session, const std::vector<std::string> &deviceIds,
                           const std::vector<int64_t> &timestamps,
                           const std::vector<std::vector<std::string>> &measurementsList,
                           const std::vector<std::vector<string>> &valuesList);

    void sendInsertRecords2(shared_ptr<Session> &session, const std::vector<std::string> &deviceIds,
                            const std::vector<int64_t> &timestamps,
                            const std::vector<std::vector<std::string>> &measurementsList,
                            const vector<vector<TSDataType::TSDataType>> &typesList,
                            const std::vector<std::vector<char *>> &valuesList);

    void prepareData();

    string genValueStr(TSDataType::TSDataType);

private:
    string sgPrefix = "records_";

    vector<vector<string>> measurementsList;    //all Sessions use same measurementsList
    vector<vector<TSDataType::TSDataType>> typesList; //all Sessions use same typesList
    vector<vector<string>> valuesList;         //all Sessions use same valuesList
    vector<vector<char *>> valuesList2;         //all Sessions use same valuesList

};


#endif //INSERTRECORDS_HPP
