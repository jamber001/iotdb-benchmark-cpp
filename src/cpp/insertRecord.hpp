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

#ifndef INSERTRECORD_HPP
#define INSERTRECORD_HPP

#include "operationBase.hpp"
#include <thread>

using namespace  std;

class InsertRecordOperation : public OperationBase {
public:
    InsertRecordOperation(const ServerCfg &serverCfg, const TaskCfg &taskCfg) : OperationBase(taskCfg.taskName,
                                                                                              serverCfg, taskCfg) {
        sgPrefix = "record_";
    };

    bool doPreWork() override;

    void worker(int threadIdx) override;

private:

    void insertRecordsBatch(shared_ptr<Session> &session, int sgIdx, int deviceIdx, int64_t startTs);

    bool sendInsertRecord(shared_ptr<Session> &session,
                          const string &deviceId,
                          int64_t timestamps,
                          const vector<string> &measurements,
                          const vector<string> &valuesList);

    bool sendInsertRecord2(shared_ptr<Session> &session,
                           const string &deviceId,
                           int64_t timestamps,
                           const vector<string> &measurementsList,
                           const vector<TSDataType::TSDataType> &typesList,
                           const vector<char *> &valuesList);

    char *getNewDataPtr(int sensorIdx);

private:
    vector<vector<string>> recordValueList;       //it is used by all Sessions
    vector<vector<char *>> recordValueList2;      //it is used by all Sessions

    list<bool> boolStore;
    list<int32_t> int32Store;
    list<int64_t> int64Store;
    list<float> floatStore;
    list<double> doubleStore;
    list<string> stringStore;
};



#endif //INSERTRECORD_HPP
