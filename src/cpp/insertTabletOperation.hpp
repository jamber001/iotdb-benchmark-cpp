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


#ifndef INSERTTABLETOPERATION2_HPP
#define INSERTTABLETOPERATION2_HPP

#include "operationBase.hpp"
#include <thread>

class InsertTabletOperation : public OperationBase {
public:
    InsertTabletOperation(const ServerCfg &serverCfg, const TaskCfg &workerCfg) : OperationBase("InsertTablet",
                                                                                                serverCfg,
                                                                                                workerCfg) {}
    bool createSchema() override;

    void worker(int threadIdx) override;

private:
    void prepareData();

    void insertTabletBatch(shared_ptr<Session> &session, int sgIdx, int deviceIdx, int64_t startTs);
    void insertTabletBatch2(shared_ptr<Session> &session, int sgIdx, int deviceIdx, int64_t startTs);
    void sendInsertTablet(shared_ptr<Session> &session, Tablet &tabletMap);
    void sendInsertTablet(shared_ptr<Session> &session, TSInsertTabletReq &tsInsertTabletReq);

private:
    string sgPrefix = "tablet_";

    vector<pair<string, TSDataType::TSDataType>> schemaList4Device;

    vector<TSInsertTabletReq>  requestList;   //for mode1, sgIdx ==> TSInsertTabletReq
    vector<Tablet>  tabletList;    //for mode2, sgIdx ==> Tablet
};


#endif //INSERTTABLETOPERATION2_HPP
