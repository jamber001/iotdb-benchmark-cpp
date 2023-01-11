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

#ifndef INSERTTABLETSOPERATION2_HPP
#define INSERTTABLETSOPERATION2_HPP

#include "operationBase.hpp"

class InsertTabletsOperation : public OperationBase {
public:
    InsertTabletsOperation(const ServerCfg &serverCfg, const TaskCfg &taskCfg) : OperationBase(taskCfg.taskName,
                                                                                               serverCfg,
                                                                                               taskCfg) {}
    bool doPreWork();
    bool createSchema() override;

    void worker(int threadIdx) override;

private:
    void insertTabletsBatch(shared_ptr<Session> &session, int sgIndex, int64_t startTs);
    void sendInsertTablets(shared_ptr<Session> &session, unordered_map<string, Tablet *> &tabletMap);

private:
    vector<unordered_map<string, Tablet *>> tabletMapList;   //sgId => tabletMap (devicePath => Tablet*)
    vector<vector<Tablet>> tabletsList;     //sgId => tablets;

    string sgPrefix = "tablets_";
};


#endif //INSERTTABLETSOPERATION_HPP
