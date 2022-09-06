//
// Created by haiyi.zb on 8/30/22.
//

#ifndef INSERTTABLETSOPERATION2_HPP
#define INSERTTABLETSOPERATION2_HPP

#include "operationBase.hpp"

class InsertTabletsOperation : public OperationBase {
public:
    InsertTabletsOperation(const ServerCfg &serverCfg, const WorkerCfg &workerCfg) : OperationBase("InsertTablets",
                                                                                                   serverCfg,
                                                                                                   workerCfg) {}
    bool createSchema() override;

    void worker(int threadIdx) override;

private:
    void prepareData();

    void insertTabletsBatch(shared_ptr<Session> &session, int sgIndex, int64_t startTs);
    void sendInsertTablets(shared_ptr<Session> &session, unordered_map<string, Tablet *> &tabletMap);

private:
    vector<unordered_map<string, Tablet *>> tabletMapList;   //sgId => tabletMap;
    vector<vector<Tablet>> tabletsList;     //sgId => tablets;

    string sgPrefix = "tablets_";
};


#endif //INSERTTABLETSOPERATION_HPP
