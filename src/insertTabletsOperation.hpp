//
// Created by haiyi.zb on 8/30/22.
//

#ifndef INSERTTABLETSOPERATION_HPP
#define INSERTTABLETSOPERATION_HPP

#include "operationBase.hpp"

class InsertTabletsOperation : public OperationBase {
public:
    InsertTabletsOperation(const ServerCfg &serverCfg, const WorkerCfg &workerCfg) : OperationBase("Tablets",
                                                                                                   serverCfg,
                                                                                                   workerCfg) {}
    bool createSchema() override;

    void worker(int threadIdx) override;

private:
    void insertTabletsBatch(shared_ptr<Session> &session, int sgIndex, int64_t startTs);
    void sendInsertTablets(shared_ptr<Session> &session, unordered_map<string, Tablet *> &tabletMap);

private:
    string sgPrefix = "tablets_";
};


#endif //INSERTTABLETSOPERATION_HPP
