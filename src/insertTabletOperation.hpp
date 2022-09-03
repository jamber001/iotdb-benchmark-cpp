//
// Created by haiyi.zb on 9/1/22.
//


#ifndef INSERTTABLETOPERATION_HPP
#define INSERTTABLETOPERATION_HPP

#include "operationBase.hpp"
#include <thread>

class InsertTabletOperation : public OperationBase {
public:
    InsertTabletOperation(const ServerCfg &serverCfg, const WorkerCfg &workerCfg) : OperationBase("Tablet",
                                                                                                   serverCfg,
                                                                                                   workerCfg) {}
    bool createSchema() override;

    void worker(int threadIdx) override;

private:
    void insertTabletBatch(shared_ptr<Session> &session, int sgIdx, int deviceIdx, int64_t startTs);
    void insertTabletBatch2(shared_ptr<Session> &session, int sgIdx, int deviceIdx, int64_t startTs);
    void sendInsertTablet(shared_ptr<Session> &session, Tablet &tabletMap);

    void prepareData();

private:
    string sgPrefix = "tablet_";

    vector<pair<string, TSDataType::TSDataType>> schemaList4Device;
    vector<Tablet>  tabletList;
};


#endif //INSERTTABLETOPERATION_HPP
