//
// Created by haiyi.zb on 9/1/22.
//


#ifndef INSERTTABLETOPERATION2_HPP
#define INSERTTABLETOPERATION2_HPP

#include "operationBase.hpp"
#include <thread>

class InsertTabletOperation : public OperationBase {
public:
    InsertTabletOperation(const ServerCfg &serverCfg, const WorkerCfg &workerCfg) : OperationBase("InsertTablet",
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
