//
// Created by haiyi.zb on 9/1/22.
//


#ifndef INSERTTABLETOPERATION2_HPP
#define INSERTTABLETOPERATION2_HPP

#include "operationBase.hpp"
#include <thread>

class InsertTabletOperation2 : public OperationBase {
public:
    InsertTabletOperation2(const ServerCfg &serverCfg, const WorkerCfg &workerCfg) : OperationBase("Tablet",
                                                                                                   serverCfg,
                                                                                                   workerCfg) {}
    bool createSchema() override;

    void worker(int threadIdx) override;

private:
    void insertTabletBatch(shared_ptr<Session> &session, int sgIdx, int deviceIdx, int64_t startTs);
    void insertTabletBatch2(shared_ptr<Session> &session, int sgIdx, int deviceIdx, int64_t startTs);
    void sendInsertTablet(shared_ptr<Session> &session, Tablet &tabletMap);
    void sendInsertTablet(shared_ptr<Session> &session, TSInsertTabletReq &tsInsertTabletReq);

    void prepareData();

private:
    string sgPrefix = "tablet_";

    vector<pair<string, TSDataType::TSDataType>> schemaList4Device;
    //vector<Tablet>  tabletList;
    vector<TSInsertTabletReq>  requestList;

};


#endif //INSERTTABLETOPERATION2_HPP
