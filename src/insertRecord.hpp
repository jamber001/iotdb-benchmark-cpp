//
// Created by haiyi.zb on 9/1/22.
//


#ifndef INSERTRECORD_HPP
#define INSERTRECORD_HPP

#include "operationBase.hpp"
#include <thread>

using namespace  std;

class InsertRecordOperation : public OperationBase {
public:
    InsertRecordOperation(const ServerCfg &serverCfg, const WorkerCfg &workerCfg) : OperationBase("InsertRecord",
                                                                                                   serverCfg,
                                                                                                   workerCfg) {};

    bool createSchema() override;

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

    void prepareData();

    string genValue(TSDataType::TSDataType);

private:
    string sgPrefix = "record_";

    vector<string> measurements4OneRecord;           //it is used by all Sessions
    vector<TSDataType::TSDataType> types4OneRecord;  //it is used by all Sessions

    vector<vector<string>> valuesList;                //it is used by all Sessions
    vector<vector<char *>> valuesList2;               //it is used by all Sessions
};



#endif //INSERTRECORD_HPP
