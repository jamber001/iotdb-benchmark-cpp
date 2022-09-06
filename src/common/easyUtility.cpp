//
// Created by haiyi.zb on 8/31/22.
//

#include "easyUtility.hpp"

int64_t getTimeUs() {
    timeval tv;
    gettimeofday(&tv, 0);
    return (tv.tv_sec * 1000000llu + tv.tv_usec);
};

string getTimeStr() {
    timeval tv;
    gettimeofday(&tv, 0);
    time_t ts = tv.tv_sec;
    char str[30];
    strftime(str, sizeof(str), "%Y-%m-%d %T", localtime(&ts));
    str[sizeof(str) - 1] = 0;

    return string(str);
};