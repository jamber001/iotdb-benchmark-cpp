//
// Created by haiyi.zb on 8/31/22.
//

#include "easyUtility.hpp"

uint64_t getTimeUs() {
    timeval tv;
    gettimeofday(&tv, 0);
    return (tv.tv_sec * 1000000llu + tv.tv_usec);
}
