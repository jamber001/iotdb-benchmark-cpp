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

#include <cstring>
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

string msToTimeStr(long epochMs) {
    time_t ts = epochMs / 1000;
    char str[50];
    strftime(str, sizeof(str), "%Y.%m.%d %T", localtime(&ts));
    size_t len = strlen(str);
    snprintf(str + len, sizeof(str) - len, ".%03ld", epochMs % 1000);
    str[sizeof(str) - 1] = 0;

    return string(str);
};