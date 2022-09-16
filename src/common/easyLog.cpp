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

#include "easyLog.hpp"
#include <time.h>
#include <sys/time.h>

bool flag_Debug=false;

void initEasyLog()
{
    //TODO: if (!EncCfg::GetInstance()->GetParamBool("debug_enable", flag_Debug))
    {
        flag_Debug = false;
    }
}

void log_prefix(const char *prefix)
{
    timeval tv;
    gettimeofday(&tv, 0);
    time_t ts = tv.tv_sec;
    char ss[30];
    strftime(ss, sizeof(ss), "%Y-%m-%d %T", localtime(&ts));
    ss[sizeof(ss) - 1] = 0;

    fprintf(stderr, "%s,%03ld %s", ss, tv.tv_usec/1000, prefix);
}

