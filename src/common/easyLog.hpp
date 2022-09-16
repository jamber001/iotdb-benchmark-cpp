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

#ifndef EASY_LOG_H
#define EASY_LOG_H

#include <stdio.h>

extern bool flag_Debug;

void initEasyLog();
void log_prefix(const char* prefix);

#define show(args...)                                                   \
    do                                                                  \
    {                                                                   \
        fprintf(stdout, args);                                          \
    } while (0)

#define debug_log(args...)                                                  \
    do                                                                      \
    {                                                                       \
        if (flag_Debug)                                                     \
        {                                                                   \
            log_prefix("DEBUG ");                                           \
            fprintf(stderr, "%s:%d - ", __FILE__, __LINE__);                \
            fprintf(stderr, args);                                          \
            fprintf(stderr, "\n");                                          \
        }                                                                   \
    } while (0)

#define error_log(args...)                                 \
    do                                                    \
    {                                                     \
        log_prefix("ERROR ");                             \
        fprintf(stderr, "%s:%d - ", __FILE__, __LINE__);  \
        fprintf(stderr, args);                            \
        fprintf(stderr, "\n");                            \
    } while (0)

#define info_log(args...)         \
    do                            \
    {                             \
        log_prefix("INFO ");      \
        fprintf(stderr, "%s:%d - ", __FILE__, __LINE__);  \
        fprintf(stderr, args);    \
        fprintf(stderr, "\n");    \
    } while (0)

#define log_flush()     \
    do                  \
    {                   \
        fflush(stderr); \
    } while (0)

#endif //EASY_LOG_H
