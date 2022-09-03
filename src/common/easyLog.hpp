//
// Created by haiyi.zb on 8/26/22.
//
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
