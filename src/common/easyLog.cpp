//
// Created by haiyi.zb on 8/26/22.
//
#include "easyLog.hpp"
//#include "encCfg.hpp"
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

