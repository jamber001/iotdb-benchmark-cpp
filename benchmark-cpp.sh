#!/bin/bash

baseDir=`dirname $0`
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:${baseDir}/sdk/clientIncLib/lib

${baseDir}/sbin/benchmark-cpp
