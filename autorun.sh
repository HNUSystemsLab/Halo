#!/bin/sh
#Lib path of libvmem
export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH
# CURR=$(pwd)
# export LD_LIBRARY_PATH=$CURR/third/pmdk/src/nondebug:$LD_LIBRARY_PATH

for w in ycsba ycsbb ycsbc ycsbd ycsbe ycsbf ycsbg ycsbh ycsbj ycsbak ycsbal ycsbam PiBench1 PiBench2 PiBench3 PiBench4 PiBench5 PiBench6 PiBench7 PiBench8
do
    for t in 1 4 8 16 24 32
    do  
        for h in HALO CCEH DASH CLEVEL PCLHT VIPER SOFT
        do
            numactl -N 0 ./$h $w $t
            rm /mnt/pmem/hash/* -rf
            echo "------------------------------------------------\n"
        done
    done
done