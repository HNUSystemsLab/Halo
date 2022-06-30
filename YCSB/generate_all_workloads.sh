#!/bin/bash


KEY_TYPE=randint

for WORKLOAD_TYPE in a b c d e f g h; do
  echo workload${WORKLOAD_TYPE} > workload_config.inp
  echo ${KEY_TYPE} >> workload_config.inp
  python2 gen_workload.py workload_config.inp
done