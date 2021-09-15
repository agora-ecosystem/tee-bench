#!/bin/bash

ARGS="$@"

help(){
  echo "SGX join algorithms available: NPO_st, NPO"
  echo "  -a    Select join algorithm to run [NPO_st]"
  echo ""
  echo "Other options:"
  echo ""
  echo "  -n    Number of threads [2]"
  echo "  -r    Build relation size R [2097152] (32 MB)"
  echo "  -s    Probe relation size S [2097152] (32 MB)"
  exit 1
}

POSITIONAL=()
while [[ $# -gt 0 ]]
do 
  key="$1"
  case $key in
    -h|--help)
      help
      ;;
    *) #unknown option
      shift
      ;;
  esac
done


sudo ./app $ARGS 
#ENCL_COUNTERS=$(sudo tail -1 /sys/kernel/debug/sgx_paging/log_file)
LOGS=$(sudo tail -1 /proc/sgx_logs)
LOGS=($LOGS)
ENCL_ID=${LOGS[0]}
ENCL_SIZE=${LOGS[1]}
EADD_CNT=${LOGS[2]}
EWB_CNT=${LOGS[3]}
#EWB="$(($EWB_COUNTER * 4 / 1024 ))"
SIZE="$(($ENCL_SIZE / 1024 / 1024 ))"
EWB=$(echo "scale=2; $EWB_CNT * 4 / 1024" | bc -l)
EADD=$(echo "scale=2; $EADD_CNT * 4 / 1024" | bc -l)

echo "ENCL_ID = $ENCL_ID"
echo "ENCL_SIZE = $SIZE MB"
echo "EWB = $EWB MB"
echo "EADD = $EADD MB"

