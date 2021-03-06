#!/usr/bin/env bash

# Usage:
# --neighborhood_partitioning_lvl
# --input_remote_dir
# --output_remote_dir
# --experiment_index
# --local_exp_dir
# --parallelism
# --numPartitions
# --minpts
# --epsilon
# --partitioningStrategy
# --geoDecimalPlacesSensitivity
# --debug
# --maxPointsPerPartition
# --driverMemory
# --executorMemory
# --maxResultSize

# Experiment setup
MINPTS=20
EPSILON=40

# Set args defaults
LOCAL="/mnt/experiments/"
INPUT_REMOTE="s3://mybucket/data/"
OUTPUT_REMOTE="s3://mybucket/output/"
PARTITION_LVL=15
INDEX=0
PARALLELISM=256
NUM_PARTITIONS=256
PARTITIONING_STRATEGY="S2"
GEO_SENSITIVITY=-1
DEBUG="false"
MAX_POINTS_PER_PARTITION=256
EXECUTOR_MEMORY="6g"
DRIVER_MEMORY="12g"
MAX_RESULT_SIZE="4g"

# Read params
echo 'Reading script params...'
#while [ $# -gt 0 ]; do
for i in "$@"
do
case $i in
--neighborhood_partitioning_lvl=*)
PARTITION_LVL="${i#*=}"
shift
;;
--input_remote_dir=*)
INPUT_REMOTE="${i#*=}"
shift
;;
--output_remote_dir=*)
OUTPUT_REMOTE="${i#*=}"
shift
;;
--experiment_index=*)
INDEX="${i#*=}"
shift
;;
--local_exp_dir=*)
LOCAL="${i#*=}"
shift
;;
--parallelism=*)
PARALLELISM="${i#*=}"
shift
;;
--numPartitions=*)
NUM_PARTITIONS="${i#*=}"
shift
;;
--minpts=*)
MINPTS="${i#*=}"
shift
;;
--epsilon=*)
EPSILON="${i#*=}"
shift
;;
--partitioningStrategy=*)
PARTITIONING_STRATEGY="${i#*=}"
shift
;;
--geoDecimalPlacesSensitivity=*)
GEO_SENSITIVITY="${i#*=}"
shift
;;
--debug=*)
DEBUG="${i#*=}"
shift
;;
--maxPointsPerPartition=*)
MAX_POINTS_PER_PARTITION="${i#*=}"
shift
;;
--driverMemory=*)
DRIVER_MEMORY="${i#*=}"
shift
;;
--executorMemory=*)
EXECUTOR_MEMORY="${i#*=}"
shift
;;
--maxResultSize=*)
MAX_RESULT_SIZE="${i#*=}"
shift
;;
-*)
# do not exit out, just note failure
echo "unrecognized option: ${i#*=}"
;;
*)
break;
;;
esac
shift
done
echo 'Running with parameters : '
echo "INPUT_REMOTE = ${INPUT_REMOTE}"
echo "OUTPUT_REMOTE = ${OUTPUT_REMOTE}"
echo "LOCAL = ${LOCAL}"
echo "PARTITION_LVL = ${PARTITION_LVL}"
echo "MINPTS = ${MINPTS}"
echo "EPSILON = ${EPSILON}"
echo "INDEX = ${INDEX}"
echo "PARALLELISM = ${PARALLELISM}"
echo "NUM_PARTITIONS = ${NUM_PARTITIONS}"
echo "PARTITIONING_STRATEGY = ${PARTITIONING_STRATEGY}"
echo "GEO_SENSITIVITY = ${GEO_SENSITIVITY}"
echo "MAX_POINTS_PER_PARTITION = ${MAX_POINTS_PER_PARTITION}"
echo "DRIVER_MEMORY = ${DRIVER_MEMORY}"
echo "EXECUTOR_MEMORY = ${EXECUTOR_MEMORY}"
echo "MAX_RESULT_SIZE = ${MAX_RESULT_SIZE}"
echo "DEBUG = ${DEBUG}"

# Set useful variables
JAR_PATH="/resources/jar/dDBGSCAN_2.11-2.4.3_1.0.0.jar"
CURRENT_EXP_OUTPUT=$OUTPUT_REMOTE/dDBGSCAN/$PARTITIONING_STRATEGY/partlvl_$PARTITION_LVL/maxp_$MAX_POINTS_PER_PARTITION/exp_$INDEX/

echo "Preparing run cmd"
RUN_CMD="/usr/lib/spark/bin/spark-submit --class com.github.dmarcous.ddbgscan.api.CLIRunner --driver-java-options='-Dspark.yarn.app.container.log.dir=/mnt/var/log/hadoop' --conf spark.default.parallelism=${PARALLELISM} --conf spark.driver.memory=${DRIVER_MEMORY} --conf spark.executor.memory=${EXECUTOR_MEMORY} --conf spark.driver.maxResultSize=${MAX_RESULT_SIZE} ${LOCAL}${JAR_PATH} --inputFilePath ${INPUT_REMOTE} --outputFolderPath ${CURRENT_EXP_OUTPUT} --positionFieldId 0 --positionFieldLon 1 --positionFieldLat 2 --inputFieldDelimiter , --numPartitions ${NUM_PARTITIONS} --epsilon ${EPSILON} --minPts ${MINPTS} --partitioningStrategy ${PARTITIONING_STRATEGY} --geoDecimalPlacesSensitivity ${GEO_SENSITIVITY} --neighborhoodPartitioningLvl ${PARTITION_LVL} --maxPointsPerPartition ${MAX_POINTS_PER_PARTITION} --debug ${DEBUG}"
echo ${RUN_CMD}

echo "Starting run"
`${RUN_CMD}`
