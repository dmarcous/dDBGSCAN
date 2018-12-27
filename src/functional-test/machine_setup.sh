#!/usr/bin/env bash

# Usage:
# --resources_remote_dir
# --local_dir

# Set defaults
RESOURCES_REMOTE="s3://mybucket/experiment_dir/"
LOCAL="/mnt/experiments"

# Read params
echo 'Reading script params...'
#while [ $# -gt 0 ]; do
for i in "$@"
do
case $i in
--resources_remote_dir=*)
RESOURCES_REMOTE="${i#*=}"
shift
;;
--resources_local_dir=*)
LOCAL="${i#*=}"
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
echo "RESOURCES_REMOTE = ${RESOURCES_REMOTE}"
echo "LOCAL = ${LOCAL}"

echo "Cleaning if old data exists"
rm -rf $LOCAL

echo "Downloading resources"
aws s3 cp --recursive $RESOURCES_REMOTE $LOCAL/resources

